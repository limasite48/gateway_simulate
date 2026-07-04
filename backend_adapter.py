"""
Backend Adapter — cầu nối giữa Gateway (đã giải mã AES-CCM) và Backend Spring Boot.

Gateway giữ NGUYÊN liên kết bảo mật AES-CCM với cảm biến. Sau khi giải mã, gateway dùng
module này để dịch dữ liệu sang đúng "hợp đồng" MQTT của backend
(xem iot-platform-integration-guide.md §5):

  - Telemetry:  iot/telemetry/{zone}/{gateway_id}
                {timestamp, zone, gateway_id, sensors:[{id,type,value,unit?}]}
  - Heartbeat:  iot/heartbeat/{device_id}
                {device_id, timestamp, memory_usage_pct, cpu_usage_pct, wifi_rssi}
  - Command:    iot/command/{device_id}     (server -> gateway)
  - Ack:        iot/command_ack/{device_id}  {command_id, device_id, status, executed_at}

Chỉ 2 zone office_1 & meeting được ánh xạ vì backend chỉ đăng ký (seed) các thiết bị này.
"""
import json
import random
from datetime import datetime, timezone

# ---- Topic backend ----
def telemetry_topic(zone, gateway_id):
    return f"iot/telemetry/{zone}/{gateway_id}"

def heartbeat_topic(device_id):
    return f"iot/heartbeat/{device_id}"

def ack_topic(device_id):
    return f"iot/command_ack/{device_id}"

BACKEND_COMMAND_SUB = "iot/command/#"

# ---- Ánh xạ zone emulator -> gateway_id backend ----
ZONE_TO_GATEWAY = {
    "office_1": "gw_office1_01",
    "meeting": "gw_meeting_01",
}

# ---- Ánh xạ (zone, loại cảm biến emulator) -> danh sách sensor backend ----
# mỗi phần tử: (sensor_id, backend_type, unit|None, key trong decoded_data)
SENSOR_MAP = {
    ("office_1", "dht22"): [
        ("s_temp_1", "temp", "C", "temp"),
        ("s_hmid_1", "hmid", "%", "humid"),
    ],
    ("office_1", "mq2"): [("s_smoke_1", "smoke", None, "smoke")],
    ("meeting", "dht22"): [
        ("s_temp_2", "temp", "C", "temp"),
        ("s_hmid_2", "hmid", "%", "humid"),
    ],
    ("meeting", "mq2"): [("s_smoke_2", "smoke", None, "smoke")],
}

# ---- 12 thiết bị ACTIVE cần heartbeat để hiện ONLINE trên dashboard ----
HEARTBEAT_DEVICES = [
    "gw_office1_01", "gw_meeting_01",
    "s_temp_1", "s_hmid_1", "s_smoke_1",
    "s_temp_2", "s_hmid_2", "s_smoke_2",
    "act_light_1", "act_exhaust_1", "act_ac_1", "act_curtain_1",
]

# ---- Ánh xạ actuator backend -> thiết bị downlink emulator ----
# emu_target: tên zone/window trong ZONE_CODES; emu_device: khóa trong TYPE_CODES
BACKEND_ACTUATORS = {
    "act_light_1": {"emu_target": "office_1", "emu_device": "light"},
    "act_ac_1": {"emu_target": "meeting", "emu_device": "ahu"},
    "act_curtain_1": {"emu_target": "wd_05", "emu_device": "curtain"},  # cửa sổ phòng họp
    "act_exhaust_1": {"emu_target": None, "emu_device": None},          # sim không có quạt hút
}


def _now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_telemetry(zone_name, type_name, decoded):
    """Dựng bản tin telemetry backend từ dữ liệu đã giải mã. Trả (topic, json_str) hoặc None."""
    entries = SENSOR_MAP.get((zone_name, type_name))
    if not entries:
        return None
    gw = ZONE_TO_GATEWAY.get(zone_name)
    if not gw:
        return None
    sensors = []
    for sensor_id, backend_type, unit, key in entries:
        if key not in decoded:
            continue
        item = {"id": sensor_id, "type": backend_type, "value": decoded[key]}
        if unit:
            item["unit"] = unit
        sensors.append(item)
    if not sensors:
        return None
    payload = {
        "timestamp": _now_iso(),
        "zone": zone_name,
        "gateway_id": gw,
        "sensors": sensors,
    }
    return telemetry_topic(zone_name, gw), json.dumps(payload, ensure_ascii=False)


def build_heartbeat(device_id):
    """Dựng bản tin heartbeat backend. Trả (topic, json_str)."""
    payload = {
        "device_id": device_id,
        "timestamp": _now_iso(),
        "status": "ONLINE",
        "memory_usage_pct": random.randint(35, 50),
        "cpu_usage_pct": random.randint(8, 20),
        "wifi_rssi": random.randint(-65, -50),
    }
    return heartbeat_topic(device_id), json.dumps(payload)


def translate_command(actuator_id, parameters):
    """
    Dịch tham số lệnh của backend sang (emu_target, emu_device, emu_cmd) để gateway
    mã hóa xuống sensor. Trả (None, None, None) nếu không có thiết bị vật lý tương ứng.
    """
    mapping = BACKEND_ACTUATORS.get(actuator_id)
    if not mapping or not mapping["emu_device"]:
        return None, None, None
    target = mapping["emu_target"]
    device = mapping["emu_device"]
    params = parameters or {}

    if device == "light":
        active = str(params.get("status", "")).upper() == "ON"
        return target, "light", {"active": active}

    if device == "ahu":
        active = str(params.get("status", "")).upper() == "ON"
        temp_set = params.get("set_temp", 24)
        return target, "ahu", {"active": active, "fan_speed": 2, "temp_set": float(temp_set)}

    if device == "curtain":
        direction = str(params.get("direction", "")).upper()
        pct = 100 if direction == "DOWN" else (0 if direction == "UP" else 50)
        return target, "curtain", {"percentage_cover": pct}

    return None, None, None
