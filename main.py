import sys
import os
import time
import threading
import json
from datetime import datetime

# Cấu hình encoding UTF-8 cho dòng lệnh để chạy tốt trên Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stdin, 'reconfigure'):
    sys.stdin.reconfigure(encoding='utf-8')

# Thêm thư mục hiện tại vào sys.path để tránh lỗi import tĩnh
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

import paho.mqtt.client as mqtt
from config import (
    MQTT_BROKER, MQTT_PORT, TOPIC_ZIGBEE_TELEMETRY, TOPIC_ZIGBEE_COMMAND,
    TOPIC_SERVER_SEND, TOPIC_SERVER_RECEIVE,
    ZONES, ZONE_CODES, TYPE_CODES, CODE_TO_ZONE, CODE_TO_TYPE
)
from protocol import (
    parse_uplink_frame, decode_uplink_payload,
    encode_downlink_command, wrap_downlink_frame
)
from gateway import GatewayEngine
import backend_adapter as ba

# Khởi tạo engine của Gateway
gateway = GatewayEngine()

# Flag log để in thông báo Hex nhận được ra màn hình
log_packets = False

# Event đồng bộ hóa kết nối mạng trước khi chạy gateway
connected_event = threading.Event()

# Bộ đệm lưu trữ các lệnh đang chờ xác thực phản hồi trạng thái từ thiết bị (Handshake ACK)
pending_commands = {}
pending_commands_lock = threading.Lock()

def is_state_matching(device_type, emu_cmd, decoded_data):
    """Kiểm tra dữ liệu phản hồi thô từ thiết bị có trùng khớp với trạng thái mong muốn không"""
    if device_type == "light":
        expected_status = "BẬT" if emu_cmd.get("active") else "TẮT"
        return decoded_data.get("status") == expected_status
    elif device_type == "ahu":
        expected_status = "BẬT" if emu_cmd.get("active") else "TẮT"
        if decoded_data.get("status") != expected_status:
            return False
        if emu_cmd.get("fan_speed") is not None and decoded_data.get("fan_speed") != emu_cmd.get("fan_speed"):
            return False
        if emu_cmd.get("temp_set") is not None and abs(decoded_data.get("temp_set", 0.0) - emu_cmd.get("temp_set")) > 0.1:
            return False
        return True
    elif device_type == "curtain":
        expected_pct = emu_cmd.get("percentage_cover")
        return decoded_data.get("percentage_cover") == expected_pct
    return True

def on_connect(client, userdata, flags, rc, properties=None):
    """Callback khi kết nối thành công tới broker"""
    print(f"\n[MQTT] Gateway đã kết nối thành công tới Broker: {MQTT_BROKER}", flush=True)
    
    # Đăng ký nhận tin nhắn
    client.subscribe(TOPIC_ZIGBEE_TELEMETRY)
    client.subscribe(TOPIC_SERVER_RECEIVE)
    
    print(f"[MQTT] Đã subscribe kênh Zigbee Telemetry: {TOPIC_ZIGBEE_TELEMETRY}", flush=True)
    print(f"[MQTT] Đã subscribe kênh Server Receive:  {TOPIC_SERVER_RECEIVE}", flush=True)

    # Subscribe kênh lệnh của backend thật (iot/command/{device_id})
    client.subscribe(ba.BACKEND_COMMAND_SUB)
    print(f"[MQTT] Đã subscribe kênh lệnh Backend:    {ba.BACKEND_COMMAND_SUB}\n", flush=True)
    connected_event.set()

def on_message(client, userdata, msg):
    """Xử lý điều phối tin nhắn từ MQTT broker"""
    global log_packets
    
    # 1. Nhận dữ liệu Hex từ thiết bị (Zigbee Uplink)
    if msg.topic == TOPIC_ZIGBEE_TELEMETRY:
        payload_str = msg.payload.decode("utf-8", errors="ignore").strip()
        
        parsed = parse_uplink_frame(payload_str)
        if not parsed:
            return
            
        zone_id = parsed["zone_id"]
        type_code = parsed["type_code"]
        payload_bytes = parsed["payload"]
        
        # Giải mã chi tiết dữ liệu cảm biến/thiết bị
        decoded_data = decode_uplink_payload(type_code, payload_bytes)
        if not decoded_data:
            print(f"[DEBUG ERR] Không thể decode payload thô cho type_code {type_code:02X} zone {zone_id:02X}", flush=True)
            return
            
        # Đẩy dữ liệu vào Engine để xử lý (lọc ngưỡng, cập nhật bảng trạng thái)
        was_published = gateway.process_device_update(zone_id, type_code, decoded_data)

        # Kiểm tra các lệnh đang chờ xác thực phản hồi từ thiết bị (Handshake ACK)
        type_name_check = CODE_TO_TYPE.get(type_code)
        if type_name_check:
            with pending_commands_lock:
                cmds = pending_commands.get((zone_id, type_code), [])
                for cmd in cmds:
                    if is_state_matching(type_name_check, cmd["emu_cmd"], decoded_data):
                        cmd["event"].set()

        # Chuyển tiếp lên BACKEND THẬT: sau khi đã giải mã AES-CCM, dịch sang JSON chuẩn backend.
        # Chỉ office_1/meeting với dht22 (temp/humid) & mq2 (smoke) — khớp thiết bị seed ở backend.
        zone_name_fwd = CODE_TO_ZONE.get(zone_id)
        type_name_fwd = CODE_TO_TYPE.get(type_code)
        if zone_name_fwd and type_name_fwd:
            fwd = ba.build_telemetry(zone_name_fwd, type_name_fwd, decoded_data)
            if fwd:
                client.publish(fwd[0], fwd[1])

        if log_packets:
            zone_name = CODE_TO_ZONE.get(zone_id, f"0x{zone_id:02X}")
            type_name = CODE_TO_TYPE.get(type_code, f"0x{type_code:02X}")
            filter_status = "ĐÃ ĐẨY LÊN" if was_published else "BỊ LỌC BỎ"
            if not gateway.network_connected and was_published:
                filter_status = "LƯU OFFLINE"
            print(f"[ZIGBEE RX] {zone_name:<15} | Cảm biến: {type_name:<8} | Giá trị: {decoded_data} | Kết quả: {filter_status}", flush=True)

    # 2. Nhận lệnh JSON từ Server (Server Downlink)
    elif msg.topic == TOPIC_SERVER_RECEIVE:
        if not gateway.network_connected:
            # Nếu mất kết nối mạng, coi như không nhận được lệnh từ Server
            return
            
        payload_str = msg.payload.decode("utf-8", errors="ignore").strip()
        try:
            cmd_json = json.loads(payload_str)
            target_name = cmd_json.get("target")
            device_name = cmd_json.get("device")
            command_params = cmd_json.get("command", {})
            
            zone_code = ZONE_CODES.get(target_name)
            type_code = TYPE_CODES.get(device_name)
            
            if zone_code is None or type_code is None:
                print(f"[SERVER ERR] Không tìm thấy mã định danh cho target: {target_name} hoặc device: {device_name}")
                return
                
            # Mã hóa lệnh thành byte thô
            raw_payload = encode_downlink_command(type_code, command_params)
            if raw_payload is None:
                print(f"[SERVER ERR] Lệnh điều khiển không đúng cú pháp: {command_params}")
                return
                
            # Đóng gói thành khung Hex downlink
            hex_command = wrap_downlink_frame(zone_code, type_code, raw_payload)
            
            if log_packets:
                print(f"\n[SERVER RX] Nhận lệnh JSON từ Server: {payload_str}")
                print(f"            Dịch sang Hex: {hex_command} -> Gửi xuống thiết bị qua Zigbee.")
                
            # Phát xuống thiết bị qua kênh Zigbee Command
            client.publish(TOPIC_ZIGBEE_COMMAND, hex_command)
            gateway.stats["tx_zigbee"] += 1
            
        except Exception as e:
            print(f"[SERVER ERR] Gói tin JSON từ Server bị lỗi cú pháp: {e}")

    # 3. Nhận lệnh từ BACKEND THẬT (iot/command/{device_id})
    elif msg.topic.startswith("iot/command/"):
        handle_backend_command(client, msg)


def handle_backend_command(client, msg):
    """Lệnh backend -> mã hóa AES-CCM downlink cho sensor -> ack (RECEIVED/SUCCESS) về backend sau khi xác thực phản hồi thô."""
    global log_packets
    device_id = msg.topic.split("/")[-1]
    try:
        cmd_json = json.loads(msg.payload.decode("utf-8", errors="ignore").strip())
    except Exception as e:
        print(f"[BACKEND ERR] Lệnh JSON từ backend bị lỗi: {e}", flush=True)
        return

    command_id = cmd_json.get("command_id")
    parameters = cmd_json.get("parameters", {})

    emu_target, emu_device, emu_cmd = ba.translate_command(device_id, parameters)
    ack_t = ba.ack_topic(device_id)

    # Gửi RECEIVED ngay lập tức để backend biết gateway đã nhận lệnh
    client.publish(ack_t, json.dumps({"command_id": command_id, "device_id": device_id, "status": "RECEIVED"}))

    if emu_device:
        zone_code = ZONE_CODES.get(emu_target)
        type_code = TYPE_CODES.get(emu_device)
        raw_payload = encode_downlink_command(type_code, emu_cmd)
        if zone_code is not None and type_code is not None and raw_payload is not None:
            # Dùng lại đúng đường mã hóa AES-CCM downlink của gateway
            hex_command = wrap_downlink_frame(zone_code, type_code, raw_payload)
            client.publish(TOPIC_ZIGBEE_COMMAND, hex_command)
            gateway.stats["tx_zigbee"] += 1
            if log_packets:
                print(f"[BACKEND CMD] {device_id} -> {emu_target}.{emu_device} {emu_cmd} | đã mã hóa & gửi Hex downlink", flush=True)

            # Luồng chờ phản hồi từ thiết bị (Handshake ACK)
            def _wait_for_ack(cmd_id, dev_id, z_code, t_code, cmd_params, timeout=4.0):
                event = threading.Event()
                with pending_commands_lock:
                    if (z_code, t_code) not in pending_commands:
                        pending_commands[(z_code, t_code)] = []
                    pending_commands[(z_code, t_code)].append({
                        "command_id": cmd_id,
                        "emu_cmd": cmd_params,
                        "event": event
                    })
                
                # Đợi tối đa 4 giây để thiết bị phản hồi
                success = event.wait(timeout=timeout)
                
                # Gỡ bỏ lệnh ra khỏi danh sách pending
                with pending_commands_lock:
                    if (z_code, t_code) in pending_commands:
                        pending_commands[(z_code, t_code)] = [c for c in pending_commands[(z_code, t_code)] if c["command_id"] != cmd_id]
                
                if success:
                    client.publish(ack_t, json.dumps({
                        "command_id": cmd_id,
                        "device_id": dev_id,
                        "status": "SUCCESS",
                        "executed_at": ba._now_iso(),
                    }))
                    if log_packets:
                        print(f"[BACKEND ACK] Lệnh {cmd_id} cho {dev_id} THÀNH CÔNG (Xác thực phản hồi trạng thái từ thiết bị)", flush=True)
                else:
                    client.publish(ack_t, json.dumps({
                        "command_id": cmd_id,
                        "device_id": dev_id,
                        "status": "FAILED",
                        "error": "Device response timeout or disconnected",
                        "executed_at": ba._now_iso(),
                    }))
                    if log_packets:
                        print(f"[BACKEND ACK] Lệnh {cmd_id} cho {dev_id} THẤT BẠI (Hết thời gian phản hồi từ thiết bị)", flush=True)
            
            threading.Thread(target=_wait_for_ack, args=(command_id, device_id, zone_code, type_code, emu_cmd), daemon=True).start()
    else:
        # Nếu thiết bị không được mô phỏng vật lý, phản hồi SUCCESS lập tức
        print(f"[BACKEND CMD] {device_id}: sim không có thiết bị vật lý tương ứng — chỉ ack.", flush=True)
        def _send_immediate_success():
            time.sleep(0.1)
            client.publish(ack_t, json.dumps({
                "command_id": command_id,
                "device_id": device_id,
                "status": "SUCCESS",
                "executed_at": ba._now_iso(),
            }))
        threading.Thread(target=_send_immediate_success, daemon=True).start()


def polling_loop():
    """Luồng phụ thực hiện quét vòng (Polling) các cảm biến"""
    # Đợi kết nối MQTT thành công trước khi quét
    connected_event.wait()
    
    sensor_types = ["dht22", "mq2", "lm393"]
    
    while True:
        start_time = time.time()
        
        # Quét tuần tự 12 zone, mỗi zone quét 3 cảm biến
        for zone in ZONES:
            zone_code = ZONE_CODES[zone]
            for s_type in sensor_types:
                type_code = TYPE_CODES[s_type]
                
                # Payload lệnh poll: 1 byte 0x01
                raw_payload = b"\x01"
                hex_command = wrap_downlink_frame(zone_code, type_code, raw_payload)
                
                # Publish lệnh poll xuống mạng Zigbee giả lập
                mqtt_client.publish(TOPIC_ZIGBEE_COMMAND, hex_command)
                gateway.stats["tx_zigbee"] += 1
                
                # Giãn cách 100ms để tránh nghẽn
                time.sleep(0.1)
                
        # Một lượt quét 36 cảm biến mất 3.6 giây. Nghỉ nốt phần còn lại của chu kỳ 30s.
        elapsed = time.time() - start_time
        sleep_time = max(0.1, 30.0 - elapsed)
        time.sleep(sleep_time)

def heartbeat_loop():
    """Gửi heartbeat định kỳ (30s) cho 12 thiết bị ACTIVE để backend đánh dấu ONLINE."""
    while True:
        if gateway.network_connected:
            for device_id in ba.HEARTBEAT_DEVICES:
                topic, payload = ba.build_heartbeat(device_id)
                mqtt_client.publish(topic, payload)
        time.sleep(30)


# Khởi tạo MQTT Client sử dụng callback API version 2
mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

def gateway_loop():
    """Luồng phụ cập nhật định kỳ và đẩy tin nhắn Server từ hàng đợi"""
    last_flush_time = time.time()
    while True:
        now = time.time()
        # 1. Gom tin thường gửi Server (Debounce 3 giây)
        if now - last_flush_time >= 3.0:
            last_flush_time = now
            if gateway.pending_publish:
                delta_payload = gateway.get_delta_payload()
                payload = json.dumps(delta_payload, ensure_ascii=False)
                if gateway.network_connected:
                    gateway.server_publish_queue.append((TOPIC_SERVER_SEND, payload))
                    gateway.stats["tx_server"] += 1
                else:
                    gateway.save_offline_data(delta_payload)
                gateway.pending_publish = False

        # 2. Đẩy tin nhắn từ hàng đợi ra MQTT broker (nếu kết nối mạng)
        if gateway.network_connected:
            while gateway.server_publish_queue:
                topic, payload = gateway.server_publish_queue.pop(0)
                mqtt_client.publish(topic, payload)
                time.sleep(0.02) # Trễ ngắn tránh nghẽn/rớt gói tin JSON lên Server
                
        # Nghỉ ngắn
        time.sleep(0.1)

def print_help():
    print("\n================= CÁC LỆNH ĐIỀU KHIỂN GATEWAY =================")
    print("1. Trạng thái: `status` hoặc `st` (Xem bộ đệm dữ liệu và các chỉ số)")
    print("2. Giả lập Mạng: `network <on/off>` (Bật/tắt kết nối mạng lên Server)")
    print("3. Giả lập Lệnh Server: `send <json_lệnh>` (Gửi lệnh JSON để dịch sang Hex)")
    print("   * Ví dụ bật đèn:   `send {\"target\": \"pantry\", \"device\": \"light\", \"command\": {\"active\": true}}`")
    print("   * Ví dụ rèm:       `send {\"target\": \"wd_01\", \"device\": \"curtain\", \"command\": {\"percentage_cover\": 80}}`")
    print("4. Bật/Tắt Log: `log <on/off>` (Bật/tắt in các gói tin Zigbee nhận/gửi)")
    print("5. Hướng dẫn: `help` hoặc `h`")
    print("6. Thoát: `exit` hoặc `quit`")
    print("==============================================================")

def display_status():
    """Hiển thị trạng thái Gateway, chỉ số thống kê và dữ liệu lưu trữ hiện tại"""
    print(f"\n--- TRẠNG THÁI HOẠT ĐỘNG GATEWAY ---")
    conn_status = "ĐANG KẾT NỐI (BẬT)" if gateway.network_connected else "MẤT KẾT NỐI (TẮT - NGOẠI TUYẾN)"
    print(f"Kết nối Server: {conn_status}")
    print(f"Thống kê gói tin:")
    print(f"  - Nhận từ Zigbee (Uplink): {gateway.stats['rx_zigbee']}")
    print(f"  - Bị lọc nhiễu (Filter):   {gateway.stats['filtered']}")
    print(f"  - Gửi lên Server (JSON):   {gateway.stats['tx_server']}")
    print(f"  - Gửi xuống Zigbee (Cmd):  {gateway.stats['tx_zigbee']}")
    print(f"  - Đã lưu trữ ngoại tuyến:  {gateway.stats['offline_saved']}")
    
    # Kiểm tra sự tồn tại của file offline log
    if os.path.exists(gateway.offline_file_path):
        size = os.path.getsize(gateway.offline_file_path)
        print(f"  * File log offline hiện tại: {gateway.offline_file_path} ({size} bytes)")
    
    # Hiển thị bảng Device Shadow của 12 zone
    print(f"+-----------------+----------+--------+-------------+-------+-------+-------------------------+")
    print(f"|       Phân vùng | Nhiệt độ |  Độ ẩm |    Ánh sáng |  Khói |   Đèn |            Điều hòa AHU |")
    print(f"+-----------------+----------+--------+-------------+-------+-------+-------------------------+")
    for zone in ZONES:
        data = gateway.state_document["zones"][zone]
        temp_val = data["temp"]
        humid_val = data["humid"]
        light_val = data["light_intensity"]
        
        smoke_str = "CÓ KHÓI" if data["smoke"] is True else ("Không" if data["smoke"] is False else "N/A")
        
        lamp_val = data["light"]
        light_str = lamp_val.get("status", "N/A") if isinstance(lamp_val, dict) else "N/A"
        
        ahu_val = data["ahu"]
        if isinstance(ahu_val, dict):
            ahu_str = f"{ahu_val.get('status')} (TĐ:{ahu_val.get('fan_speed')}, Đặt:{ahu_val.get('temp_set'):.1f}°C)"
        else:
            ahu_str = "N/A"
            
        temp_part = f"{temp_val:>5.1f}°C" if temp_val is not None else "     N/A"
        humid_part = f"{humid_val:>5.1f}%" if humid_val is not None else "   N/A"
        light_part = f"{light_val:>7} lux" if light_val is not None else "   N/A lux"
            
        print(f"| {zone:<15} |  {temp_part} | {humid_part} | {light_part} | {smoke_str:<5} |  {light_str:<5}| {ahu_str:<23} |")
    print(f"+-----------------+----------+--------+-------------+-------+-------+-------------------------+")
    
    # Bảng Cửa đi
    print("\n--- TRẠNG THÁI CỬA ĐI ---")
    doors_list = []
    for i in range(1, 6):
        d_name = f"door_0{i}"
        d_state = gateway.state_document["doors"][d_name].get("status", "N/A")
        doors_list.append(f"{d_name}: {d_state.upper() if d_state else 'N/A'}")
    print(" | ".join(doors_list))
    
    # Bảng Cửa sổ & Rèm
    print("\n--- TRẠNG THÁI CỬA SỔ & RÈM ---")
    for i in range(1, 7):
        w_name = f"wd_0{i}"
        w_data = gateway.state_document["windows"][w_name]
        w_state = w_data.get("status", "N/A")
        
        curt_val = w_data.get("curtain")
        curt_str = f"{curt_val.get('percentage_cover')}%" if isinstance(curt_val, dict) else "N/A"
        
        print(f"- {w_name}: {w_state.upper() if w_state else 'N/A'} | Rèm che phủ: {curt_str}")
    print("")

def console_loop():
    """Vòng lặp chính điều khiển bằng console"""
    global log_packets
    print_help()
    while True:
        try:
            cmd_line = input("\nNhập lệnh Gateway: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nĐang dừng Gateway...")
            break
            
        if not cmd_line:
            continue
            
        parts = cmd_line.split(maxsplit=1)
        cmd = parts[0].lower()
        
        if cmd in ["exit", "quit"]:
            print("Đang dừng bộ giả lập Gateway...")
            break
            
        elif cmd in ["h", "help"]:
            print_help()
            
        elif cmd in ["status", "st"]:
            display_status()
            
        elif cmd == "log":
            if len(parts) > 1 and parts[1].lower() in ["on", "off"]:
                log_packets = (parts[1].lower() == "on")
                print(f"Đã {'BẬT' if log_packets else 'TẮT'} nhật ký gói tin.")
            else:
                print("Sai cú pháp. Dùng: `log <on/off>`")
                
        elif cmd == "network":
            if len(parts) > 1 and parts[1].lower() in ["on", "off"]:
                state = (parts[1].lower() == "on")
                gateway.network_connected = state
                print(f"Đã cập nhật trạng thái mạng: {'KẾT NỐI (BẬT)' if state else 'MẤT KẾT NỐI (TẮT)'}")
                
                if state:
                    # Gửi bù tin nhắn lưu offline khi mạng phục hồi
                    gateway.sync_offline_data()
            else:
                print("Sai cú pháp. Dùng: `network <on/off>`")
                
        elif cmd == "send":
            if len(parts) > 1:
                json_str = parts[1]
                # Đóng vai trò của Server publish thẳng vào topic data_receive của chính gateway
                mqtt_client.publish(TOPIC_SERVER_RECEIVE, json_str)
                print("Đã giả lập gửi tin nhắn JSON từ Server xuống.")
            else:
                print("Sai cú pháp. Dùng: `send <json_str>`")
        else:
            print("Lệnh không hợp lệ. Gõ `help` để xem danh sách lệnh.")

if __name__ == "__main__":
    print("-------------------------------------------------------------")
    print("Bộ Giả Lập IoT Gateway - HCMC Office")
    print("Phần cứng giả lập: Raspberry Pi (Cáp quang)")
    print("-------------------------------------------------------------")
    
    # Kết nối MQTT Broker
    print(f"Đang kết nối tới MQTT Broker: {MQTT_BROKER}...", flush=True)
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()
    except Exception as e:
        print(f"[ERROR] Không thể kết nối tới MQTT Broker: {e}", flush=True)
        sys.exit(1)
        
    # Chờ kết nối Broker thành công (tối đa 10s) trước khi chạy vòng lặp Gateway
    print("Đang chờ thiết lập phiên kết nối MQTT...", flush=True)
    if not connected_event.wait(timeout=10.0):
        print("[WARNING] Phiên kết nối MQTT chưa sẵn sàng, gateway sẽ bắt đầu ở chế độ offline.")
    else:
        print("[MQTT] Phiên kết nối MQTT đã sẵn sàng.", flush=True)
        
    # Chạy luồng phụ để đẩy tin Server
    t = threading.Thread(target=gateway_loop, daemon=True)
    t.start()
    
    # Chạy luồng heartbeat gửi lên backend (đánh dấu thiết bị ONLINE)
    hb = threading.Thread(target=heartbeat_loop, daemon=True)
    hb.start()
    
    # Chạy luồng quét vòng (polling) các cảm biến
    pl = threading.Thread(target=polling_loop, daemon=True)
    pl.start()

    # Chạy giao diện dòng lệnh console chính (hoặc chạy nền nếu không có console)
    try:
        if sys.stdin is not None and sys.stdin.isatty():
            console_loop()
        else:
            print("[HEADLESS] Không có console tương tác — gateway chạy nền.", flush=True)
            while True:
                time.sleep(1)
    except Exception as e:
        print(f"Lỗi vòng lặp console: {e}")
    finally:
        print("Đang ngắt kết nối MQTT...")
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print("Đã dừng Gateway thành công.")
