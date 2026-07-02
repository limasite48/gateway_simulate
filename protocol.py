import struct
from config import TYPE_CODES, CODE_TO_TYPE, ZONE_CODES, CODE_TO_ZONE

def calculate_checksum(zone_id: int, type_code: int, length: int, payload: bytes) -> int:
    """Tính toán XOR Checksum cho khung truyền tin"""
    chk = zone_id ^ type_code ^ length
    for b in payload:
        chk ^= b
    return chk

def parse_uplink_frame(hex_str: str):
    """Giải mã khung truyền tin uplink (Device -> Gateway)"""
    try:
        hex_clean = hex_str.strip().replace(" ", "")
        data = bytes.fromhex(hex_clean)
        if len(data) < 7:
            return None
        if data[0] != 0xA5 or data[-1] != 0x5A:
            return None
            
        zone_id = data[1]
        type_code = data[2]
        length = data[3]
        
        if len(data) != 6 + length:
            return None
            
        payload = data[4:4+length]
        checksum = data[4+length]
        
        if checksum != calculate_checksum(zone_id, type_code, length, payload):
            return None
            
        return {
            "zone_id": zone_id,
            "type_code": type_code,
            "payload": payload
        }
    except Exception:
        return None

def decode_uplink_payload(type_code: int, payload: bytes):
    """Giải mã dữ liệu thô nhận được từ thiết bị"""
    try:
        if type_code == TYPE_CODES["dht22"]:
            temp_val, humid_val = struct.unpack(">hH", payload)
            return {"temp": temp_val / 10.0, "humid": humid_val / 10.0}
            
        elif type_code == TYPE_CODES["mq2"]:
            smoke = struct.unpack(">B", payload)[0] == 1
            return {"smoke": smoke}
            
        elif type_code == TYPE_CODES["lm393"]:
            light_intensity = struct.unpack(">H", payload)[0]
            return {"light_intensity": light_intensity}
            
        elif type_code == TYPE_CODES["mc38"]:
            is_open = struct.unpack(">B", payload)[0] == 1
            return {"status": "open" if is_open else "closed"}
            
        elif type_code == TYPE_CODES["light"]:
            active = struct.unpack(">B", payload)[0] == 1
            return {"status": "ON" if active else "OFF"}
            
        elif type_code == TYPE_CODES["ahu"]:
            active_val, fan_speed, temp_set_val = struct.unpack(">BBh", payload)
            return {
                "status": "ON" if active_val == 1 else "OFF",
                "fan_speed": fan_speed,
                "temp_set": temp_set_val / 10.0
            }
            
        elif type_code == TYPE_CODES["curtain"]:
            percentage_cover = struct.unpack(">B", payload)[0]
            return {
                "status": "closed" if percentage_cover > 80 else ("open" if percentage_cover < 20 else "half"),
                "percentage_cover": percentage_cover
            }
            
    except Exception:
        pass
    return None

def wrap_downlink_frame(zone_id: int, type_code: int, payload: bytes) -> str:
    """Đóng gói dữ liệu downlink (Gateway -> Device) thành chuỗi Hex"""
    length = len(payload)
    checksum = calculate_checksum(zone_id, type_code, length, payload)
    frame = bytearray([0x5A, zone_id, type_code, length]) + payload + bytearray([checksum, 0xA5])
    return frame.hex().upper()

def encode_downlink_command(type_code: int, params: dict):
    """Mã hóa lệnh từ server thành byte payload thô cho thiết bị"""
    try:
        if type_code == TYPE_CODES["light"]:
            # active: True/False
            active = 1 if params.get("active") else 0
            return struct.pack(">B", active)
            
        elif type_code == TYPE_CODES["ahu"]:
            # active: True/False, fan_speed: 1-3, temp_set: float
            active = 1 if params.get("active") else 0
            fan_speed = int(params.get("fan_speed", 1))
            temp_set = float(params.get("temp_set", 25.0))
            return struct.pack(">BBh", active, fan_speed, int(temp_set * 10))
            
        elif type_code == TYPE_CODES["curtain"]:
            # percentage_cover: 0-100
            percentage_cover = int(params.get("percentage_cover", 0))
            return struct.pack(">B", percentage_cover)
            
    except Exception:
        pass
    return None
