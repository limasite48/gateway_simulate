import struct
from Crypto.Cipher import AES
from config import TYPE_CODES, CODE_TO_TYPE, ZONE_CODES, CODE_TO_ZONE, AES_KEY

tx_sequence_counter = 0

def get_next_sequence_number() -> int:
    """Tự động tăng và trả về Sequence Counter cho bản tin tiếp theo"""
    global tx_sequence_counter
    tx_sequence_counter += 1
    return tx_sequence_counter

def aes_ccm_encrypt(zone_id: int, type_code: int, seq_num: int, plaintext: bytes) -> tuple[bytes, bytes]:
    """Mã hóa AES-CCM và trả về (ciphertext, tag)"""
    # Tạo Nonce 13-Byte: [Zone ID (1B)][Type Code (1B)][Seq (4B)] + 7 Bytes 0x00
    nonce = struct.pack(">BBI", zone_id, type_code, seq_num) + b"\x00" * 7
    cipher = AES.new(AES_KEY, AES.MODE_CCM, nonce=nonce, mac_len=4)
    ciphertext, tag = cipher.encrypt_and_digest(plaintext)
    return ciphertext, tag

def aes_ccm_decrypt(zone_id: int, type_code: int, seq_num: int, ciphertext: bytes, tag: bytes) -> bytes:
    """Giải mã AES-CCM và xác thực tính toàn vẹn (trả về plaintext hoặc None)"""
    nonce = struct.pack(">BBI", zone_id, type_code, seq_num) + b"\x00" * 7
    cipher = AES.new(AES_KEY, AES.MODE_CCM, nonce=nonce, mac_len=4)
    try:
        return cipher.decrypt_and_verify(ciphertext, tag)
    except ValueError:
        return None

def calculate_checksum(zone_id: int, type_code: int, length: int, payload: bytes) -> int:
    """Tính toán XOR Checksum cho khung truyền tin"""
    chk = zone_id ^ type_code ^ length
    for b in payload:
        chk ^= b
    return chk

def parse_uplink_frame(hex_str: str):
    """Giải mã khung truyền tin uplink (Device -> Gateway) và giải mã AES-CCM"""
    try:
        hex_clean = hex_str.strip().replace(" ", "")
        data = bytes.fromhex(hex_clean)
        if len(data) < 7:
            print(f"[DEBUG ERR] Gói tin quá ngắn: {hex_str}", flush=True)
            return None
        if data[0] != 0xA5 or data[-1] != 0x5A:
            print(f"[DEBUG ERR] Gói tin sai Start/End byte: {hex_str}", flush=True)
            return None
            
        zone_id = data[1]
        type_code = data[2]
        length = data[3]
        
        if len(data) != 6 + length:
            print(f"[DEBUG ERR] Gói tin sai độ dài: {hex_str}", flush=True)
            return None
            
        secure_payload = data[4:4+length]
        checksum = data[4+length]
        
        if checksum != calculate_checksum(zone_id, type_code, length, secure_payload):
            print(f"[DEBUG ERR] Gói tin sai checksum: {hex_str}", flush=True)
            return None
            
        # Kiểm tra độ dài Secure Payload cho AES-CCM
        if len(secure_payload) < 8:
            print(f"\n[SECURITY ALERT] Nhận gói tin không được mã hóa từ Zone ID 0x{zone_id:02X}, Type Code 0x{type_code:02X}!\n", flush=True)
            return None
            
        seq_num = struct.unpack(">I", secure_payload[:4])[0]
        ciphertext = secure_payload[4:-4]
        tag = secure_payload[-4:]
        
        # Giải mã và xác thực AES-CCM
        plaintext = aes_ccm_decrypt(zone_id, type_code, seq_num, ciphertext, tag)
        if plaintext is None:
            print(f"\n[SECURITY ALERT] Phát hiện gói tin lỗi xác thực hoặc giả mạo từ Zone ID 0x{zone_id:02X}, Type Code 0x{type_code:02X}! (AES-CCM Auth Failed)\n", flush=True)
            return None
            
        return {
            "zone_id": zone_id,
            "type_code": type_code,
            "payload": plaintext
        }
    except Exception as e:
        print(f"[DEBUG ERR] Lỗi giải mã gói tin Hex: {e}", flush=True)
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
    """Đóng gói dữ liệu downlink (Gateway -> Device) thành chuỗi Hex có mã hóa AES-CCM"""
    seq_num = get_next_sequence_number()
    ciphertext, tag = aes_ccm_encrypt(zone_id, type_code, seq_num, payload)
    secure_payload = struct.pack(">I", seq_num) + ciphertext + tag
    
    length = len(secure_payload)
    checksum = calculate_checksum(zone_id, type_code, length, secure_payload)
    frame = bytearray([0x5A, zone_id, type_code, length]) + secure_payload + bytearray([checksum, 0xA5])
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
