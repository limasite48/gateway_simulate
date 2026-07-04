import time
import json
import os
from datetime import datetime
from config import (
    ZONES, ZONE_CODES, TYPE_CODES, CODE_TO_ZONE, CODE_TO_TYPE,
    TEMP_THRESHOLD, HUMID_THRESHOLD, LIGHT_THRESHOLD,
    TOPIC_SERVER_SEND
)

class GatewayEngine:
    def __init__(self):
        # Trạng thái kết nối Server mô phỏng
        self.network_connected = True
        
        # Thống kê gói tin
        self.stats = {
            "rx_zigbee": 0,
            "tx_zigbee": 0,
            "tx_server": 0,
            "filtered": 0,
            "offline_saved": 0
        }
        
        # Bảng dữ liệu Device Shadow (Trạng thái hiện tại của toàn bộ thiết bị)
        self.state_document = {
            "timestamp": None,
            "system_status": "NORMAL",
            "zones": {},
            "doors": {},
            "windows": {}
        }
        
        # Khởi tạo trạng thái mặc định cho các Zone để đồng bộ và tránh N/A
        for zone in ZONES:
            self.state_document["zones"][zone] = {
                "temp": 25.0,
                "humid": 60.0,
                "smoke": False,
                "light_intensity": 50,
                "light": {"status": "TẮT"},
                "ahu": {
                    "status": "TẮT",
                    "fan_speed": 1,
                    "temp_set": 25.0
                }
            }
            
        # Khởi tạo trạng thái mặc định cho các cửa đi (mặc định đóng)
        for i in range(1, 6):
            self.state_document["doors"][f"door_0{i}"] = {
                "status": "ĐÓNG"
            }
            
        # Khởi tạo trạng thái mặc định cho các cửa sổ & rèm (mặc định đóng, rèm che 100%)
        for i in range(1, 7):
            self.state_document["windows"][f"wd_0{i}"] = {
                "status": "ĐÓNG",
                "curtain": {
                    "status": "ĐÓNG",
                    "percentage_cover": 100
                }
            }
            
        # Lưu trữ giá trị gần nhất được gửi lên Server để kiểm tra ngưỡng lọc
        self.last_sent_values = {}
        # Lưu trữ mốc thời gian thực gửi thành công gần nhất (để ép gửi Heartbeat sau 5 phút chống trôi)
        self.last_sent_time = {}
        for zone in ZONES:
            self.last_sent_values[zone] = {
                "temp": None,
                "humid": None,
                "light_intensity": None,
                "smoke": None
            }
            self.last_sent_time[zone] = {
                "temp": 0.0,
                "humid": 0.0,
                "light_intensity": 0.0,
                "smoke": 0.0
            }
            
        # Hàng đợi các gói JSON cần gửi lên Server (dành cho client kết nối đẩy đi)
        self.server_publish_queue = []
        
        # Cờ đánh dấu có cập nhật dữ liệu thường đang chờ gửi lên Server (Gom tin/Debounce)
        self.pending_publish = False
        
        # Bộ đệm tích lũy dữ liệu delta để gửi lên Server
        self.pending_delta = {
            "timestamp": None,
            "zones": {},
            "doors": {},
            "windows": {}
        }
        
        # Đường dẫn file log lưu trữ ngoại tuyến
        self.offline_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "offline_telemetry.json")

    def get_delta_payload(self) -> dict:
        """Trả về bản tin delta chứa các trường thay đổi và reset bộ đệm delta"""
        payload = {
            "timestamp": self.pending_delta["timestamp"] or int(time.time()),
            "system_status": "NORMAL"
        }
        
        # Chỉ đưa các nhánh có thay đổi vào payload
        for section in ["zones", "doors", "windows"]:
            if self.pending_delta[section]:
                payload[section] = self.pending_delta[section]
                
        # Reset bộ đệm delta
        self.pending_delta = {
            "timestamp": None,
            "zones": {},
            "doors": {},
            "windows": {}
        }
        return payload

    def process_device_update(self, zone_id: int, type_code: int, decoded_data: dict) -> bool:
        """Xử lý dữ liệu nhận từ thiết bị Zigbee, cập nhật Device Shadow và áp dụng bộ lọc"""
        self.stats["rx_zigbee"] += 1
        
        zone_name = CODE_TO_ZONE.get(zone_id)
        type_name = CODE_TO_TYPE.get(type_code)
        
        if not zone_name or not type_name:
            return False
            
        # Cập nhật timestamp của Gateway
        self.state_document["timestamp"] = int(time.time())
        
        # Biến đánh giá xem cập nhật này có đủ điều kiện gửi lên Server hay không
        should_publish = False
        is_smoke_alert = False
        
        # 1. Cập nhật trạng thái và áp dụng logic lọc theo từng loại cảm biến/thiết bị
        if zone_name in ZONES:
            # Thuộc phân vùng văn phòng
            zone_data = self.state_document["zones"][zone_name]
            last_sent = self.last_sent_values[zone_name]
            
            current_time = time.time()
            if type_name == "dht22":
                temp = decoded_data["temp"]
                humid = decoded_data["humid"]
                zone_data["temp"] = temp
                zone_data["humid"] = humid
                
                # Kiểm tra lọc nhiệt độ (hoặc cưỡng bức gửi sau 5 phút chống trôi)
                last_t = self.last_sent_time[zone_name]["temp"]
                is_temp_timeout = (current_time - last_t >= 300.0)
                
                if last_sent["temp"] is None or abs(temp - last_sent["temp"]) >= TEMP_THRESHOLD or is_temp_timeout:
                    should_publish = True
                    last_sent["temp"] = temp
                    self.last_sent_time[zone_name]["temp"] = current_time
                    if zone_name not in self.pending_delta["zones"]:
                        self.pending_delta["zones"][zone_name] = {}
                    self.pending_delta["zones"][zone_name]["temp"] = temp
                    
                # Kiểm tra lọc độ ẩm (hoặc cưỡng bức gửi sau 5 phút chống trôi)
                last_h = self.last_sent_time[zone_name]["humid"]
                is_humid_timeout = (current_time - last_h >= 300.0)
                
                if last_sent["humid"] is None or abs(humid - last_sent["humid"]) >= HUMID_THRESHOLD or is_humid_timeout:
                    should_publish = True
                    last_sent["humid"] = humid
                    self.last_sent_time[zone_name]["humid"] = current_time
                    if zone_name not in self.pending_delta["zones"]:
                        self.pending_delta["zones"][zone_name] = {}
                    self.pending_delta["zones"][zone_name]["humid"] = humid
                    
                if not should_publish:
                    self.stats["filtered"] += 1
                    
            elif type_name == "lm393":
                light = decoded_data["light_intensity"]
                zone_data["light_intensity"] = light
                
                # Kiểm tra lọc ánh sáng (hoặc cưỡng bức gửi sau 5 phút chống trôi)
                last_l = self.last_sent_time[zone_name]["light_intensity"]
                is_light_timeout = (current_time - last_l >= 300.0)
                
                if last_sent["light_intensity"] is None or abs(light - last_sent["light_intensity"]) >= LIGHT_THRESHOLD or is_light_timeout:
                    should_publish = True
                    last_sent["light_intensity"] = light
                    self.last_sent_time[zone_name]["light_intensity"] = current_time
                    if zone_name not in self.pending_delta["zones"]:
                        self.pending_delta["zones"][zone_name] = {}
                    self.pending_delta["zones"][zone_name]["light_intensity"] = light
                else:
                    self.stats["filtered"] += 1
                    
            elif type_name == "mq2":
                smoke = decoded_data["smoke"]
                zone_data["smoke"] = smoke
                
                # Kiểm tra thay đổi báo khói (hoặc cưỡng bức gửi sau 5 phút chống trôi)
                last_s = self.last_sent_time[zone_name]["smoke"]
                is_smoke_timeout = (current_time - last_s >= 300.0)
                
                if last_sent["smoke"] != smoke or is_smoke_timeout:
                    should_publish = True
                    # Báo cháy khẩn cấp chỉ kích hoạt khi chuyển từ không khói -> có khói
                    if last_sent["smoke"] != smoke and smoke:
                        is_smoke_alert = True
                    last_sent["smoke"] = smoke
                    self.last_sent_time[zone_name]["smoke"] = current_time
                    if zone_name not in self.pending_delta["zones"]:
                        self.pending_delta["zones"][zone_name] = {}
                    self.pending_delta["zones"][zone_name]["smoke"] = smoke
                else:
                    self.stats["filtered"] += 1
                    
            elif type_name == "light":
                # Thiết bị chấp hành: Cập nhật ngay lập tức
                zone_data["light"] = decoded_data
                should_publish = True
                if zone_name not in self.pending_delta["zones"]:
                    self.pending_delta["zones"][zone_name] = {}
                self.pending_delta["zones"][zone_name]["light"] = decoded_data
                
            elif type_name == "ahu":
                # Thiết bị chấp hành: Cập nhật ngay lập tức
                zone_data["ahu"] = decoded_data
                should_publish = True
                if zone_name not in self.pending_delta["zones"]:
                    self.pending_delta["zones"][zone_name] = {}
                self.pending_delta["zones"][zone_name]["ahu"] = decoded_data
                
        elif zone_name.startswith("door_"):
            # Cảm biến cửa đi (MC38): Cập nhật ngay lập tức
            self.state_document["doors"][zone_name] = decoded_data
            should_publish = True
            self.pending_delta["doors"][zone_name] = decoded_data
            
        elif zone_name.startswith("wd_"):
            # Cửa sổ (MC38 hoặc Curtain)
            win_data = self.state_document["windows"][zone_name]
            
            if type_name == "mc38":
                win_data["status"] = decoded_data["status"]
                should_publish = True
                if zone_name not in self.pending_delta["windows"]:
                    self.pending_delta["windows"][zone_name] = {}
                self.pending_delta["windows"][zone_name]["status"] = decoded_data["status"]
            elif type_name == "curtain":
                win_data["curtain"] = decoded_data
                should_publish = True
                if zone_name not in self.pending_delta["windows"]:
                    self.pending_delta["windows"][zone_name] = {}
                self.pending_delta["windows"][zone_name]["curtain"] = decoded_data
 
        # 2. Xử lý gửi dữ liệu lên Server (nếu vượt qua bộ lọc)
        if should_publish:
            self.pending_delta["timestamp"] = self.state_document["timestamp"]
            
            if is_smoke_alert:
                # Cảnh báo cháy: Gửi ngay lập tức 3 lần để tránh mất tin, không trì hoãn
                delta_payload = self.get_delta_payload()
                payload = json.dumps(delta_payload, ensure_ascii=False)
                if self.network_connected:
                    for _ in range(3):
                        self.server_publish_queue.append((TOPIC_SERVER_SEND, payload))
                        self.stats["tx_server"] += 1
                else:
                    self.save_offline_data(delta_payload)
            else:
                # Dữ liệu thường: Đánh dấu cần gửi (sẽ được gom và gửi sau ở gateway_loop)
                self.pending_publish = True
                
            return True
            
        return False

    def save_offline_data(self, data: dict):
        """Lưu trữ dữ liệu ngoại tuyến vào file JSON cục bộ"""
        self.stats["offline_saved"] += 1
        
        # Đọc dữ liệu cũ nếu file tồn tại
        existing_logs = []
        if os.path.exists(self.offline_file_path):
            try:
                with open(self.offline_file_path, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    if content:
                        existing_logs = json.loads(content)
            except Exception:
                existing_logs = []
                
        # Append bản ghi mới
        existing_logs.append(data)
        
        # Ghi lại file dạng JSON array
        try:
            with open(self.offline_file_path, "w", encoding="utf-8") as f:
                json.dump(existing_logs, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[ERROR] Không thể ghi file lưu trữ ngoại tuyến: {e}")

    def sync_offline_data(self):
        """Gửi bù toàn bộ dữ liệu lưu trữ offline lên Server khi kết nối lại mạng và xóa file log"""
        if not os.path.exists(self.offline_file_path):
            return 0
            
        count = 0
        try:
            with open(self.offline_file_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    offline_records = json.loads(content)
                    for record in offline_records:
                        payload = json.dumps(record, ensure_ascii=False)
                        # Đẩy vào hàng đợi gửi Server
                        self.server_publish_queue.append((TOPIC_SERVER_SEND, payload))
                        self.stats["tx_server"] += 1
                        count += 1
                        
            # Xóa file log sau khi gửi bù thành công
            os.remove(self.offline_file_path)
            print(f"\n[OFFLINE] Đã gửi bù thành công {count} gói tin lưu trữ ngoại tuyến và xóa file log.")
        except Exception as e:
            print(f"[ERROR] Lỗi đồng bộ dữ liệu ngoại tuyến: {e}")
            
        return count
