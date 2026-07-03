# Cấu hình cho bộ giả lập IoT Gateway

MQTT_BROKER = "broker.emqx.io"
MQTT_PORT = 1883

# Các topic MQTT dùng cho Gateway
# 1. Kết nối với thiết bị (Zigbee Simulation)
TOPIC_ZIGBEE_TELEMETRY = "duk1chvietcong/hcmc_office/telemetry" # Subscribe nhận Hex từ thiết bị
TOPIC_ZIGBEE_COMMAND = "duk1chvietcong/hcmc_office/command"     # Publish Hex xuống thiết bị

# 2. Kết nối với Server (JSON)
TOPIC_SERVER_SEND = "duk1chvietcong/hcmc_office/data_send"       # Publish JSON lên Server
TOPIC_SERVER_RECEIVE = "duk1chvietcong/hcmc_office/data_receive" # Subscribe JSON nhận từ Server

# Cấu hình bảo mật AES-CCM (Giả lập Link Key từ Zigbee 3.0 Install Code)
AES_KEY = b"IoT_prj_gr21"

# Mã hex định danh cho các phân vùng (Zone ID)
ZONE_CODES = {
    "pantry": 0x01,
    "storage": 0x02,
    "prvt_meeting": 0x03,
    "office_1": 0x04,
    "office_2": 0x05,
    "lobby": 0x06,
    "connect": 0x07,
    "director": 0x08,
    "finance_mng": 0x09,
    "finace_mng": 0x09, # Hỗ trợ sai chính tả từ file đặc tả
    "meeting": 0x0A,
    "technical_mng": 0x0B,
    "vice_director": 0x0C,
    
    # Cửa đi (Doors)
    "door_01": 0xD1,
    "door_02": 0xD2,
    "door_03": 0xD3,
    "door_04": 0xD4,
    "door_05": 0xD5,
    
    # Cửa sổ (Windows)
    "wd_01": 0xE1,
    "wd_02": 0xE2,
    "wd_03": 0xE3,
    "wd_04": 0xE4,
    "wd_05": 0xE5,
    "wd_06": 0xE6
}

# Ánh xạ ngược từ Code sang Tên đối tượng
CODE_TO_ZONE = {
    0x01: "pantry",
    0x02: "storage",
    0x03: "prvt_meeting",
    0x04: "office_1",
    0x05: "office_2",
    0x06: "lobby",
    0x07: "connect",
    0x08: "director",
    0x09: "finance_mng",
    0x0A: "meeting",
    0x0B: "technical_mng",
    0x0C: "vice_director",
    0xD1: "door_01",
    0xD2: "door_02",
    0xD3: "door_03",
    0xD4: "door_04",
    0xD5: "door_05",
    0xE1: "wd_01",
    0xE2: "wd_02",
    0xE3: "wd_03",
    0xE4: "wd_04",
    0xE5: "wd_05",
    0xE6: "wd_06"
}

# Mã hex cho các loại thiết bị & cảm biến
TYPE_CODES = {
    # Cảm biến
    "dht22": 0x11,
    "mq2": 0x12,
    "lm393": 0x13,
    "mc38": 0x14,
    
    # Thiết bị chấp hành
    "light": 0x21,
    "ahu": 0x22,
    "curtain": 0x23
}

CODE_TO_TYPE = {code: name for name, code in TYPE_CODES.items()}

# Danh sách Zones tiêu chuẩn
ZONES = ["pantry", "storage", "prvt_meeting", "office_1", "office_2", "lobby", 
         "connect", "director", "finance_mng", "meeting", "technical_mng", "vice_director"]

# Ngưỡng tiền xử lý (Filtering thresholds) để giảm tải cho Server
TEMP_THRESHOLD = 0.2  # °C
HUMID_THRESHOLD = 0.5 # %
LIGHT_THRESHOLD = 10  # lux
