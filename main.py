from google.cloud import pubsub_v1
import os
import json
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

# Cấu hình biến môi trường cho Google Cloud từ file key.json
key_path = os.path.join(os.path.dirname(__file__), 'key.json')
if os.path.exists(key_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

app = FastAPI(
    title="Broker cho Data Engineering Pipeline",
    version = "1.0.0"
)
PROJECT_ID = os.environ.get("PROJECT_ID", "n8nproject-461516")
TOPIC_ID = os.environ.get("TOPIC_ID", "iot-telemetry-topic")
API_SECRET_KEY = os.environ.get("API_SECRET_KEY", "mat-khau-bi-mat-esp32").strip()

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

class TelemetryData(BaseModel):
    station_id: str = 'UNKNOWN'
    temperature_c: Optional[float] = None
    PH: Optional[float] = 0.0
@app.post('/api/telemetry')
async def receive_telemetry(data: TelemetryData, x_api_key: str = Header(None)):
    # 1. Bảo mật: Kiểm tra API Key từ Header
    print(f"Key từ ESP32 gửi lên: {x_api_key}")
    
    if x_api_key != API_SECRET_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized. Sai mat khau!")

    # 2. Xử lý dữ liệu
    try:
        payload = {
            "station_id": data.station_id,
            "temperature_c": data.temperature_c,
            "PH": data.PH
        }
        
        # 3. Đẩy vào Pub/Sub
        data_bytes = json.dumps(payload).encode("utf-8")
        future = publisher.publish(topic_path, data_bytes)
        message_id = future.result()
        
        print(f"Thành công! Đã đẩy message {message_id} vào PubSub.")
        return {"status": "success", "message_id": message_id}

    except Exception as e:
        print(f"Lỗi hệ thống: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")