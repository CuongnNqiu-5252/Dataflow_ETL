import os
from google.cloud import aiplatform

# Sử dụng Service Account có sẵn trong thư mục
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\Admin\\Documents\\DT_demo\\key.json"
PROJECT_ID = "n8nproject-461516"
REGION = "asia-southeast1"
ENDPOINT_ID = "785159054371586048"

print("Đang khởi tạo kết nối tới Vertex AI Endpoint...")
aiplatform.init(project=PROJECT_ID, location=REGION)
endpoint = aiplatform.Endpoint(ENDPOINT_ID)

print("\n🟢 TEST 1: Nước Sạch (pH 7.2, Temp 26.5)")
test_1 = [[7.2, 26.5]]
res_1 = endpoint.predict(instances=test_1)
print(f" => Kết quả AI trả về: {res_1.predictions[0]} (1.0 = Bình thường)")

print("\n🔴 TEST 2: Nước Ô Nhiễm (pH 3.5, Temp 40.0)")
test_2 = [[3.5, 40.0]]
res_2 = endpoint.predict(instances=test_2)
print(f" => Kết quả AI trả về: {res_2.predictions[0]} (-1.0 = Ô nhiễm/Bất thường)")
