import os
from google.cloud import storage
from google.cloud import aiplatform

# ==========================================
# CẤU HÌNH (Hãy thay đổi nếu cần thiết)
# ==========================================
# Dự án GCP của bạn (Lấy từ project_id n8nproject-461516 bạn đang dùng)
PROJECT_ID = os.environ.get("PROJECT_ID", "n8nproject-461516")
REGION = "asia-southeast1"

# Tên bucket trên Cloud Storage (Phải là duy nhất trên toàn cầu)
BUCKET_NAME = f"water-quality-models-{PROJECT_ID}"
MODEL_FILE_NAME = "anomaly_model.pkl"

# Tên model và endpoint khi hiển thị trên Vertex AI
MODEL_DISPLAY_NAME = "water_quality_anomaly_model"
ENDPOINT_DISPLAY_NAME = "water_quality_endpoint"
# ==========================================

def upload_to_gcs(bucket_name, source_file_name):
    """Tạo bucket (nếu chưa có) và upload file model lên GCS"""
    storage_client = storage.Client(project=PROJECT_ID)
    
    # Kiểm tra hoặc tạo bucket mới
    try:
        bucket = storage_client.get_bucket(bucket_name)
        print(f"✅ Đã tìm thấy bucket: {bucket_name}")
    except Exception:
        print(f"⚠️ Không tìm thấy bucket {bucket_name}. Đang tạo mới...")
        bucket = storage_client.create_bucket(bucket_name, location=REGION)
        print(f"✅ Đã tạo bucket: {bucket_name}")
        
    # Vertex AI yêu cầu file phải có tên chính xác là 'model.pkl' hoặc 'model.joblib'
    # khi dùng container có sẵn của scikit-learn.
    destination_blob_name = f"v1/model.pkl" 
    blob = bucket.blob(destination_blob_name)
    
    print(f"Đang upload {source_file_name} lên gs://{bucket_name}/{destination_blob_name}...")
    blob.upload_from_filename(source_file_name)
    print("✅ Upload hoàn tất!")
    
    # Trả về URI thư mục chứa file model
    return f"gs://{bucket_name}/v1/"

def deploy_to_vertex(gcs_artifact_uri):
    """Đăng ký model vào Model Registry và Deploy ra Endpoint"""
    print("Đang khởi tạo kết nối Vertex AI...")
    aiplatform.init(project=PROJECT_ID, location=REGION)
    
    # 1. Upload (Đăng ký) Model vào Registry
    # Dùng container có sẵn của Google cho scikit-learn
    print(f"Đang đăng ký model từ {gcs_artifact_uri}...")
    model = aiplatform.Model.upload(
        display_name=MODEL_DISPLAY_NAME,
        artifact_uri=gcs_artifact_uri,
        serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest" 
    )
    print(f"✅ Đăng ký model thành công! Model ID: {model.name}")
    
    # 2. Tạo Endpoint
    print("Đang tạo Endpoint...")
    endpoint = aiplatform.Endpoint.create(display_name=ENDPOINT_DISPLAY_NAME)
    print(f"✅ Đã tạo Endpoint: {endpoint.name}")
    
    # 3. Deploy model ra Endpoint (Quá trình này tốn từ 10 - 15 phút)
    print("Đang Deploy model lên Endpoint (Xin vui lòng chờ khoảng 10-15 phút, KHÔNG TẮT MÁY)...")
    model.deploy(
        endpoint=endpoint,
        machine_type="n1-standard-2", # Loại máy ảo cơ bản rẻ nhất có thể
        min_replica_count=1,
        max_replica_count=1
    )
    
    print("\n" + "="*50)
    print("🚀 ĐÃ DEPLOY THÀNH CÔNG LÊN VERTEX AI!")
    print(f"🎉 ENDPOINT ID CỦA BẠN LÀ: {endpoint.name}")
    print("="*50)
    print("\nBạn hãy copy ENDPOINT ID này và truyền vào khi chạy Dataflow nhé!")

if __name__ == "__main__":
    if not os.path.exists(MODEL_FILE_NAME):
        print(f"❌ LỖI: Không tìm thấy file {MODEL_FILE_NAME}. Bạn cần chạy train_anomaly_model.py trước!")
        exit(1)
        
    print("Bắt đầu quá trình đưa mô hình lên Google Cloud...")
    
    # 1. Upload model lên GCS
    artifact_uri = upload_to_gcs(BUCKET_NAME, MODEL_FILE_NAME)
    
    # 2. Deploy lên Vertex AI
    deploy_to_vertex(artifact_uri)
