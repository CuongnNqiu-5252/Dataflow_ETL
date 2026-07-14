import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import joblib
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_synthetic_data(num_normal=1000, num_anomalies=50):
    """
    Tạo dữ liệu giả lập cho việc huấn luyện. 
    Trong thực tế, bạn sẽ lấy dữ liệu này từ BigQuery.
    """
    logging.info("Đang tạo dữ liệu giả lập...")
    # 1. Dữ liệu bình thường (Normal)
    # pH thường dao động quanh 6.5 - 7.5
    normal_ph = np.random.normal(loc=7.0, scale=0.3, size=num_normal)
    # Nhiệt độ thường dao động quanh 25 - 30 độ C
    normal_temp = np.random.normal(loc=27.5, scale=1.5, size=num_normal)
    
    df_normal = pd.DataFrame({'pH': normal_ph, 'temperature_c': normal_temp})
    
    # 2. Dữ liệu bất thường (Anomalies)
    # Giả lập sự cố: pH quá thấp/cao hoặc nhiệt độ quá cao
    anomaly_ph = np.random.uniform(low=2.0, high=10.0, size=num_anomalies)
    anomaly_temp = np.random.uniform(low=15.0, high=45.0, size=num_anomalies)
    
    df_anomalies = pd.DataFrame({'pH': anomaly_ph, 'temperature_c': anomaly_temp})
    
    # Gộp lại thành một tập dữ liệu (Không gán nhãn cho mô hình huấn luyện)
    df_train = pd.concat([df_normal, df_anomalies], ignore_index=True)
    # Trộn đều dữ liệu
    df_train = df_train.sample(frac=1).reset_index(drop=True)
    
    return df_train

def train_model(df):
    """
    Huấn luyện mô hình Isolation Forest để phát hiện bất thường.
    """
    logging.info("Bắt đầu huấn luyện mô hình Isolation Forest...")
    
    # Khởi tạo mô hình
    # contamination: Tỷ lệ dữ liệu bất thường dự kiến trong tập dữ liệu (VD: 5%)
    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    
    # Huấn luyện mô hình (Chỉ cần truyền đặc trưng, không cần nhãn)
    model.fit(df[['pH', 'temperature_c']])
    
    logging.info("Huấn luyện hoàn tất!")
    return model

def test_model(model):
    """
    Thử nghiệm dự đoán với một vài mẫu dữ liệu.
    """
    logging.info("Thử nghiệm mô hình với dữ liệu mới...")
    
    test_data = pd.DataFrame([
        {'pH': 7.1, 'temperature_c': 28.0},  # Bình thường
        {'pH': 7.3, 'temperature_c': 27.5},  # Bình thường
        {'pH': 3.5, 'temperature_c': 38.0},  # Bất thường (Axit + Nóng)
        {'pH': 9.5, 'temperature_c': 20.0}   # Bất thường (Kiềm)
    ])
    
    # Dự đoán
    # Kết quả: 1 (Bình thường - Inlier), -1 (Bất thường - Outlier/Anomaly)
    predictions = model.predict(test_data[['pH', 'temperature_c']])
    
    test_data['prediction'] = predictions
    test_data['status'] = test_data['prediction'].apply(lambda x: 'Bình thường' if x == 1 else 'Bất thường (!)')
    
    print("\nKết quả dự đoán thử nghiệm:")
    print(test_data.to_string(index=False))

def save_model(model, filename='model.pkl'):
    """
    Lưu mô hình ra file để sau này có thể load lên Vertex AI.
    """
    logging.info(f"Đang lưu mô hình vào file {filename}...")
    joblib.dump(model, filename)
    logging.info("Lưu mô hình thành công.")

if __name__ == "__main__":
    # 1. Chuẩn bị dữ liệu (Giả lập)
    # TODO: Thay thế hàm này bằng code đọc từ BigQuery trong thực tế
    # Ví dụ: df = pd.read_gbq('SELECT PH, temperature_c FROM `project.dataset.table`')
    df_training = generate_synthetic_data(num_normal=1000, num_anomalies=50)
    
    # 2. Huấn luyện mô hình
    anomaly_model = train_model(df_training)
    
    # 3. Test thử mô hình
    test_model(anomaly_model)
    
    # 4. Lưu mô hình
    save_model(anomaly_model, 'anomaly_model.pkl')
    
    logging.info("-" * 40)
    logging.info("TIẾP THEO:")
    logging.info("1. File 'anomaly_model.pkl' đã được tạo.")
    logging.info("2. Để sử dụng trong Vertex AI, bạn cần upload file này lên Google Cloud Storage (GCS).")
    logging.info("3. Đăng ký mô hình trong Vertex AI Model Registry và tạo Endpoint.")
