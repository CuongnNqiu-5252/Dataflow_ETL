import argparse
import json
import logging
from datetime import datetime

import apache_beam as beam
import apache_beam.pvalue as pvalue
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import window
from apache_beam.io.mongodbio import WriteToMongoDB

VALID_DATA_TAG = 'valid'
DLQ_TAG = 'dlq'

# 1. Hàm Parse dữ liệu JSON từ Pub/Sub
class ParseIoTData(beam.DoFn):
    """
    Nhận chuỗi byte từ Pub/Sub, parse thành Dictionary (JSON).
    Đồng thời gán khóa (key) là station_id để chuẩn bị cho việc gom nhóm.
    """
    def process(self, element):
        try:
            # Decode byte string thành dạng text và parse JSON
            record = json.loads(element.decode('utf-8'))
            
            # Trích xuất các trường quan trọng (Ví dụ: Trạm, pH, Nhiệt độ)
            station_id = record.get('station_id')
            ph = float(record.get('PH', 0))
            temp = float(record.get('temperature_c', 0))
            
            # Kiểm tra lỗi cảm biến phần cứng (giới hạn vật lý)
            if ph < 0 or ph > 14 or temp < -50 or temp > 100:
                error_msg = 'HARDWARE ERROR: Vượt quá giới hạn vật lý của cảm biến'
                logging.warning(f"SCHEMA_MISMATCH_OR_OUT_OF_RANGE: {error_msg}. Payload: {element}")
                error_record = {
                    'error_message': error_msg,
                    'raw_payload': element.decode('utf-8', errors='ignore'),
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                }
                yield pvalue.TaggedOutput(DLQ_TAG, error_record)
            else:
                # Trả về một Tuple dạng (Key, Value) để tiện tính toán trung bình
                # Key = station_id, Value = dict chứa các thông số
                yield pvalue.TaggedOutput(VALID_DATA_TAG, (station_id, {'ph': ph, 'temp': temp}))
        except Exception as e:
            # Nếu dữ liệu bị lỗi (Corrupted), log lại để xử lý sau (Dead-letter pattern)
            logging.error(f"PIPELINE_PARSE_ERROR: {str(e)}. Payload: {element}")
            error_record = {
                'error_message': str(e),
                # decode lại một lần nữa với errors='ignore' để bắt dù payload là byte lỗi
                'raw_payload': element.decode('utf-8', errors='ignore'),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
            yield pvalue.TaggedOutput(DLQ_TAG, error_record)

# 2. Hàm Định dạng lại dữ liệu trước khi ném vào BigQuery
class FormatForBigQuery(beam.DoFn):
    """
    Chuyển đổi dữ liệu đã được tổng hợp (tính trung bình) thành định dạng
    phù hợp với Table Schema của BigQuery.
    """
    def process(self, element, window=beam.DoFn.WindowParam):
        # Element lúc này có dạng: ('CT CANAL 001', {'ph': 7.2, 'temp': 29.5})
        station_id, metrics = element
        
        # Lấy thời gian kết thúc của Cửa sổ (Window) làm timestamp cho bản ghi
        window_end_time = window.end.to_utc_datetime().isoformat() + 'Z'
        
        row = {
            'station_id': station_id,
            'timestamp': window_end_time,
            'PH': round(metrics['ph'], 2),
            'temperature_c': round(metrics['temp'], 2),
            'quality_flag': 'VALID', # Gắn cờ dữ liệu đã qua xử lý
            'prediction_result': metrics.get('prediction_result', 'N/A')
        }
        yield row

# 3. Hàm tính trung bình tùy chỉnh cho nhiều thông số cùng lúc
class CalculateAverage(beam.CombineFn):
    """
    Hàm Combine tự định nghĩa để tính trung bình cộng cho cả pH và Nhiệt độ
    cùng một lúc trong một Cửa sổ thời gian.
    """
    def create_accumulator(self):
        # sum_ph, sum_temp, count
        return (0.0, 0.0, 0)

    def add_input(self, sum_count, input_dict):
        (sum_ph, sum_temp, count) = sum_count
        return sum_ph + input_dict['ph'], sum_temp + input_dict['temp'], count + 1

    def merge_accumulators(self, accumulators):
        sums_ph, sums_temp, counts = zip(*accumulators)
        return sum(sums_ph), sum(sums_temp), sum(counts)

    def extract_output(self, sum_count):
        (sum_ph, sum_temp, count) = sum_count
        if count == 0:
            return {'ph': 0, 'temp': 0}
        return {'ph': sum_ph / count, 'temp': sum_temp / count}
class WriteToMongoDBSecurely(beam.DoFn):
    def __init__(self, project_id, secret_name, db_name, collection_name):
        self.project_id = project_id
        self.secret_name = secret_name
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = None
    # Khởi tạo kết nối và gọi Secret Manager trên mỗi worker
    def setup(self):
        from google.cloud import secretmanager
        import pymongo
        
        # 1. Gọi Secret Manager API
        client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{self.project_id}/secrets/{self.secret_name}/versions/latest"
        
        # 2. Lấy Mongo URI từ Secret Manager vào bộ nhớ tạm thời
        response = client.access_secret_version(request={"name": secret_path})
        mongo_uri = response.payload.data.decode("UTF-8")
        
        # 3. Khởi tạo connection 
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
    def process(self, element):
        # Ghi dữ liệu vào MongoDB
        self.collection.insert_one(element)
    # Đóng kết nối và giải phóng bộ nhớ sau khi Worker xử lý xong
    def teardown(self):
        if self.client:
            self.client.close()

from google.cloud import aiplatform

class PredictWithVertexAI(beam.DoFn):
    def __init__(self, project_id, region, endpoint_id):
        self.project_id = project_id
        self.region = region
        self.endpoint_id = endpoint_id
        self.endpoint = None

    def setup(self):
        # Khởi tạo client Vertex AI trên worker
        aiplatform.init(project=self.project_id, location=self.region)
        self.endpoint = aiplatform.Endpoint(self.endpoint_id)

    def process(self, element):
        station_id, metrics = element
        try:
            # Chuẩn bị dữ liệu đầu vào cho model (phụ thuộc vào model bạn deploy)
            # Giả sử model nhận mảng 2 chiều [pH, temp]
            instances = [[metrics['ph'], metrics['temp']]]
            
            # Gọi API dự đoán
            prediction = self.endpoint.predict(instances=instances)
            
            # Lấy kết quả trả về
            predicted_value = prediction.predictions[0]
            metrics['prediction_result'] = str(predicted_value)
        except Exception as e:
            logging.error(f"VERTEX_AI_ERROR: Failed to predict. Reason: {e}")
            metrics['prediction_result'] = "ERROR"
            
        yield (station_id, metrics)

def run():
    # Khởi tạo các tham số dòng lệnh (Pipeline Options)
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_subscription', required=True, help='Pub/Sub Subscription ID')
    parser.add_argument('--output_table', required=True, help='BigQuery Table (project:dataset.table)')
    parser.add_argument('--mongo_uri_secret', required=False, help='MongoDB Atlas Connection URI')
    parser.add_argument('--mongo_db', required=False, help='MongoDB Database Name')
    parser.add_argument('--mongo_collection', required=False, help='MongoDB Collection Name')
    parser.add_argument('--vertex_endpoint_id', required=False, help='Vertex AI Endpoint ID')
    parser.add_argument('--vertex_project_id', required=False, help='Vertex AI Project ID')
    parser.add_argument('--vertex_region', required=False, help='Vertex AI Region (e.g. us-central1)')
    known_args, pipeline_args = parser.parse_known_args()

    # Thiết lập tùy chọn cho Dataflow
    pipeline_options = PipelineOptions(pipeline_args)
    # BẮT BUỘC BẬT CHẾ ĐỘ STREAMING
    pipeline_options.view_as(StandardOptions).streaming = True 

    # Lược đồ (Schema) của bảng BigQuery
    bq_schema = 'station_id:STRING, timestamp:TIMESTAMP, PH:FLOAT, temperature_c:FLOAT, quality_flag:STRING, prediction_result:STRING'
    dlq_schema = 'error_message:STRING, raw_payload:STRING, timestamp:TIMESTAMP'
    
    # Khởi tạo Pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        parsed_data = (
            p
            # BƯỚC 1: Đọc dữ liệu Real-time từ Pub/Sub
            | 'Doc_Tu_PubSub' >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
            
            # BƯỚC 2: Giải mã JSON và trích xuất dữ liệu
            | 'Parse_JSON' >> beam.ParDo(ParseIoTData()).with_outputs(VALID_DATA_TAG, DLQ_TAG)
        )

        # Nhánh 1: Xử lý dữ liệu hợp lệ
        valid_data_formatted = (
            parsed_data[VALID_DATA_TAG]
            # BƯỚC 3: WINDOWING (Tumbling Window)
            | 'Gom_Cua_So_5_Phut' >> beam.WindowInto(window.FixedWindows(300))
            
            # BƯỚC 4: Tính trung bình cộng các chỉ số đo được trong 5 phút đó theo từng trạm
            | 'Tinh_Trung_Binh' >> beam.CombinePerKey(CalculateAverage())
        )

        # BƯỚC 4.5: Dự đoán bằng Vertex AI (Nếu có endpoint)
        if known_args.vertex_endpoint_id:
            valid_data_formatted = (
                valid_data_formatted
                | 'Du_Doan_VertexAI' >> beam.ParDo(PredictWithVertexAI(
                    known_args.vertex_project_id,
                    known_args.vertex_region,
                    known_args.vertex_endpoint_id
                ))
            )

        valid_data_formatted = (
            valid_data_formatted
            # BƯỚC 5: Đóng gói lại thành định dạng BigQuery
            | 'Chuan_Hoa_BigQuery' >> beam.ParDo(FormatForBigQuery())
        )

        # BƯỚC 6: Ghi dữ liệu vào BigQuery
        (
            valid_data_formatted
            | 'Ghi_Vao_BigQuery' >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=bq_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        # BƯỚC 7: Ghi dữ liệu vào MongoDB Atlas (Nếu cấu hình được cung cấp)
        # Sửa lỗi cú pháp biến known_args: argparse tự động chuyển dấu gạch ngang '-' thành dấu gạch dưới '_'
        if known_args.mongo_uri_secret and known_args.mongo_db and known_args.mongo_collection:
            (
                valid_data_formatted
                | 'Ghi_Vao_MongoDB' >> beam.ParDo(WriteToMongoDBSecurely(
                    project_id='n8nproject-461516',
                    secret_name=known_args.mongo_uri_secret,
                    db_name=known_args.mongo_db,
                    collection_name=known_args.mongo_collection
                ))
            )

        # Nhánh 2: Xử lý dữ liệu bị lỗi (Dead-Letter Queue)
        (
            parsed_data[DLQ_TAG]
            | 'Ghi_Vao_Bang_Loi_DLQ' >> beam.io.WriteToBigQuery(
                known_args.output_table + "_DLQ", # Tự động tạo bảng có hậu tố _DLQ
                schema=dlq_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()