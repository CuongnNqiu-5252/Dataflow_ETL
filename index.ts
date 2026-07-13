import * as pulumi from "@pulumi/pulumi";
import * as gcp from "@pulumi/gcp";

// Lấy version từ Github Actions (Git SHA). Nếu chạy local thì mặc định là "latest"
const templateVersion = process.env.TEMPLATE_VERSION || "latest";

// Các biến cấu hình cho MongoDB (Nếu có)
const mongoUri = process.env.MONGODB_URI;
const mongoDb = process.env.MONGO_DB || "DT_TWIN";
const mongoCollection = process.env.MONGO_COLLECTION || "DT_TWIN_SENSOR";


const dataflowBucket = new gcp.storage.Bucket("water-quality-dataflow-bucket", {
    location: "asia-southeast1", // Đặt tại Singapore để tối ưu độ trễ cho Việt Nam
    forceDestroy: true,          // Cho phép xóa bucket kể cả khi có file bên trong
    uniformBucketLevelAccess: true,
});

const iotTopic = new gcp.pubsub.Topic("iot-telemetry-topic", {
    messageRetentionDuration: "86400s",
});

const iotSubscription = new gcp.pubsub.Subscription("iot-telemetry-sub", {
    topic: iotTopic.name,
    enableExactlyOnceDelivery: true,
    ackDeadlineSeconds: 60,
    messageRetentionDuration: "259200s",
    retainAckedMessages: false,
});

const bqDataset = new gcp.bigquery.Dataset("water_quality_ds", {
    datasetId: "water_quality_analytics",
    location: "asia-southeast1",
});

const bqTable = new gcp.bigquery.Table("sensor_observations_table", {
    datasetId: bqDataset.datasetId,
    tableId: "sensor_observations",
    schema: JSON.stringify([
        { name: "station_id", type: "STRING", mode: "REQUIRED", description: "Mã trạm đo" },
        { name: "timestamp", type: "TIMESTAMP", mode: "REQUIRED", description: "Thời gian kết thúc cửa sổ (Window)" },
        { name: "PH", type: "FLOAT", mode: "NULLABLE", description: "Độ pH trung bình" },
        { name: "temperature_c", type: "FLOAT", mode: "NULLABLE", description: "Nhiệt độ trung bình" },
        { name: "quality_flag", type: "STRING", mode: "NULLABLE", description: "Cờ đánh giá chất lượng dữ liệu" },
    ]),
    timePartitioning: { type: "DAY", field: "timestamp" },
    clusterings: ["station_id"],
    deletionProtection: false,
});

const dataflowJob = new gcp.dataflow.FlexTemplateJob("water-quality-streaming-job", {
    // Đổi tên job theo version để tránh lỗi trùng tên khi Pulumi thay thế job cũ
    name: `iot-water-quality-pipeline-${templateVersion.substring(0, 7)}`,
    region: "asia-southeast1",
    // Trỏ tới file spec JSON có chứa mã version
    containerSpecGcsPath: pulumi.interpolate`gs://${dataflowBucket.name}/templates/iot-pipeline-spec-${templateVersion}.json`,
    network: "headless-vpc",
    subnetwork: "regions/asia-southeast1/subnetworks/headless-subnet-v2",
    parameters: {
        input_subscription: iotSubscription.id,
        output_table: pulumi.interpolate`${bqDataset.project}:${bqDataset.datasetId}.${bqTable.tableId}`,
        ...(mongoUri && { mongo_uri_secret: mongoUri }),
        ...(mongoDb && { mongo_db: mongoDb }),
        ...(mongoCollection && { mongo_collection: mongoCollection }),
    },
    onDelete: "cancel",
});
// ============================================================================
// GIÁM SÁT & KIỂM TOÁN TỰ ĐỘNG BẰNG CLOUD LOGGING / MONITORING
// ============================================================================

// 1. Tạo Log-based Metric để đếm số lượng lỗi Pipeline (Parse Error hoặc Schema Mismatch)
const pipelineErrorsMetric = new gcp.logging.Metric("pipeline-errors-metric", {
    name: "dataflow_pipeline_errors",
    // Lọc các log có từ khóa lỗi mà chúng ta đã định nghĩa trong file Python
    filter: `resource.type="dataflow_step" AND severity>=WARNING AND (textPayload:"PIPELINE_PARSE_ERROR" OR textPayload:"SCHEMA_MISMATCH_OR_OUT_OF_RANGE")`,
    description: "Đếm số lượng lỗi parse hoặc dữ liệu sai lệch từ Dataflow",
    metricDescriptor: {
        metricKind: "DELTA",
        valueType: "INT64",
    },
});

// 2. Kênh thông báo (Notification Channel) - Ở đây dùng Email
// (Với Zalo/SMS bạn sẽ cần dùng type = "webhook_tokenauth" trỏ tới API trung gian)
const emailChannel = new gcp.monitoring.NotificationChannel("alert-email", {
    type: "email",
    labels: {
        email_address: "cngo98279@gmail.com", // Đổi thành Email của đội vận hành
    },
    description: "Kênh thông báo lỗi Dataflow khẩn cấp",
});

// 3. Chính sách cảnh báo (Alert Policy)
const alertPolicy = new gcp.monitoring.AlertPolicy("pipeline-error-alert", {
    displayName: "Cảnh báo Lỗi Dataflow Pipeline - Vượt ngưỡng",
    combiner: "OR",
    conditions: [{
        displayName: "Số lỗi vượt quá 10 lần trong 5 phút",
        conditionThreshold: {
            // Liên kết với Metric vừa tạo ở trên
            filter: pulumi.interpolate`metric.type="logging.googleapis.com/user/${pipelineErrorsMetric.name}" AND resource.type="global"`,
            comparison: "COMPARISON_GT",
            thresholdValue: 10, // Ngưỡng cho phép (Ví dụ: 10 lỗi)
            duration: "0s",
            aggregations: [{
                alignmentPeriod: "300s", // Cửa sổ theo dõi: 5 phút
                crossSeriesReducer: "REDUCE_SUM",
                perSeriesAligner: "ALIGN_DELTA",
            }],
        },
    }],
    notificationChannels: [emailChannel.id],
    alertStrategy: {
        autoClose: "1800s", // Tự động đóng cảnh báo sau 30 phút nếu không có lỗi mới
    },
});

export const bucketUrl = pulumi.interpolate`gs://${dataflowBucket.name}`;
export const pubsubTopicName = iotTopic.id;
export const bigqueryTableName = pulumi.interpolate`${bqDataset.project}:${bqDataset.datasetId}.${bqTable.tableId}`;
export const dataflowJobId = dataflowJob.id;
export const dataflowJobState = dataflowJob.state;