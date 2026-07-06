import * as pulumi from "@pulumi/pulumi";
import * as gcp from "@pulumi/gcp";

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
    name: "iot-water-quality-streaming-pipeline-v2",
    region: "asia-southeast1",
    containerSpecGcsPath: pulumi.interpolate`gs://${dataflowBucket.name}/templates/iot-pipeline-spec-v2.json`,
    network: "headless-vpc",
    subnetwork: "regions/asia-southeast1/subnetworks/headless-subnet-v2",
    parameters: {
        input_subscription: iotSubscription.id,
        output_table: pulumi.interpolate`${bqDataset.project}:${bqDataset.datasetId}.${bqTable.tableId}`,
    },
    onDelete: "cancel",
});

export const bucketUrl = pulumi.interpolate`gs://${dataflowBucket.name}`;
export const pubsubTopicName = iotTopic.id;
export const bigqueryTableName = pulumi.interpolate`${bqDataset.project}:${bqDataset.datasetId}.${bqTable.tableId}`;
export const dataflowJobId = dataflowJob.id;
export const dataflowJobState = dataflowJob.state;