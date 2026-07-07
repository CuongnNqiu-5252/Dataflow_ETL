# 🌊 IoT Water Quality Monitoring Pipeline

A real-time data engineering pipeline that collects IoT sensor data (water quality), processes it using Apache Beam on Google Cloud Dataflow, and stores the results in both BigQuery and MongoDB Atlas.

---

## 🏗️ Infrastructure Architecture

### Full System Overview

```mermaid
graph TB
    subgraph Devices["📡 Data Sources"]
        ESP32["ESP32 Sensor<br/>(Hardware)"]
        SIM["simulator.html<br/>(Web Simulator)"]
    end

    subgraph CloudRun["☁️ Google Cloud Run"]
        FASTAPI["FastAPI Broker<br/>main.py<br/>─────────────<br/>• API Key Auth<br/>• POST /api/telemetry"]
    end

    subgraph PubSub["📨 Google Cloud Pub/Sub"]
        TOPIC["Topic<br/>iot-telemetry-topic"]
        SUB["Subscription<br/>iot-telemetry-sub<br/>(3-day retention)"]
    end

    subgraph Dataflow["⚙️ Google Cloud Dataflow (Streaming)"]
        direction TB
        READ["Step 1: Doc_Tu_PubSub<br/>ReadFromPubSub"]
        PARSE["Step 2: Parse_JSON<br/>Decode bytes to Python Dict"]
        WINDOW["Step 3: Gom_Cua_So_5_Phut<br/>5-minute Fixed Window"]
        VALID["Step 4: Data Validation<br/>Flag anomalies"]
        FORMAT["Step 5: Format Output"]
        BQ_WRITE["Step 6: Ghi_Vao_BigQuery"]
        MONGO_WRITE["Step 7: Ghi_Vao_MongoDB"]

        READ --> PARSE --> WINDOW --> VALID --> FORMAT
        FORMAT --> BQ_WRITE
        FORMAT --> MONGO_WRITE
    end

    subgraph Storage["🗄️ Storage Layer"]
        BQ["BigQuery<br/>water_quality_analytics<br/>.sensor_observations"]
        MONGO["MongoDB Atlas<br/>water_quality_db<br/>.sensor_data"]
    end

    subgraph GCS["🪣 Google Cloud Storage"]
        BUCKET["GCS Bucket<br/>water-quality-dataflow-bucket-*<br/>─────────────<br/>Dataflow Template JSON"]
    end

    ESP32 -->|"POST /api/telemetry + x-api-key"| FASTAPI
    SIM -->|"POST /api/telemetry + x-api-key"| FASTAPI
    FASTAPI -->|"publish bytes"| TOPIC
    TOPIC --> SUB
    SUB -->|"Streaming Pull 24/7"| READ
    BQ_WRITE --> BQ
    MONGO_WRITE --> MONGO
    BUCKET -.->|"Template Spec JSON"| Dataflow
```

---

### 🔄 CI/CD Deployment Flow

```mermaid
flowchart LR
    DEV["👨‍💻 Developer<br/>Local Machine"]

    subgraph GitHub["GitHub"]
        REPO["Repository<br/>main branch"]
        ACTIONS["GitHub Actions<br/>deploy-dataflow.yml"]
        SECRETS["🔐 Secrets<br/>─────────────<br/>GCP_CREDENTIALS<br/>PULUMI_ACCESS_TOKEN<br/>BUCKET_NAME<br/>PROJECT_ID<br/>MONGODB_URI<br/>MONGO_DB<br/>MONGO_COLLECTION"]
    end

    subgraph GoogleCloud["Google Cloud"]
        AR["Artifact Registry<br/>iot-dataflow:{sha}"]
        GCS2["GCS Bucket<br/>iot-pipeline-spec-{sha}.json"]
        DF["Dataflow Job<br/>iot-water-quality-pipeline-{sha}"]
    end

    DEV -->|"git push"| REPO
    REPO -->|"triggers"| ACTIONS
    SECRETS -.->|"inject"| ACTIONS

    ACTIONS -->|"1. docker build + push"| AR
    ACTIONS -->|"2. gcloud flex-template build"| GCS2
    ACTIONS -->|"3. pulumi up"| DF
    AR -.->|"image ref"| GCS2
    GCS2 -.->|"template spec"| DF
```

---

### 🔐 Security Model

```mermaid
graph LR
    APIKEY["API Key Header<br/>x-api-key<br/>ESP32 to FastAPI"]
    SA["Service Account<br/>key.json<br/>Local / Cloud Run to GCP"]
    GHSA["GitHub Secret<br/>GCP_CREDENTIALS<br/>Actions to GCP"]

    APIKEY -->|"validated by"| FASTAPI2["FastAPI Auth Middleware"]
    SA -->|"ADC credentials"| GCP["Google Cloud APIs"]
    GHSA -->|"google-github-actions/auth"| GCP
```

---

## 📁 Project Structure

```
DT_demo/
├── 📄 main.py                          # FastAPI broker (Cloud Run)
├── 📄 iot_water_quality_pipeline.py    # Apache Beam pipeline (Dataflow)
├── 📄 index.ts                         # Pulumi IaC (Infrastructure as Code)
├── 📄 metadata.json                    # Dataflow Flex Template parameters
├── 📄 Dockerfile                       # Docker image for Cloud Run (FastAPI)
├── 📄 Dockerfile.dataflow              # Docker image for Dataflow workers
├── 📄 requirements.txt                 # Python dependencies
├── 📄 simulator.html                   # Web UI to send test telemetry
├── 📄 Pulumi.yaml                      # Pulumi project config
├── 📄 .env                             # Local environment variables (gitignored)
└── 📁 .github/
    └── 📁 workflows/
        └── 📄 deploy-dataflow.yml      # GitHub Actions CI/CD pipeline
```

---

## ⚙️ Infrastructure Components (Managed by Pulumi)

| Component | Service | Purpose |
|-----------|---------|---------|
| **GCS Bucket** | Google Cloud Storage | Store Dataflow Flex Template spec JSON files |
| **Pub/Sub Topic** | Google Cloud Pub/Sub | Message queue entry point |
| **Pub/Sub Subscription** | Google Cloud Pub/Sub | Dataflow pull endpoint (3-day retention) |
| **BigQuery Dataset** | Google BigQuery | Analytics data warehouse |
| **BigQuery Table** | Google BigQuery | `sensor_observations` — partitioned by day, clustered by station |
| **Dataflow Job** | Google Cloud Dataflow | Streaming pipeline — Pub/Sub → BigQuery + MongoDB |

---

## 🚀 Quick Start

### Prerequisites
- Google Cloud SDK (`gcloud`)
- Pulumi CLI
- Node.js 18+
- Python 3.10+
- Docker

### Local Development

```bash
# 1. Set environment variables
cp .env.example .env
# Fill in your values in .env

# 2. Run FastAPI locally
pip install -r requirements.txt
uvicorn main:app --reload

# 3. Deploy infrastructure manually
pulumi up
```

### GitHub Secrets Required

| Secret Name | Description |
|-------------|-------------|
| `GCP_CREDENTIALS` | Full content of Service Account `key.json` |
| `PULUMI_ACCESS_TOKEN` | Pulumi Cloud access token |
| `PROJECT_ID` | Google Cloud Project ID |
| `BUCKET_NAME` | GCS bucket name (e.g. `water-quality-dataflow-bucket-a26081e`) |
| `MONGODB_URI` | MongoDB Atlas connection string |
| `MONGO_DB` | MongoDB database name |
| `MONGO_COLLECTION` | MongoDB collection name |

---

## 📊 Data Schema

### BigQuery Table: `sensor_observations`

| Column | Type | Mode | Description |
|--------|------|------|-------------|
| `station_id` | STRING | REQUIRED | Station identifier |
| `timestamp` | TIMESTAMP | REQUIRED | End of 5-min window |
| `PH` | FLOAT | NULLABLE | Average pH value |
| `temperature_c` | FLOAT | NULLABLE | Average temperature (°C) |
| `quality_flag` | STRING | NULLABLE | Data quality assessment flag |

### MongoDB Collection: `sensor_data`
Same schema as BigQuery, optimized for real-time API queries.

---

## 🌐 API Endpoints

**Base URL:** `https://fastapi-iot-broker-899157291449.asia-southeast1.run.app`

### `POST /api/telemetry`

Send sensor data from IoT device.

**Headers:**
```
x-api-key: <your-api-secret>
Content-Type: application/json
```

**Request Body:**
```json
{
  "station_id": "STATION_01",
  "temperature_c": 28.5,
  "PH": 7.4
}
```

**Response:**
```json
{
  "status": "success",
  "message_id": "1234567890"
}
```
