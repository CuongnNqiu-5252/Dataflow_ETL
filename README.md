# 🌊 IoT Water Quality Monitoring Pipeline

A real-time data engineering pipeline that collects IoT sensor data, processes it using Apache Beam on Google Cloud Dataflow, and stores results in both BigQuery and MongoDB Atlas.

---

## 🏗️ Infrastructure Architecture

### Data Pipeline Flow

```mermaid
flowchart TD
    A["📡 ESP32 Sensor / simulator.html"] -->|"POST /api/telemetry + x-api-key"| B

    subgraph CloudRun["☁️ Cloud Run"]
        B["FastAPI Broker\nmain.py\n- API Key Auth\n- CORS enabled"]
    end

    B -->|"publish message"| C

    subgraph PubSub["📨 Cloud Pub/Sub"]
        C["Topic: iot-telemetry-topic"]
        D["Subscription: iot-telemetry-sub\n3-day retention"]
        C --> D
    end

    D -->|"Streaming Pull 24/7"| E

    subgraph Dataflow["⚙️ Cloud Dataflow - Streaming Job"]
        E["Step 1: ReadFromPubSub"]
        F["Step 2: Parse JSON"]
        G["Step 3: 5-min Fixed Window"]
        H["Step 4: Validate & Flag anomalies"]
        I["Step 5: Format output"]
        E --> F --> G --> H --> I
    end

    I --> J
    I --> K

    subgraph Storage["🗄️ Storage Layer"]
        J["BigQuery\nwater_quality_analytics.sensor_observations"]
        K["MongoDB Atlas\nwater_quality_db.sensor_data"]
    end

    L["GCS Bucket\nDataflow Template JSON"] -.->|"Flex Template spec"| Dataflow
```

---

### CI/CD Deployment Flow

```mermaid
flowchart LR
    DEV["👨‍💻 git push"] --> REPO["GitHub\nmain branch"]
    REPO -->|"triggers"| ACTIONS["GitHub Actions"]

    SECRETS["🔐 GitHub Secrets\nGCP_CREDENTIALS\nPULUMI_ACCESS_TOKEN\nMONGODB_URI\netc."] -.->|"injected"| ACTIONS

    ACTIONS -->|"1. docker build"| AR["Artifact Registry\niot-dataflow:{sha}"]
    ACTIONS -->|"2. gcloud build template"| GCS["GCS Bucket\niot-pipeline-spec-{sha}.json"]
    ACTIONS -->|"3. pulumi up"| DF["Dataflow Job\niot-pipeline-{sha}"]

    AR -.->|"image"| GCS
    GCS -.->|"template"| DF
```

---

### Security Model

```mermaid
flowchart LR
    A["ESP32 / Browser"] -->|"x-api-key header"| B["FastAPI Auth Check"]
    C["key.json\nService Account"] -->|"ADC"| D["Google Cloud APIs"]
    E["GitHub Secret\nGCP_CREDENTIALS"] -->|"google-github-actions/auth"| D
```

---

## 📁 Project Structure

```
DT_demo/
├── main.py                         # FastAPI broker (Cloud Run)
├── iot_water_quality_pipeline.py   # Apache Beam pipeline (Dataflow)
├── index.ts                        # Pulumi IaC — GCP infrastructure
├── metadata.json                   # Dataflow Flex Template parameters
├── Dockerfile                      # Cloud Run Docker image
├── Dockerfile.dataflow             # Dataflow worker Docker image
├── requirements.txt                # Python dependencies
├── simulator.html                  # Web UI to simulate IoT sensor
├── Pulumi.yaml                     # Pulumi project config
├── .env                            # Local env vars (gitignored)
└── .github/
    └── workflows/
        └── deploy-dataflow.yml     # GitHub Actions CI/CD pipeline
```

---

## ⚙️ GCP Infrastructure (Managed by Pulumi)

| Component | GCP Service | Purpose |
|-----------|-------------|---------|
| GCS Bucket | Cloud Storage | Stores Dataflow Flex Template JSON |
| Pub/Sub Topic | Cloud Pub/Sub | Message queue entry point |
| Pub/Sub Subscription | Cloud Pub/Sub | Dataflow pull endpoint (3-day retention) |
| BigQuery Dataset | BigQuery | Analytics data warehouse |
| BigQuery Table | BigQuery | `sensor_observations` — day-partitioned, station-clustered |
| Dataflow Streaming Job | Cloud Dataflow | Processes Pub/Sub → BigQuery + MongoDB |

---

## 🚀 Getting Started

### Prerequisites

- Google Cloud SDK (`gcloud`)
- Pulumi CLI
- Node.js 18+
- Python 3.10+
- Docker

### Run Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run FastAPI
uvicorn main:app --reload

# Deploy infrastructure
pulumi up
```

### Required GitHub Secrets

| Secret | Value |
|--------|-------|
| `GCP_CREDENTIALS` | Content of Service Account `key.json` |
| `PULUMI_ACCESS_TOKEN` | Pulumi Cloud token |
| `PROJECT_ID` | GCP Project ID |
| `BUCKET_NAME` | e.g. `water-quality-dataflow-bucket-a26081e` |
| `MONGODB_URI` | MongoDB Atlas connection string |
| `MONGO_DB` | MongoDB database name |
| `MONGO_COLLECTION` | MongoDB collection name |

---

## 📊 Data Schema

### BigQuery: `sensor_observations`

| Column | Type | Mode | Description |
|--------|------|------|-------------|
| `station_id` | STRING | REQUIRED | Sensor station ID |
| `timestamp` | TIMESTAMP | REQUIRED | End of 5-min window |
| `PH` | FLOAT | NULLABLE | Average pH |
| `temperature_c` | FLOAT | NULLABLE | Average temperature (°C) |
| `quality_flag` | STRING | NULLABLE | Data quality flag |

---

## 🌐 API Reference

**Base URL:** `https://fastapi-iot-broker-899157291449.asia-southeast1.run.app`

### `POST /api/telemetry`

**Headers:**

| Header | Value |
|--------|-------|
| `x-api-key` | Your API secret |
| `Content-Type` | `application/json` |

**Request:**
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
