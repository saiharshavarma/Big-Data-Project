# FunnelPulse: Real-Time E-Commerce Funnel Analytics Platform

[![PySpark](https://img.shields.io/badge/PySpark-3.4+-orange.svg)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![GCP](https://img.shields.io/badge/GCP-Dataproc-4285F4.svg)](https://cloud.google.com/dataproc)

FunnelPulse is an end-to-end big data pipeline for monitoring e-commerce conversion funnels in both **batch** and **real-time streaming** modes. Built using Apache Spark, it implements a **Lakehouse architecture** (Bronze → Silver → Gold) with **statistical anomaly detection** to surface conversion drops and spikes.

**Current Status**: ✅ Fully functional on both local development and Google Cloud Platform (GCP Dataproc)

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Project Structure](#3-project-structure)
4. [Dataset](#4-dataset)
5. [Quick Start](#5-quick-start)
   - [Local Development](#51-local-development)
   - [GCP Deployment](#52-gcp-deployment)
6. [Pipeline Components](#6-pipeline-components)
7. [Running the Pipeline](#7-running-the-pipeline)
8. [Configuration](#8-configuration)
9. [Data Schema](#9-data-schema)
10. [Anomaly Detection](#10-anomaly-detection)
11. [GCP Cost & Resource Management](#11-gcp-cost--resource-management)
12. [Troubleshooting](#12-troubleshooting)
13. [Future Enhancements](#13-future-enhancements)
14. [Kafka Integration (For Production)](#14-kafka-integration-for-production)

---

## 1. Overview

### Problem Statement

E-commerce platforms generate millions of user events daily (views, cart additions, purchases). Understanding the **conversion funnel** and detecting anomalies in real-time is critical for:

- Identifying broken checkout flows
- Detecting fraud or bot activity
- Measuring marketing campaign effectiveness
- Monitoring brand and category performance

### Solution

FunnelPulse provides:

| Feature | Description |
|---------|-------------|
| **Batch Analytics** | Historical funnel metrics by brand, category, and price band |
| **Streaming Analytics** | Real-time hourly funnel metrics with watermarking |
| **Anomaly Detection** | Z-score based detection of conversion drops and spikes |
| **Multi-dimensional Views** | Analyze funnels across brands, categories, time, and price segments |
| **Cloud Ready** | One-click deployment to GCP Dataproc |

### Key Metrics Computed

- **Views** → **Carts** → **Purchases** funnel
- **Conversion rates** at each stage
- **Revenue** by brand, category, time period
- **Anomaly scores** (z-scores) for unusual behavior

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FunnelPulse Architecture                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────────────┐  │
│   │ Raw CSV  │────▶│  BRONZE  │────▶│  SILVER  │────▶│      GOLD        │  │
│   │ (Kaggle) │     │ (Parquet)│     │ (Cleaned)│     │   (Aggregated)   │  │
│   └──────────┘     └──────────┘     └──────────┘     └──────────────────┘  │
│        │                                                       │            │
│        │                BATCH PIPELINE                         │            │
│   ═════╪═══════════════════════════════════════════════════════╪══════════  │
│        │              STREAMING PIPELINE                       │            │
│        │                                                       ▼            │
│   ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────────────┐  │
│   │  Stream  │────▶│ Streaming│────▶│ Streaming│────▶│     ANOMALY      │  │
│   │  Input   │     │  Silver  │     │   Gold   │     │    DETECTION     │  │
│   └──────────┘     └──────────┘     └──────────┘     └──────────────────┘  │
│                                                                             │
│   Storage: Local (Parquet) │ GCS (gs://funnelpulse-data-479512)            │
│   Compute: Local Spark │ GCP Dataproc                                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Lakehouse Layers

| Layer | Purpose | Storage | Partitioning |
|-------|---------|---------|--------------|
| **Bronze** | Raw ingestion, minimal transformation | Parquet | `event_date` |
| **Silver** | Cleaned, deduplicated, normalized | Parquet | `event_date` |
| **Gold** | Business-level aggregations | Parquet | `window_date` or `date` |

---

## 3. Project Structure

```
Big-Data-Project/
├── README.md                    # This documentation
├── config.py                    # Centralized configuration (local/GCP)
├── requirements.txt             # Python dependencies
├── setup.sh                     # Local environment setup
├── gcp_setup.sh                 # GCP one-click deployment ⭐
├── .gitignore                   # Git ignore rules
│
├── notebooks/                   # Jupyter notebooks (local development)
│   ├── 01_batch_bronze_silver_gold.ipynb
│   ├── 02_additional_gold_tables.ipynb
│   ├── 03_build_stream_input_from_bronze.ipynb
│   ├── 04_streaming_funnel_hourly_brand.ipynb
│   ├── 05_anomaly_detection_hourly_brand.ipynb
│   └── 06_visualizations.ipynb
│
├── gcp_jobs/                    # PySpark scripts for GCP Dataproc ⭐
│   ├── 01_batch_bronze_silver_gold.py
│   ├── 02_additional_gold_tables.py
│   ├── 03_build_stream_input.py
│   ├── 04_streaming_funnel.py
│   ├── 05_anomaly_detection.py
│   ├── 06_summary_report.py
│   └── test_gcp_pipeline.py
│
├── kafka/                       # Kafka integration scripts (for production)
│   ├── kafka_replay_producer.py
│   └── streaming_from_kafka.py
│
├── data_raw/                    # Raw CSV files (gitignored)
│   ├── 2019-Oct.csv
│   └── 2019-Nov.csv
│
├── tables/                      # Generated Parquet tables (gitignored)
│   ├── bronze_events/
│   ├── silver_events/
│   ├── gold_funnel_hourly_brand/
│   ├── gold_funnel_daily_brand/
│   ├── gold_funnel_daily_category/
│   ├── gold_funnel_hourly_price_band/
│   ├── gold_stream_funnel_hourly_brand/
│   └── gold_anomalies_hourly_brand/
│
├── stream_input/                # Streaming simulation files (gitignored)
└── checkpoints/                 # Spark streaming checkpoints (gitignored)
```

---

## 4. Dataset

**Source**: [Kaggle - eCommerce Events History in Cosmetics Shop](https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop)

| Attribute | Value |
|-----------|-------|
| **Total Events** | ~8.7 million |
| **Time Period** | October - November 2019 |
| **Event Types** | view, cart, purchase, remove_from_cart |
| **Dimensions** | brand, category, price, user_id, session |
| **File Size** | ~1.5 GB (CSV) |

### Sample Record

```json
{
  "event_time": "2019-10-01 00:00:00",
  "event_type": "view",
  "product_id": 5300797,
  "category_id": 2053013563173241677,
  "category_code": "electronics.smartphone",
  "brand": "samsung",
  "price": 274.85,
  "user_id": 541312140,
  "user_session": "72d76fde-8bb3-4e00-8c23-a032dfed738c"
}
```

---

## 5. Quick Start

### 5.1 Local Development

#### Prerequisites

- **Python 3.8+**
- **Java 17** (required for PySpark 3.4+)
- **~10GB disk space** for data and tables

#### Step-by-Step Setup

```bash
# 1. Clone the repository
git clone <repository-url>
cd Big-Data-Project

# 2. Run setup script (creates venv, installs dependencies)
./setup.sh

# 3. Activate virtual environment
source venv/bin/activate

# 4. Download dataset from Kaggle
# Option A: Using Kaggle CLI (requires ~/.kaggle/kaggle.json)
kaggle datasets download -d mkechinov/ecommerce-events-history-in-cosmetics-shop
unzip ecommerce-events-history-in-cosmetics-shop.zip -d data_raw/

# Option B: Manual download
# Visit: https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop
# Download and extract CSV files to data_raw/

# 5. Verify configuration
python config.py

# 6. Start Jupyter Lab
jupyter lab

# 7. Run notebooks in order (01 → 06)
```

#### Java 17 Installation

```bash
# macOS (Homebrew)
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

# Ubuntu/Debian
sudo apt install openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Verify
java -version  # Should show version 17.x
```

### 5.2 GCP Deployment

#### Prerequisites

- **GCP account** with billing enabled (~$50/month for light usage)
- **gcloud CLI** installed and authenticated
- **GCS bucket** created (or use existing `gs://funnelpulse-data-479512`)

#### One-Click Deployment ⭐

```bash
# Setup cluster + upload scripts (takes ~3 minutes)
./gcp_setup.sh

# Setup + run ALL pipeline jobs
./gcp_setup.sh --run-all

# Check status
./gcp_setup.sh --status

# Delete cluster (preserves data in GCS)
./gcp_setup.sh --delete
```

#### Manual GCP Setup

```bash
# 1. Authenticate
gcloud auth login

# 2. Set project
gcloud config set project funnelpulse-479512

# 3. Create bucket (if not exists)
gsutil mb -l us-central1 gs://funnelpulse-data-479512

# 4. Upload local tables to GCS (if running batch locally first)
gsutil -m cp -r tables/ gs://funnelpulse-data-479512/

# 5. Create Dataproc cluster
gcloud dataproc clusters create funnelpulse-cluster \
    --region=us-central1 \
    --zone=us-central1-a \
    --master-machine-type=n1-standard-2 \
    --master-boot-disk-size=50GB \
    --num-workers=2 \
    --worker-machine-type=n1-standard-2 \
    --worker-boot-disk-size=50GB \
    --image-version=2.1-debian11 \
    --optional-components=JUPYTER \
    --enable-component-gateway

# 6. Upload job scripts
gsutil cp gcp_jobs/*.py gs://funnelpulse-data-479512/jobs/

# 7. Submit a job
gcloud dataproc jobs submit pyspark \
    gs://funnelpulse-data-479512/jobs/06_summary_report.py \
    --cluster=funnelpulse-cluster --region=us-central1
```

---

## 6. Pipeline Components

### Notebook/Job Descriptions

| # | Notebook / GCP Job | Purpose | Input | Output |
|---|-------------------|---------|-------|--------|
| 01 | `batch_bronze_silver_gold` | Core lakehouse pipeline | Raw CSV | bronze, silver, gold_hourly_brand |
| 02 | `additional_gold_tables` | Extended analytics views | silver | gold_daily_brand, gold_daily_category, gold_hourly_price |
| 03 | `build_stream_input` | Prepare streaming simulation | bronze | stream_input/ (50 files) |
| 04 | `streaming_funnel` | Real-time aggregation | stream_input | gold_stream_funnel_hourly_brand |
| 05 | `anomaly_detection` | Statistical anomaly flagging | gold_hourly_brand | gold_anomalies_hourly_brand |
| 06 | `visualizations` / `summary_report` | Charts / stats | All gold tables | Visual output |

### Data Flow & Row Counts

```
Raw CSV (8.7M rows)
        │
        ▼
Bronze (8,738,120 rows)
        │
        ▼
Silver (8,260,755 rows) ──────┬──────────────────────┐
        │                     │                      │
        ▼                     ▼                      ▼
Gold Hourly Brand      Gold Daily Brand      Gold Daily Category
(183,145 rows)         (13,107 rows)         (374 rows)
        │                     │                      │
        │                     └──────────────────────┘
        ▼
Gold Hourly Price Band (5,856 rows)
        │
        ▼
Anomalies (442 rows)
```

---

## 7. Running the Pipeline

### 7.1 Local (Notebooks)

Run notebooks **in sequence** (dependencies exist):

```
01_batch_bronze_silver_gold.ipynb      ← Must run first
        ↓
02_additional_gold_tables.ipynb        ← Requires silver
        ↓
03_build_stream_input_from_bronze.ipynb ← Requires bronze
        ↓
04_streaming_funnel_hourly_brand.ipynb  ← Requires stream_input
        ↓
05_anomaly_detection_hourly_brand.ipynb ← Requires gold_hourly_brand
        ↓
06_visualizations.ipynb                 ← Requires all gold tables
```

### 7.2 GCP (Job Scripts)

Submit jobs using `gcloud dataproc jobs submit pyspark`:

```bash
# All jobs in sequence
gcloud dataproc jobs submit pyspark gs://funnelpulse-data-479512/jobs/01_batch_bronze_silver_gold.py \
    --cluster=funnelpulse-cluster --region=us-central1

gcloud dataproc jobs submit pyspark gs://funnelpulse-data-479512/jobs/02_additional_gold_tables.py \
    --cluster=funnelpulse-cluster --region=us-central1

gcloud dataproc jobs submit pyspark gs://funnelpulse-data-479512/jobs/03_build_stream_input.py \
    --cluster=funnelpulse-cluster --region=us-central1

gcloud dataproc jobs submit pyspark gs://funnelpulse-data-479512/jobs/04_streaming_funnel.py \
    --cluster=funnelpulse-cluster --region=us-central1

gcloud dataproc jobs submit pyspark gs://funnelpulse-data-479512/jobs/05_anomaly_detection.py \
    --cluster=funnelpulse-cluster --region=us-central1

gcloud dataproc jobs submit pyspark gs://funnelpulse-data-479512/jobs/06_summary_report.py \
    --cluster=funnelpulse-cluster --region=us-central1
```

Or use the one-click script: `./gcp_setup.sh --run-all`

---

## 8. Configuration

### config.py

The `config.py` file provides centralized configuration for all environments:

```python
# Switch between environments
ENVIRONMENT = "local"  # Options: "local", "jupyterhub", "gcp"

# GCP Settings (used when ENVIRONMENT = "gcp")
GCS_BUCKET = "funnelpulse-data-479512"
GCS_PROJECT = "funnelpulse-479512"
GCS_REGION = "us-central1"

# Spark Settings
SPARK_CONFIG = {
    "app_name": "FunnelPulse",
    "master": "local[*]",      # Use all cores locally
    "driver_memory": "4g",
    "sql_shuffle_partitions": 200,
}

# Anomaly Detection Thresholds
ANOMALY_CONFIG = {
    "min_views_threshold": 10,  # Minimum views for baseline
    "z_score_threshold": 2.0,   # Z-score cutoff for anomalies
}
```

### Environment-Specific Paths

| Environment | Storage Path |
|-------------|--------------|
| local | `./tables/bronze_events` |
| gcp | `gs://funnelpulse-data-479512/tables/bronze_events` |

---

## 9. Data Schema

### Bronze/Silver Events

| Column | Type | Description |
|--------|------|-------------|
| event_time | timestamp | Event timestamp |
| event_type | string | view, cart, purchase, remove_from_cart |
| product_id | int | Product identifier |
| category_id | long | Category identifier |
| category_code | string | Category hierarchy |
| brand | string | Product brand |
| price | double | Product price |
| user_id | int | User identifier |
| user_session | string | Session UUID |
| event_date | date | Partition column |

### Silver Additional Columns

| Column | Type | Description |
|--------|------|-------------|
| brand_norm | string | Lowercase normalized brand |
| category_code_norm | string | Normalized category |
| dq_missing_* | boolean | Data quality flags |

### Gold Funnel Metrics

| Column | Type | Description |
|--------|------|-------------|
| window_start | timestamp | Hour window start |
| window_end | timestamp | Hour window end |
| brand | string | Brand name |
| views | long | View event count |
| carts | long | Cart event count |
| purchases | long | Purchase event count |
| revenue | double | Sum of purchase prices |
| view_to_cart_rate | double | carts / views |
| cart_to_purchase_rate | double | purchases / carts |
| conversion_rate | double | purchases / views |

---

## 10. Anomaly Detection

### Algorithm

FunnelPulse uses **Z-score based anomaly detection**:

1. **Compute Baselines**:
   - Per-brand: mean and stddev of conversion rate across all hours
   - Per-brand-hour: mean and stddev at each hour of day (0-23)

2. **Calculate Z-scores**:
   ```
   z_brand = (current_conversion - brand_mean) / brand_std
   z_brand_hour = (current_conversion - brand_hour_mean) / brand_hour_std
   ```

3. **Flag Anomalies**:
   - **Drop**: z_score ≤ -2.0 (conversion significantly lower than normal)
   - **Spike**: z_score ≥ +2.0 (conversion significantly higher than normal)
   - Requires minimum 50 views to flag

### Sample Output

```
+-------------------+--------+-----+----------+-----------------+--------+
|window_start       |brand   |views|conversion|conv_mean_brand  |z_brand |
+-------------------+--------+-----+----------+-----------------+--------+
|2019-10-15 12:00:00|runail  |156  |0.0128    |0.200            |-2.34   | ← DROP
|2019-10-20 18:00:00|grattol |89   |0.505     |0.119            |+3.12   | ← SPIKE
+-------------------+--------+-----+----------+-----------------+--------+
```

---

## 11. GCP Cost & Resource Management

### Cost Estimate

| Resource | Configuration | Hourly Cost | Monthly (8hr/day) |
|----------|---------------|-------------|-------------------|
| Dataproc Master | n1-standard-2 | ~$0.10 | ~$17 |
| Dataproc Workers (2) | n1-standard-2 | ~$0.20 | ~$34 |
| GCS Storage | ~1GB | ~$0.02 | ~$0.02 |
| **Total** | | **~$0.32/hr** | **~$51/month** |

### Cost-Saving Tips

```bash
# Always delete cluster when not in use!
./gcp_setup.sh --delete

# Check for orphaned resources
gcloud compute instances list --project=funnelpulse-479512
gcloud compute disks list --project=funnelpulse-479512
gcloud dataproc clusters list --region=us-central1
```

---

## 12. Troubleshooting

### Common Issues

| Issue | Symptom | Solution |
|-------|---------|----------|
| **Java Version** | `UnsupportedClassVersionError: class file version 61.0` | Install Java 17 and set JAVA_HOME |
| **Path with Spaces** | `No such file or directory` | Use Python `os` module instead of bash |
| **GCS Permission** | `AccessDeniedException: 403` | Grant Storage Admin role to service account |
| **Port Conflict** | `Service 'SparkUI' could not bind on port 4040` | Normal - Spark auto-finds next port |
| **Out of Memory** | `java.lang.OutOfMemoryError` | Increase `driver_memory` in config.py |

### Debugging Commands

```bash
# Check GCS bucket contents
gsutil ls -la gs://funnelpulse-data-479512/tables/

# View Dataproc job logs
gcloud dataproc jobs list --region=us-central1 --state-filter=ACTIVE
gcloud dataproc jobs describe <JOB_ID> --region=us-central1

# Check cluster status
gcloud dataproc clusters describe funnelpulse-cluster --region=us-central1
```

---

## 13. Future Enhancements

### Recommended Next Steps

| Priority | Enhancement | Description |
|----------|-------------|-------------|
| **HIGH** | Kafka Integration | Replace file-based streaming with Kafka (see Section 14) |
| **HIGH** | Alerting System | Email/Slack notifications for anomalies |
| **MEDIUM** | Dashboard | Streamlit/Dash interactive visualization |
| **MEDIUM** | ML Anomaly Detection | Isolation Forest, LSTM for better detection |
| **LOW** | CI/CD | GitHub Actions, Terraform for infrastructure |
| **LOW** | Data Quality | Great Expectations integration |

### Production Architecture Vision

```
┌─────────────────────────────────────────────────────────────┐
│                   Production Architecture                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  [Web/Mobile Apps] ──▶ [Kafka] ──▶ [Spark Streaming]       │
│                           │              │                  │
│                           ▼              ▼                  │
│                      [Raw Topic]   [Gold Tables]           │
│                           │              │                  │
│                           ▼              ▼                  │
│                    [Data Lake]    [Dashboard/Alerts]       │
│                    (Bronze/Silver)                          │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 14. Kafka Integration (For Production)

The `kafka/` directory contains reference implementations for moving from file-based streaming to Kafka:

### kafka_replay_producer.py

Replays bronze events into a Kafka topic (replaces `03_build_stream_input`):

```python
# Key configuration
KAFKA_BOOTSTRAP_SERVERS = "your-kafka-cluster:9092"
KAFKA_TOPIC = "funnelpulse_events"
BRONZE_PATH = "gs://your-bucket/tables/bronze_events"
```

### streaming_from_kafka.py

Consumes from Kafka and computes streaming gold (replaces `04_streaming_funnel`):

```python
# Key configuration
KAFKA_BOOTSTRAP_SERVERS = "your-kafka-cluster:9092"
KAFKA_TOPIC = "funnelpulse_events"
GOLD_STREAM_PATH = "gs://your-bucket/tables/gold_stream_funnel_hourly_brand"
```

### Migration Path

| Current (File-based) | Production (Kafka) |
|---------------------|-------------------|
| `03_build_stream_input.py` | `kafka_replay_producer.py` |
| `04_streaming_funnel.py` | `streaming_from_kafka.py` |

The cleaning logic, aggregations, and anomaly detection remain unchanged - only the transport layer changes.

---

## Summary

FunnelPulse provides a complete, production-ready foundation for e-commerce funnel analytics:

| Component | Status |
|-----------|--------|
| Batch Pipeline (Bronze → Silver → Gold) | ✅ Complete |
| Streaming Pipeline | ✅ Complete (file-based) |
| Anomaly Detection | ✅ Complete |
| Local Development | ✅ Working |
| GCP Dataproc Deployment | ✅ Working |
| One-Click Setup Script | ✅ Available |
| Kafka Integration | 📋 Reference implementation provided |

**Next Developer Focus Areas**:
1. Replace file-based streaming with Kafka
2. Add alerting/notification system
3. Build interactive dashboard
4. Enhance anomaly detection with ML models

---

## License

MIT License - See [LICENSE](LICENSE) for details.

---

## Acknowledgments

- Dataset: [REES46 Marketing Platform](https://rees46.com/) via Kaggle
- Apache Spark and PySpark community
- Google Cloud Platform

---

*Built with Apache Spark on Google Cloud Platform*
