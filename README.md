# 🌆 UrbanStream

> Real-time urban event detection & gig worker pollution exposure tracking  
> Apache Kafka · Apache Spark · MinIO · Redis · Streamlit

---

## Architecture Overview

```
NYC Open Data CSVs
        │
        ▼
  ┌─────────────┐     ┌──────────────────────┐     ┌──────────────┐
  │  Producers  │────▶│  Redpanda (Kafka)     │────▶│  Spark       │
  │  (4 topics) │     │  traffic_stream       │     │  Structured  │
  │             │     │  pollution_stream     │     │  Streaming   │
  │  Traffic    │     │  weather_stream       │     └──────┬───────┘
  │  Pollution  │     │  worker_stream        │            │
  │  Weather    │     └──────────────────────┘            │ Parquet
  │  Workers    │                                         ▼
  └─────────────┘                              ┌──────────────────┐
                                               │  MinIO (S3)      │
                                               │  /zone_scores    │
                                               │  /events         │
                                               │  /worker_exp.    │
                                               └─────────┬────────┘
                                                         │
                                               ┌─────────▼────────┐
                                               │  ML Jobs         │
                                               │  clustering.py   │──▶ Redis
                                               │  recommender.py  │◀──
                                               └─────────┬────────┘
                                                         │
                                               ┌─────────▼────────┐
                                               │  Streamlit       │
                                               │  Dashboard       │
                                               └──────────────────┘
```

---

## Prerequisites

- Docker Desktop (or Docker + Docker Compose) installed
- Python 3.10+
- ~8 GB free RAM (for Spark + Redpanda + MinIO)

---

## Step 1 – Download the Datasets

Create the `data/` directory and download three CSVs:

### 1. NYC Traffic Speed (NYC OpenData)
```bash
# Visit: https://data.cityofnewyork.us/Transportation/Traffic-Speed/4h9m-uh3q
# Click Export → CSV
# Save as: data/nyc_traffic.csv
```
Required columns: `SPEED`, `LINK_ID`, `DATA_AS_OF`, `BOROUGH`, `LINK_POINTS`

### 2. OpenAQ Air Quality – NYC Stations
```bash
# Visit: https://openaq.org/data/
# Filter: Country=US, City=New York, Parameter=pm25 AND no2
# Export CSV
# Save as: data/openaq_nyc.csv
```
Required columns: `location`, `parameter`, `value`, `unit`, `date_utc`, `latitude`, `longitude`

### 3. Open-Meteo Historical Weather
```bash
# Option A – Web UI:
# Visit: https://open-meteo.com/
# Location: New York (lat=40.7128, lon=-74.0060)
# Variables: temperature_2m, relativehumidity_2m, windspeed_10m
# Start date: 2023-01-01, End date: 2024-01-01
# Export CSV → Save as: data/weather_nyc.csv

# Option B – API (Python):
python3 - << 'EOF'
import requests, csv, json

url = (
    "https://archive-api.open-meteo.com/v1/archive"
    "?latitude=40.7128&longitude=-74.0060"
    "&start_date=2023-01-01&end_date=2024-01-01"
    "&hourly=temperature_2m,relativehumidity_2m,windspeed_10m"
    "&timezone=America%2FNew_York"
)
r = requests.get(url)
data = r.json()
hourly = data["hourly"]

with open("data/weather_nyc.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["time","temperature_2m","relativehumidity_2m","windspeed_10m"])
    w.writeheader()
    for i, t in enumerate(hourly["time"]):
        w.writerow({
            "time": t,
            "temperature_2m": hourly["temperature_2m"][i],
            "relativehumidity_2m": hourly["relativehumidity_2m"][i],
            "windspeed_10m": hourly["windspeed_10m"][i],
        })
print("Downloaded", len(hourly["time"]), "weather records")
EOF
```

---

## Step 2 – Install Python Dependencies (for local producers / dashboard)

```bash
pip install -r requirements.txt
```

Create `requirements.txt`:
```
confluent-kafka>=2.3.0
redis>=5.0.0
pandas>=2.0.0
streamlit>=1.30.0
pydeck>=0.8.0
plotly>=5.18.0
requests>=2.31.0
boto3>=1.34.0
pyspark>=3.5.0
```

---

## Step 3 – Start All Docker Services

```bash
cd urbanstream/
docker compose up -d

# Wait ~60 seconds for all services to be healthy
docker compose ps

# Check logs
docker compose logs redpanda      # Kafka broker
docker compose logs spark-master  # Spark UI
docker compose logs minio         # Object storage
docker compose logs redis         # Cache
```

**Service URLs after startup:**
| Service | URL |
|---------|-----|
| Redpanda Console | http://localhost:8080 |
| Spark Master UI | http://localhost:8888 |
| MinIO Console | http://localhost:9001 (user: minioadmin / minioadmin) |
| Redis | localhost:6379 |

---

## Step 4 – Run the Kafka Producers

Open 4 terminal windows and run each producer:

```bash
# Terminal 1 – Traffic (100 rows/sec default)
cd urbanstream/
KAFKA_BROKER=localhost:9092 REPLAY_SPEED=100 python3 producers/traffic_producer.py

# Terminal 2 – Pollution (50 rows/sec default)
cd urbanstream/
KAFKA_BROKER=localhost:9092 REPLAY_SPEED=50 python3 producers/pollution_producer.py

# Terminal 3 – Weather (10 rows/sec default)
cd urbanstream/
KAFKA_BROKER=localhost:9092 REPLAY_SPEED=10 python3 producers/weather_producer.py

# Terminal 4 – Workers (simulated, no CSV needed)
cd urbanstream/
KAFKA_BROKER=localhost:9092 python3 producers/worker_producer.py
```

**Speed up for throughput testing:**
```bash
REPLAY_SPEED=500 python3 producers/traffic_producer.py   # 500 ev/sec
REPLAY_SPEED=1000 python3 producers/traffic_producer.py  # 1000 ev/sec
```

---

## Step 5 – Submit the Spark Streaming Job

```bash
docker exec urbanstream-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages \
    org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
    org.apache.hadoop:hadoop-aws:3.3.4,\
    com.amazonaws:aws-java-sdk-bundle:1.12.367 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.executor.memory=1g \
  /opt/spark/jobs/stream_processor.py
```

Watch the Spark UI at http://localhost:8888 to see active streaming queries.

---

## Step 6 – Run ML Jobs (Optional, enhances dashboard)

```bash
# Clustering (run hourly; can be added to cron)
docker exec urbanstream-spark-master /opt/spark/bin/spark-submit \
  --packages \
    org.apache.hadoop:hadoop-aws:3.3.4,\
    com.amazonaws:aws-java-sdk-bundle:1.12.367 \
  /opt/spark/jobs/../ml/clustering.py

# Recommender (run every 30 seconds)
REDIS_HOST=localhost python3 ml/recommender.py
```

---

## Step 7 – Launch the Dashboard

```bash
cd urbanstream/
REDIS_HOST=localhost streamlit run dashboard/dashboard.py --server.port 8501
```

Open http://localhost:8501 in your browser.

The dashboard works even before data arrives – it shows synthetic placeholder data with graceful empty states.

---

## Data Volume Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Records in 2h | 5M+ | At 100 ev/sec sustained |
| Parquet compression | 3×+ | vs raw JSON |
| Lag at 100 ev/s | <100ms | |
| Lag at 500 ev/s | <400ms | |
| Lag at 1000 ev/s | <1000ms | |

---

## File Structure

```
urbanstream/
├── docker-compose.yml      ← All services (Redpanda, Spark, MinIO, Redis)
├── data/
│   ├── nyc_traffic.csv     ← Download per Step 1
│   ├── openaq_nyc.csv      ← Download per Step 1
│   └── weather_nyc.csv     ← Download per Step 1
├── producers/
│   ├── traffic_producer.py  ← 100 rows/sec → traffic_stream
│   ├── pollution_producer.py← 50 rows/sec  → pollution_stream
│   ├── weather_producer.py  ← 10 rows/sec  → weather_stream
│   └── worker_producer.py   ← 50 simulated workers → worker_stream
├── spark/
│   └── stream_processor.py  ← Main Spark job (5 streaming queries)
├── ml/
│   ├── clustering.py        ← Hourly KMeans clustering
│   └── recommender.py       ← 30s recommendation loop
└── dashboard/
    └── dashboard.py         ← Streamlit 5-tab dashboard
```

---

## Troubleshooting

**Redpanda not starting?**
```bash
docker compose logs redpanda
# Increase Docker memory to ≥6GB in Docker Desktop settings
```

**Spark job fails with S3A errors?**
```bash
# Check MinIO is healthy
curl http://localhost:9000/minio/health/live
# Ensure buckets exist
docker logs urbanstream-minio-init
```

**Dashboard shows no data?**
```bash
# Check Redis
redis-cli -h localhost ping
# The dashboard always shows synthetic fallback data
# Real data flows in once Spark + producers are running
```

**Kafka topic not found?**
```bash
docker exec urbanstream-redpanda rpk topic list
# Topics are auto-created by producers, or pre-created by redpanda-init
```

---

## Stopping Everything

```bash
docker compose down          # Stop services (keep data volumes)
docker compose down -v       # Stop + delete all data
```
