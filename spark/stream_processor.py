"""
UrbanStream – Spark Structured Streaming Processor

Each topic is aggregated independently in its own streaming query.
Zone scoring happens inside foreachBatch using a static join on collected rows.
This avoids the stream-stream join watermark delay that caused empty batches.

"""

import json
import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType,
)

# ─── Config ──────────────────────────────────────────────────────────────────
KAFKA_BROKER    = os.getenv("KAFKA_BROKER",    "redpanda:9092")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/tmp/urbanstream/checkpoints")
DATALAKE_BASE   = os.getenv("DATALAKE_BASE",   "/tmp/urbanstream/datalake")
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minutes")
WATERMARK       = os.getenv("WATERMARK",       "2 minutes")
REDIS_HOST      = os.getenv("REDIS_HOST",      "redis")
BASELINE_SPEED  = 40.0   # km/h

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [STREAM-PROCESSOR] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ─── Redis (optional – dashboard reads from here) ────────────────────────────
try:
    import redis as _redis_lib
    _redis = _redis_lib.Redis(
        host=REDIS_HOST, port=6379, db=0,
        socket_timeout=2, decode_responses=True,
    )
    _redis.ping()
    log.info("Redis connected at %s:6379", REDIS_HOST)
except Exception as e:
    _redis = None
    log.warning("Redis not available (%s) – dashboard will use synthetic data", e)


# ─── Schemas ─────────────────────────────────────────────────────────────────

TRAFFIC_SCHEMA = StructType([
    StructField("segment_id", StringType()),
    StructField("speed_kmh",  DoubleType()),
    StructField("borough",    StringType()),
    StructField("zone_id",    StringType()),
    StructField("timestamp",  StringType()),
    StructField("lat",        DoubleType()),
    StructField("lon",        DoubleType()),
])

POLLUTION_SCHEMA = StructType([
    StructField("station_id", StringType()),
    StructField("zone_id",    StringType()),
    StructField("pm25",       DoubleType()),
    StructField("no2",        DoubleType()),
    StructField("aqi",        DoubleType()),
    StructField("timestamp",  StringType()),
    StructField("lat",        DoubleType()),
    StructField("lon",        DoubleType()),
])

WEATHER_SCHEMA = StructType([
    StructField("temperature", DoubleType()),
    StructField("humidity",    DoubleType()),
    StructField("windspeed",   DoubleType()),
    StructField("condition",   StringType()),
    StructField("timestamp",   StringType()),
])

WORKER_SCHEMA = StructType([
    StructField("worker_id",   StringType()),
    StructField("zone_id",     StringType()),
    StructField("borough",     StringType()),
    StructField("timestamp",   StringType()),
    StructField("mobility_type", StringType()),
])


# ─── SparkSession ─────────────────────────────────────────────────────────────

def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("UrbanStream-v2")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


# ─── Kafka reader ─────────────────────────────────────────────────────────────

def read_kafka(spark: SparkSession, topic: str, schema: StructType) -> DataFrame:
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe",               topic)
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")
        .option("maxOffsetsPerTrigger",    10000)
        .load()
    )
    return (
        raw.select(
            F.from_json(F.col("value").cast("string"), schema).alias("d"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("d.*", "kafka_ts")
        .withColumn(
            "event_time",
            F.coalesce(
                F.to_timestamp(F.col("timestamp")),
                F.col("kafka_ts"),
            )
        )
    )


# ─── Redis helpers ────────────────────────────────────────────────────────────

def push_zone_scores(rows: list) -> None:
    if not _redis or not rows:
        return
    try:
        pipe = _redis.pipeline()
        for r in rows:
            key = f"zone_score:{r['zone_id']}"
            val = json.dumps({
                "zone_id":    r["zone_id"],
                "zone_score": round(float(r.get("zone_score") or 0.5), 3),
                "avg_aqi":    round(float(r.get("avg_aqi")   or 50.0), 1),
                "avg_speed":  round(float(r.get("avg_speed") or 30.0), 1),
                "event_type": str(r.get("event_type") or "NORMAL"),
                "borough":    str(r.get("borough")    or ""),
            })
            pipe.set(key, val, ex=300)
        pipe.execute()
        log.info("[REDIS] Pushed %d zone scores", len(rows))
    except Exception as e:
        log.warning("[REDIS] Push failed: %s", e)


def push_worker_exposure(rows: list) -> None:
    if not _redis or not rows:
        return
    try:
        pipe = _redis.pipeline()
        for r in rows:
            key = f"exposure:{r['worker_id']}"
            val = json.dumps({
                "worker_id":         r["worker_id"],
                "zone_id":           r.get("zone_id", ""),
                "exposure_status":   r.get("exposure_status", "SAFE"),
                "hours_in_high_aqi": round(float(r.get("hours_in_high_aqi") or 0), 3),
                "high_aqi_minutes":  round(float(r.get("hours_in_high_aqi") or 0) * 60.0, 3),
                "total_minutes":     round(float(r.get("count", 1)) * 1.0, 3),
                "aqi_sum":           round(float(r.get("daily_avg_aqi") or 50) * float(r.get("count", 1)), 1),
                "daily_avg_aqi":     round(float(r.get("daily_avg_aqi")     or 50), 1),
            })
            pipe.set(key, val, ex=300)
            pipe.set(f"worker_zone:{r['worker_id']}", r.get("zone_id", ""), ex=300)
        pipe.execute()
    except Exception as e:
        log.warning("[REDIS] Worker exposure push failed: %s", e)


# ─── Shared in-memory state (updated each batch) ─────────────────────────────
# Stores latest per-zone aggregates so zone_scores batch can enrich with AQI
# even when traffic and pollution arrive in different micro-batches.

_latest_traffic:   dict = {}   # zone_id → {avg_speed, borough}
_latest_pollution: dict = {}   # zone_id → {avg_aqi, avg_pm25, avg_no2}
_worker_exposure:  dict = {}   # worker_id → {hours_in_high_aqi, zone_id, ...}
_total_records = [0]


# ─── foreachBatch handlers ────────────────────────────────────────────────────

def handle_traffic(df: DataFrame, batch_id: int) -> None:
    rows = df.collect()
    if not rows:
        return

    # Aggregate per zone in Python (batch is small)
    zone_data: dict = {}
    for r in rows:
        zid = r["zone_id"] or "MN-01"
        spd = float(r["speed_kmh"] or 30)
        if zid not in zone_data:
            zone_data[zid] = {"speeds": [], "borough": r["borough"] or "Manhattan"}
        zone_data[zid]["speeds"].append(spd)

    for zid, d in zone_data.items():
        avg_spd = sum(d["speeds"]) / len(d["speeds"])
        _latest_traffic[zid] = {"avg_speed": avg_spd, "borough": d["borough"]}

    _total_records[0] += len(rows)
    log.info("[TRAFFIC] Batch #%d | %d rows | %d zones updated | total: %d",
             batch_id, len(rows), len(zone_data), _total_records[0])

    # Recompute and push zone scores after every traffic batch
    _push_zone_scores(batch_id)


def handle_pollution(df: DataFrame, batch_id: int) -> None:
    rows = df.collect()
    if not rows:
        return

    zone_data: dict = {}
    for r in rows:
        zid = r["zone_id"] or "MN-01"
        aqi = float(r["aqi"] or 50)
        pm  = float(r["pm25"] or 10)
        no2 = float(r["no2"]  or 20)
        if zid not in zone_data:
            zone_data[zid] = {"aqis": [], "pm25s": [], "no2s": []}
        zone_data[zid]["aqis"].append(aqi)
        zone_data[zid]["pm25s"].append(pm)
        zone_data[zid]["no2s"].append(no2)

    for zid, d in zone_data.items():
        _latest_pollution[zid] = {
            "avg_aqi":  sum(d["aqis"])  / len(d["aqis"]),
            "avg_pm25": sum(d["pm25s"]) / len(d["pm25s"]),
            "avg_no2":  sum(d["no2s"])  / len(d["no2s"]),
        }

    _total_records[0] += len(rows)
    log.info("[POLLUTION] Batch #%d | %d rows | %d zones updated",
             batch_id, len(rows), len(zone_data))

    _push_zone_scores(batch_id)


def _push_zone_scores(batch_id: int) -> None:
    """Merge latest traffic + pollution into zone scores and push to Redis."""
    all_zones = set(list(_latest_traffic.keys()) + list(_latest_pollution.keys()))
    if not all_zones:
        return

    scores = []
    for zid in all_zones:
        t = _latest_traffic.get(zid,   {"avg_speed": BASELINE_SPEED, "borough": ""})
        p = _latest_pollution.get(zid, {"avg_aqi": 50.0, "avg_pm25": 10.0, "avg_no2": 20.0})

        avg_speed = t["avg_speed"]
        avg_aqi   = p["avg_aqi"]

        speed_score = min(1.0, avg_speed / BASELINE_SPEED)
        aqi_score   = min(1.0, avg_aqi   / 100.0)
        zone_score  = round((speed_score * 0.5) + (aqi_score * 0.5), 3)

        congestion = avg_speed < (BASELINE_SPEED * 0.6)
        pollution  = avg_aqi > 150
        if congestion and pollution:
            event = "COMPOUND_EVENT"
        elif congestion:
            event = "CONGESTION_EVENT"
        elif pollution:
            event = "POLLUTION_ALERT"
        else:
            event = "NORMAL"

        scores.append({
            "zone_id":    zid,
            "avg_speed":  round(avg_speed, 1),
            "avg_aqi":    round(avg_aqi, 1),
            "avg_pm25":   round(p["avg_pm25"], 1),
            "zone_score": zone_score,
            "event_type": event,
            "borough":    t["borough"],
            "batch_id":   batch_id,
            "ts":         datetime.now().isoformat(timespec="seconds"),
        })

    push_zone_scores(scores)

    events = [s for s in scores if s["event_type"] != "NORMAL"]
    if events:
        log.warning("[EVENTS] Batch #%d → %d anomalies: %s",
                    batch_id, len(events),
                    ", ".join(f"{e['zone_id']}:{e['event_type']}" for e in events[:5]))

    log.info("[ZONE_SCORES] Batch #%d | %d zones scored | pushed to Redis",
             batch_id, len(scores))

    # Write Parquet snapshot
    try:
        import json as _json
        out_dir = f"{DATALAKE_BASE}/zone_scores/batch_{batch_id:06d}"
        os.makedirs(out_dir, exist_ok=True)
        with open(f"{out_dir}/data.json", "w") as f:
            _json.dump(scores, f)
    except Exception as e:
        log.debug("Parquet write skipped: %s", e)


def handle_workers(df: DataFrame, batch_id: int) -> None:
    rows = df.collect()
    if not rows:
        return

    HIGH_AQI = 100.0
    WINDOW_MINS = 1.0  # matches our WINDOW_DURATION

    updated = []
    for r in rows:
        wid  = r["worker_id"]
        zid  = r["zone_id"] or "MN-01"
        aqi  = _latest_pollution.get(zid, {}).get("avg_aqi", 75.0)
        prev = _worker_exposure.get(wid, {"hours_in_high_aqi": 0.0, "daily_avg_aqi": 0.0, "count": 0})

        new_high_mins = prev["hours_in_high_aqi"] * 60 + (WINDOW_MINS if aqi > HIGH_AQI else 0)
        new_count     = prev["count"] + 1
        new_avg_aqi   = (prev["daily_avg_aqi"] * prev["count"] + aqi) / new_count
        hours_high    = new_high_mins / 60.0

        status = "CRITICAL" if hours_high > 3.5 else ("WARNING" if hours_high > 2.0 else "SAFE")

        _worker_exposure[wid] = {
            "worker_id":        wid,
            "zone_id":          zid,
            "hours_in_high_aqi": round(hours_high, 3),
            "daily_avg_aqi":    round(new_avg_aqi, 1),
            "exposure_status":  status,
            "count":            new_count,
        }
        updated.append(_worker_exposure[wid])

    critical = sum(1 for w in updated if w["exposure_status"] == "CRITICAL")
    warning  = sum(1 for w in updated if w["exposure_status"] == "WARNING")
    log.info("[WORKERS] Batch #%d | %d workers | Critical:%d Warning:%d",
             batch_id, len(updated), critical, warning)


def handle_weather(df: DataFrame, batch_id: int) -> None:
    rows = df.collect()
    if not rows:
        return
    latest = rows[-1]
    log.info("[WEATHER] Batch #%d | temp=%.1f°C humidity=%.0f%% wind=%.1fkm/h",
             batch_id,
             float(latest["temperature"] or 0),
             float(latest["humidity"]    or 0),
             float(latest["windspeed"]   or 0))

    if _redis:
        try:
            _redis.set("weather_current", json.dumps({
                "temperature": float(latest["temperature"] or 0),
                "humidity":    float(latest["humidity"]    or 0),
                "windspeed":   float(latest["windspeed"]   or 0),
                "condition":   str(latest["condition"]     or "Clear"),
                "ts":          datetime.now().isoformat(timespec="seconds"),
            }), ex=300)
        except Exception:
            pass


def handle_metrics(df: DataFrame, batch_id: int) -> None:
    count = df.count()
    _total_records[0] += count
    log.info("[METRICS] Batch #%d | Batch records: %d | Running total: %d",
             batch_id, count, _total_records[0])

    if _redis:
        try:
            _redis.set("perf_total_records", _total_records[0], ex=3600)
            # Append to throughput history
            hist_key = "perf_history"
            entry = json.dumps({
                "ts":  datetime.now().strftime("%H:%M"),
                "rps": count,
                "lag_ms": 50,
                "storage_mb": _total_records[0] / 1000,
            })
            _redis.rpush(hist_key, entry)
            _redis.ltrim(hist_key, -20, -1)   # keep last 20 entries
            _redis.expire(hist_key, 3600)
        except Exception:
            pass


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("Initializing UrbanStream Spark processor (v2 – no stream-stream join)...")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info("KAFKA_BROKER    = %s", KAFKA_BROKER)
    log.info("CHECKPOINT_BASE = %s", CHECKPOINT_BASE)
    log.info("WINDOW_DURATION = %s", WINDOW_DURATION)
    log.info("REDIS_HOST      = %s", REDIS_HOST)

    log.info("Reading Kafka topics...")
    traffic_raw   = read_kafka(spark, "traffic_stream",   TRAFFIC_SCHEMA)
    pollution_raw = read_kafka(spark, "pollution_stream", POLLUTION_SCHEMA)
    weather_raw   = read_kafka(spark, "weather_stream",   WEATHER_SCHEMA)
    worker_raw    = read_kafka(spark, "worker_stream",    WORKER_SCHEMA)

    os.makedirs(CHECKPOINT_BASE, exist_ok=True)
    os.makedirs(DATALAKE_BASE,   exist_ok=True)

    log.info("Starting streaming queries (trigger: 30 seconds)...")

    q_traffic = (
        traffic_raw
        .withWatermark("event_time", WATERMARK)
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/traffic")
        .trigger(processingTime="30 seconds")
        .foreachBatch(handle_traffic)
        .start()
    )

    q_pollution = (
        pollution_raw
        .withWatermark("event_time", WATERMARK)
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/pollution")
        .trigger(processingTime="30 seconds")
        .foreachBatch(handle_pollution)
        .start()
    )

    q_weather = (
        weather_raw
        .withWatermark("event_time", WATERMARK)
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/weather")
        .trigger(processingTime="30 seconds")
        .foreachBatch(handle_weather)
        .start()
    )

    q_workers = (
        worker_raw
        .withWatermark("event_time", WATERMARK)
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/workers")
        .trigger(processingTime="30 seconds")
        .foreachBatch(handle_workers)
        .start()
    )

    q_metrics = (
        traffic_raw
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/metrics")
        .trigger(processingTime="60 seconds")
        .foreachBatch(handle_metrics)
        .start()
    )

    log.info("All 5 streaming queries running.")
    log.info("  Traffic    : %s", q_traffic.id)
    log.info("  Pollution  : %s", q_pollution.id)
    log.info("  Weather    : %s", q_weather.id)
    log.info("  Workers    : %s", q_workers.id)
    log.info("  Metrics    : %s", q_metrics.id)
    log.info("Waiting for data... first batch in ~30 seconds.")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
