#!/usr/bin/env python3
"""
UrbanStream – Zone Clustering (KMeans, PySpark MLlib)
Runs as a periodic batch job (hourly).
Reads zone_scores Parquet from MinIO, clusters zones into 4 types,
writes cluster labels back to MinIO and Redis.

Run:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
               org.apache.hadoop:hadoop-aws:3.3.4,\
               com.amazonaws:aws-java-sdk-bundle:1.12.367 \
    jobs/../ml/clustering.py
"""

import logging
import os

import redis
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CLUSTERING] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
DATALAKE_BASE    = os.getenv("DATALAKE_BASE",     "s3a://datalake")

REDIS_HOST       = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT       = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB         = int(os.getenv("REDIS_DB",   "0"))
REDIS_TTL        = int(os.getenv("REDIS_TTL",  "3600"))   # 1 hour

K = 4   # number of clusters

# Cluster label assignment (by inspection of NYC traffic patterns)
# We sort cluster centers by avg_aqi descending and avg_speed ascending
# to assign meaningful labels.
CLUSTER_LABELS = {
    0: "Permanently Hazardous",
    1: "Peak Hour Hazardous",
    2: "Weather Sensitive",
    3: "Safe Corridor",
}

CLUSTER_WEIGHTS = {
    "Permanently Hazardous": 0.1,
    "Peak Hour Hazardous":   0.5,
    "Weather Sensitive":     0.7,
    "Safe Corridor":         1.0,
}


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("UrbanStream-Clustering")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def load_zone_scores(spark: SparkSession):
    """Load zone_scores Parquet, compute per-zone feature aggregates."""
    try:
        df = spark.read.parquet(f"{DATALAKE_BASE}/zone_scores")
    except Exception as exc:
        log.warning("Could not read zone_scores Parquet: %s", exc)
        log.info("Generating synthetic zone data for bootstrapping...")
        df = _generate_synthetic_zone_data(spark)
        return df

    if df.rdd.isEmpty():
        log.info("Zone scores Parquet is empty – using synthetic data")
        return _generate_synthetic_zone_data(spark)

    return df


def _generate_synthetic_zone_data(spark: SparkSession):
    """
    Fallback: generate synthetic zone feature data when Parquet is empty.
    This allows the clustering pipeline to be tested end-to-end.
    """
    import random
    zones = (
        [f"MN-{i:02d}" for i in range(1, 7)] +
        [f"BK-{i:02d}" for i in range(1, 7)] +
        [f"QN-{i:02d}" for i in range(1, 7)] +
        [f"BX-{i:02d}" for i in range(1, 7)] +
        [f"SI-{i:02d}" for i in range(1, 7)]
    )
    rows = []
    for zone in zones:
        prefix = zone[:2]
        # Assign realistic synthetic characteristics
        if prefix == "MN":
            aqi, speed, pct_cmpd = random.uniform(80, 160), random.uniform(15, 35), random.uniform(0.1, 0.4)
        elif prefix == "BK":
            aqi, speed, pct_cmpd = random.uniform(50, 120), random.uniform(25, 50), random.uniform(0.05, 0.2)
        elif prefix == "QN":
            aqi, speed, pct_cmpd = random.uniform(40, 90),  random.uniform(35, 65), random.uniform(0.0, 0.1)
        elif prefix == "BX":
            aqi, speed, pct_cmpd = random.uniform(70, 150), random.uniform(20, 45), random.uniform(0.05, 0.3)
        else:  # SI
            aqi, speed, pct_cmpd = random.uniform(20, 60),  random.uniform(40, 80), random.uniform(0.0, 0.05)
        rows.append((
            zone,
            float(aqi),
            float(speed),
            float(pct_cmpd),
            float(random.uniform(5, 30)),  # aqi_variance
        ))

    schema = ["zone_id", "avg_aqi", "avg_speed", "pct_compound_events", "aqi_variance"]
    return spark.createDataFrame(rows, schema)


def build_features(df):
    """Aggregate per-zone and assemble ML feature vector."""
    zone_features = df.groupBy("zone_id").agg(
        F.avg("avg_aqi").alias("avg_aqi"),
        F.avg("avg_speed").alias("avg_speed"),
        F.avg(
            F.when(F.col("event_type") == "COMPOUND_EVENT", 1.0).otherwise(0.0)
        ).alias("pct_compound_events") if "event_type" in df.columns else F.lit(0.05).alias("pct_compound_events"),
        F.stddev("avg_aqi").alias("aqi_variance"),
    ).fillna(0.0)

    assembler = VectorAssembler(
        inputCols=["avg_aqi", "avg_speed", "pct_compound_events", "aqi_variance"],
        outputCol="features_raw",
        handleInvalid="skip",
    )
    assembled = assembler.transform(zone_features)

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True,
    )
    scaler_model = scaler.fit(assembled)
    scaled = scaler_model.transform(assembled)

    return scaled, zone_features


def assign_cluster_labels(predictions, zone_features):
    """
    Sort cluster centers by avg_aqi (desc) to assign semantic labels.
    Cluster with highest avg_aqi → Permanently Hazardous, etc.
    """
    # Compute per-cluster mean aqi to rank them
    cluster_stats = (
        predictions
        .join(zone_features.select("zone_id", "avg_aqi", "avg_speed"), "zone_id")
        .groupBy("prediction")
        .agg(
            F.avg("avg_aqi").alias("c_avg_aqi"),
            F.avg("avg_speed").alias("c_avg_speed"),
            F.count("*").alias("zone_count"),
        )
        .orderBy(F.col("c_avg_aqi").desc())
        .collect()
    )

    # Map cluster index → semantic label by rank
    label_order = [
        "Permanently Hazardous",
        "Peak Hour Hazardous",
        "Weather Sensitive",
        "Safe Corridor",
    ]
    cluster_to_label = {}
    for rank, row in enumerate(cluster_stats):
        label = label_order[min(rank, len(label_order) - 1)]
        cluster_to_label[row["prediction"]] = label
        log.info(
            "Cluster %d → '%s' | avg_aqi=%.1f avg_speed=%.1f zones=%d",
            row["prediction"], label, row["c_avg_aqi"], row["c_avg_speed"], row["zone_count"],
        )

    return cluster_to_label


def push_to_redis(zone_labels: dict[str, str]) -> None:
    """Push cluster:{zone_id} → label to Redis with TTL."""
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, socket_timeout=5)
        pipe = r.pipeline()
        for zone_id, label in zone_labels.items():
            key = f"cluster:{zone_id}"
            pipe.set(key, label, ex=REDIS_TTL)
        # Also write a summary hash for fast dashboard reads
        for zone_id, label in zone_labels.items():
            pipe.hset("zone_cluster_map", zone_id, label)
        pipe.expire("zone_cluster_map", REDIS_TTL)
        pipe.execute()
        log.info("Pushed %d cluster labels to Redis (TTL=%ds)", len(zone_labels), REDIS_TTL)
    except Exception as exc:
        log.error("Redis push failed: %s", exc)


def main():
    log.info("Starting UrbanStream Clustering job...")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Load data
    df = load_zone_scores(spark)

    # Build features (handle both raw Parquet and synthetic schemas)
    if "avg_aqi" in df.columns and "avg_speed" in df.columns:
        # Synthetic or pre-aggregated data
        zone_features = df
        if "aqi_variance" not in df.columns:
            zone_features = zone_features.withColumn("aqi_variance", F.lit(10.0))
        if "pct_compound_events" not in df.columns:
            zone_features = zone_features.withColumn("pct_compound_events", F.lit(0.05))

        assembler = VectorAssembler(
            inputCols=["avg_aqi", "avg_speed", "pct_compound_events", "aqi_variance"],
            outputCol="features_raw",
            handleInvalid="skip",
        )
        assembled = assembler.transform(zone_features)

        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withMean=True,
            withStd=True,
        )
        scaler_model = scaler.fit(assembled)
        scaled = scaler_model.transform(assembled)
    else:
        scaled, zone_features = build_features(df)

    log.info("Training KMeans (k=%d)...", K)
    kmeans = KMeans(featuresCol="features", predictionCol="prediction", k=K, seed=42)
    model  = kmeans.fit(scaled)
    predictions = model.transform(scaled)

    zone_count = predictions.count()
    log.info("Clustered %d zones", zone_count)

    # Assign semantic labels
    cluster_to_label = assign_cluster_labels(predictions, zone_features if "avg_aqi" in df.columns else scaled)

    # Build final output
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    label_udf = udf(lambda c: cluster_to_label.get(c, "Safe Corridor"), StringType())

    result = (
        predictions
        .withColumn("cluster_label", label_udf(F.col("prediction")))
        .withColumn("cluster_weight",
            F.when(F.col("cluster_label") == "Permanently Hazardous", 0.1)
             .when(F.col("cluster_label") == "Peak Hour Hazardous",   0.5)
             .when(F.col("cluster_label") == "Weather Sensitive",     0.7)
             .otherwise(1.0)
        )
        .select("zone_id", "cluster_label", "cluster_weight", "prediction")
    )

    # Write to MinIO
    log.info("Writing cluster labels to MinIO...")
    try:
        result.write.mode("overwrite").parquet(f"{DATALAKE_BASE}/clusters")
        log.info("Cluster labels written to %s/clusters", DATALAKE_BASE)
    except Exception as exc:
        log.error("Failed to write clusters to MinIO: %s", exc)

    # Push to Redis
    zone_labels = {row["zone_id"]: row["cluster_label"] for row in result.collect()}
    push_to_redis(zone_labels)

    log.info("Clustering job complete.")
    spark.stop()


if __name__ == "__main__":
    main()
