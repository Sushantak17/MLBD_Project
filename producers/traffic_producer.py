#!/usr/bin/env python3
"""
UrbanStream – Traffic Producer
Reads nyc_traffic.csv row by row and streams to Kafka topic: traffic_stream
"""

import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ─── Config ──────────────────────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",   "localhost:9092")
TOPIC          = os.getenv("TRAFFIC_TOPIC",  "traffic_stream")
REPLAY_SPEED   = int(os.getenv("REPLAY_SPEED", "100"))   # rows / second
DATA_FILE      = Path(os.getenv("DATA_FILE",   "data/nyc_traffic.csv"))
NUM_PARTITIONS = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TRAFFIC-PRODUCER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# Borough → rough centroid (lat, lon) for zone enrichment
BOROUGH_CENTROIDS: dict[str, tuple[float, float]] = {
    "Manhattan":    (40.7831, -73.9712),
    "Brooklyn":     (40.6782, -73.9442),
    "Queens":       (40.7282, -73.7949),
    "Bronx":        (40.8448, -73.8648),
    "Staten Island":(40.5795, -74.1502),
    "MANHATTAN":    (40.7831, -73.9712),
    "BROOKLYN":     (40.6782, -73.9442),
    "QUEENS":       (40.7282, -73.7949),
    "BRONX":        (40.8448, -73.8648),
}

# Zone mapping: each borough gets 6 zones (total 30)
BOROUGH_ZONES: dict[str, list[str]] = {
    "MANHATTAN":     [f"MN-{i:02d}" for i in range(1, 7)],
    "BROOKLYN":      [f"BK-{i:02d}" for i in range(1, 7)],
    "QUEENS":        [f"QN-{i:02d}" for i in range(1, 7)],
    "BRONX":         [f"BX-{i:02d}" for i in range(1, 7)],
    "STATEN ISLAND": [f"SI-{i:02d}" for i in range(1, 7)],
}

# ─── Helpers ─────────────────────────────────────────────────────────────────

def ensure_topic(broker: str, topic: str, partitions: int = 3) -> None:
    admin = AdminClient({"bootstrap.servers": broker})
    meta  = admin.list_topics(timeout=10)
    if topic not in meta.topics:
        admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
        log.info("Created topic %s", topic)


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed for key %s: %s", msg.key(), err)


def parse_row(row: dict, idx: int) -> dict | None:
    """Parse one CSV row into a Kafka message dict."""
    try:
        borough_raw = (row.get("BOROUGH") or row.get("borough") or "MANHATTAN").strip().upper()
        borough     = borough_raw if borough_raw in BOROUGH_ZONES else "MANHATTAN"
        zones       = BOROUGH_ZONES[borough]
        zone_id     = zones[idx % len(zones)]

        speed_raw = row.get("SPEED") or row.get("speed") or "30"
        speed     = float(speed_raw)

        lat, lon = BOROUGH_CENTROIDS.get(borough, (40.7128, -74.0060))
        # Small jitter to distinguish segments
        lat += (hash(row.get("LINK_ID", str(idx))) % 1000) / 100000
        lon += (hash(row.get("LINK_ID", str(idx))) % 1000) / 100000

        return {
            "segment_id": row.get("LINK_ID") or row.get("link_id") or f"SEG-{idx:06d}",
            "speed_kmh":  round(speed * 1.60934, 2),   # mph → km/h
            "speed_mph":  round(speed, 2),
            "borough":    borough.title(),
            "zone_id":    zone_id,
            "timestamp":  datetime.now(timezone.utc).isoformat(),
            "lat":        round(lat, 6),
            "lon":        round(lon, 6),
            "source_ts":  row.get("DATA_AS_OF") or row.get("data_as_of") or "",
        }
    except (ValueError, KeyError) as exc:
        log.debug("Skipping malformed row %d: %s", idx, exc)
        return None


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not DATA_FILE.exists():
        log.error(
            "Data file not found: %s\n"
            "Download from: https://data.cityofnewyork.us/Transportation/Traffic-Speed/4h9m-uh3q",
            DATA_FILE,
        )
        sys.exit(1)

    log.info("Connecting to Kafka broker: %s", KAFKA_BROKER)
    ensure_topic(KAFKA_BROKER, TOPIC, NUM_PARTITIONS)

    producer = Producer({
        "bootstrap.servers":  KAFKA_BROKER,
        "queue.buffering.max.messages": 100000,
        "batch.num.messages":           1000,
        "linger.ms":                    5,
        "compression.type":             "snappy",
    })

    interval   = 1.0 / REPLAY_SPEED
    total_sent = 0
    start_time = time.time()
    window_sent = 0
    window_start = time.time()

    log.info("Starting replay at %d rows/sec → topic '%s'", REPLAY_SPEED, TOPIC)

    while True:   # loop the file for continuous streaming
        with open(DATA_FILE, newline="", encoding="utf-8", errors="replace") as fh:
            reader = csv.DictReader(fh)
            for idx, row in enumerate(reader):
                msg = parse_row(row, idx)
                if msg is None:
                    continue

                payload = json.dumps(msg).encode()
                producer.produce(
                    TOPIC,
                    key=msg["segment_id"].encode(),
                    value=payload,
                    callback=delivery_report,
                )
                producer.poll(0)   # non-blocking delivery callbacks

                total_sent  += 1
                window_sent += 1

                # Throughput log every 5 seconds
                elapsed_window = time.time() - window_start
                if elapsed_window >= 5.0:
                    rps = window_sent / elapsed_window
                    log.info(
                        "Throughput: %.1f rows/sec | Total sent: %d | Uptime: %.0fs",
                        rps, total_sent, time.time() - start_time,
                    )
                    window_sent  = 0
                    window_start = time.time()

                time.sleep(interval)

        log.info("Rewinding CSV – looping for continuous stream (total sent: %d)", total_sent)
        producer.flush()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Traffic producer stopped by user.")
