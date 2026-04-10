#!/usr/bin/env python3
"""
UrbanStream – Worker Producer
Simulates 50 gig workers moving between 30 NYC zones.
Streams to Kafka topic: worker_stream
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ─── Config ──────────────────────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",   "localhost:9092")
TOPIC          = os.getenv("WORKER_TOPIC",   "worker_stream")
NUM_WORKERS    = int(os.getenv("NUM_WORKERS", "50"))
MOVE_INTERVAL  = float(os.getenv("MOVE_INTERVAL", "30"))   # seconds between zone moves
NUM_PARTITIONS = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WORKER-PRODUCER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ─── Zone definitions (30 zones across 5 boroughs) ───────────────────────────
ALL_ZONES = (
    [f"MN-{i:02d}" for i in range(1, 7)] +
    [f"BK-{i:02d}" for i in range(1, 7)] +
    [f"QN-{i:02d}" for i in range(1, 7)] +
    [f"BX-{i:02d}" for i in range(1, 7)] +
    [f"SI-{i:02d}" for i in range(1, 7)]
)

ZONE_BOROUGH = {}
ZONE_LAT = {}
ZONE_LON = {}

BOROUGH_BOUNDS = {
    "MN": (40.70, 40.88, -74.02, -73.91),
    "BK": (40.57, 40.74, -74.04, -73.84),
    "QN": (40.54, 40.80, -73.97, -73.70),
    "BX": (40.80, 40.92, -73.94, -73.74),
    "SI": (40.48, 40.65, -74.26, -74.03),
}

BOROUGH_NAMES = {
    "MN": "Manhattan",
    "BK": "Brooklyn",
    "QN": "Queens",
    "BX": "Bronx",
    "SI": "Staten Island",
}

for zone in ALL_ZONES:
    prefix = zone[:2]
    lat_lo, lat_hi, lon_lo, lon_hi = BOROUGH_BOUNDS[prefix]
    idx = int(zone[3:]) - 1   # 0-5
    # Divide each borough into a 2×3 sub-grid
    lat_step = (lat_hi - lat_lo) / 3
    lon_step = (lon_hi - lon_lo) / 2
    row_i    = idx // 2
    col_i    = idx  % 2
    ZONE_LAT[zone] = round(lat_lo + (row_i + 0.5) * lat_step, 6)
    ZONE_LON[zone] = round(lon_lo + (col_i + 0.5) * lon_step, 6)
    ZONE_BOROUGH[zone] = BOROUGH_NAMES[prefix]

# ─── Adjacency (workers tend to stay in their borough) ───────────────────────
ZONE_NEIGHBORS: dict[str, list[str]] = {}
for zone in ALL_ZONES:
    prefix = zone[:2]
    same_borough = [z for z in ALL_ZONES if z.startswith(prefix) and z != zone]
    # Occasionally cross into adjacent boroughs
    adj_prefix = {"MN": "BK", "BK": "MN", "QN": "MN", "BX": "MN", "SI": "BK"}.get(prefix, prefix)
    adj_zones  = [z for z in ALL_ZONES if z.startswith(adj_prefix)][:2]
    ZONE_NEIGHBORS[zone] = same_borough + adj_zones


# ─── Helpers ─────────────────────────────────────────────────────────────────

def ensure_topic(broker: str, topic: str, partitions: int = 3) -> None:
    admin = AdminClient({"bootstrap.servers": broker})
    meta  = admin.list_topics(timeout=10)
    if topic not in meta.topics:
        admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
        log.info("Created topic %s", topic)


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed: %s", err)


# ─── Worker state ─────────────────────────────────────────────────────────────

class GigWorker:
    def __init__(self, worker_id: str):
        self.worker_id     = worker_id
        self.current_zone  = random.choice(ALL_ZONES)
        self.last_move_ts  = time.time()
        # Workers have different mobility profiles
        self.mobility_type = random.choice(["high", "medium", "low"])   # move frequency
        self.move_interval = {
            "high":   MOVE_INTERVAL * 0.5,
            "medium": MOVE_INTERVAL,
            "low":    MOVE_INTERVAL * 2.0,
        }[self.mobility_type]

    def maybe_move(self) -> bool:
        """Returns True if the worker moved zones."""
        if time.time() - self.last_move_ts >= self.move_interval:
            neighbors     = ZONE_NEIGHBORS[self.current_zone]
            self.current_zone = random.choice(neighbors)
            self.last_move_ts = time.time()
            return True
        return False

    def to_message(self) -> dict:
        zone = self.current_zone
        return {
            "worker_id":   self.worker_id,
            "zone_id":     zone,
            "borough":     ZONE_BOROUGH[zone],
            "lat":         ZONE_LAT[zone] + random.uniform(-0.001, 0.001),
            "lon":         ZONE_LON[zone] + random.uniform(-0.001, 0.001),
            "timestamp":   datetime.now(timezone.utc).isoformat(),
            "mobility_type": self.mobility_type,
        }


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("Connecting to Kafka broker: %s", KAFKA_BROKER)
    ensure_topic(KAFKA_BROKER, TOPIC, NUM_PARTITIONS)

    producer = Producer({
        "bootstrap.servers":  KAFKA_BROKER,
        "queue.buffering.max.messages": 10000,
        "batch.num.messages":           100,
        "linger.ms":                    10,
        "compression.type":             "snappy",
    })

    # Initialize workers W-01 … W-50
    workers = [GigWorker(f"W-{i:02d}") for i in range(1, NUM_WORKERS + 1)]
    log.info("Initialized %d workers across %d zones", len(workers), len(ALL_ZONES))

    total_sent   = 0
    start_time   = time.time()
    window_sent  = 0
    window_start = time.time()

    # Heartbeat interval: send position of every worker
    HEARTBEAT_INTERVAL = 5.0   # seconds

    log.info("Starting worker position stream → topic '%s'", TOPIC)

    while True:
        batch_start = time.time()

        for worker in workers:
            worker.maybe_move()
            msg     = worker.to_message()
            payload = json.dumps(msg).encode()

            producer.produce(
                TOPIC,
                key=worker.worker_id.encode(),
                value=payload,
                callback=delivery_report,
            )
            producer.poll(0)

            total_sent  += 1
            window_sent += 1

        elapsed_window = time.time() - window_start
        if elapsed_window >= 5.0:
            rps = window_sent / elapsed_window
            zone_counts: dict[str, int] = {}
            for w in workers:
                zone_counts[w.current_zone] = zone_counts.get(w.current_zone, 0) + 1
            top_zones = sorted(zone_counts.items(), key=lambda x: -x[1])[:3]
            log.info(
                "Throughput: %.1f msgs/sec | Total: %d | Top zones: %s | Uptime: %.0fs",
                rps, total_sent,
                ", ".join(f"{z}({c})" for z, c in top_zones),
                time.time() - start_time,
            )
            window_sent  = 0
            window_start = time.time()

        # Sleep for the remainder of the heartbeat interval
        elapsed_batch = time.time() - batch_start
        sleep_time    = max(0.0, HEARTBEAT_INTERVAL - elapsed_batch)
        time.sleep(sleep_time)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Worker producer stopped by user.")
