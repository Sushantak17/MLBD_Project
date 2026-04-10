#!/usr/bin/env python3
"""
UrbanStream – Pollution Producer
Reads openaq_nyc.csv and streams to Kafka topic: pollution_stream
"""

import csv
import json
import logging
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ─── Config ──────────────────────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",     "localhost:9092")
TOPIC          = os.getenv("POLLUTION_TOPIC",  "pollution_stream")
REPLAY_SPEED   = int(os.getenv("REPLAY_SPEED", "50"))   # rows / second
DATA_FILE      = Path(os.getenv("DATA_FILE",   "data/openaq_nyc.csv"))
NUM_PARTITIONS = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [POLLUTION-PRODUCER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# Zone grid for NYC (lat/lon bounding boxes → zone_id)
# Simplified: divide NYC into a 5×6 grid mapped to our 30 zones
ZONE_GRID = {
    "MN": (40.70, 40.88, -74.02, -73.91),
    "BK": (40.57, 40.74, -74.04, -73.84),
    "QN": (40.54, 40.80, -73.97, -73.70),
    "BX": (40.80, 40.92, -73.94, -73.74),
    "SI": (40.48, 40.65, -74.26, -74.03),
}

BOROUGH_ZONES = {
    "MN": [f"MN-{i:02d}" for i in range(1, 7)],
    "BK": [f"BK-{i:02d}" for i in range(1, 7)],
    "QN": [f"QN-{i:02d}" for i in range(1, 7)],
    "BX": [f"BX-{i:02d}" for i in range(1, 7)],
    "SI": [f"SI-{i:02d}" for i in range(1, 7)],
}

# ─── AQI calculation (US EPA formula simplified) ─────────────────────────────

AQI_BREAKPOINTS_PM25 = [
    (0.0,   12.0,   0,   50),
    (12.1,  35.4,  51,  100),
    (35.5,  55.4, 101,  150),
    (55.5, 150.4, 151,  200),
    (150.5, 250.4, 201, 300),
    (250.5, 350.4, 301, 400),
    (350.5, 500.4, 401, 500),
]

AQI_BREAKPOINTS_NO2 = [
    (0,    53,   0,  50),
    (54,  100,  51, 100),
    (101, 360, 101, 150),
    (361, 649, 151, 200),
    (650, 1249, 201, 300),
    (1250, 1649, 301, 400),
    (1650, 2049, 401, 500),
]


def calc_aqi(concentration: float, breakpoints: list) -> float:
    for (c_lo, c_hi, i_lo, i_hi) in breakpoints:
        if c_lo <= concentration <= c_hi:
            return ((i_hi - i_lo) / (c_hi - c_lo)) * (concentration - c_lo) + i_lo
    return 500.0


def compute_aqi(pm25: float, no2: float) -> float:
    aqi_pm25 = calc_aqi(pm25, AQI_BREAKPOINTS_PM25)
    aqi_no2  = calc_aqi(no2,  AQI_BREAKPOINTS_NO2)
    return round(max(aqi_pm25, aqi_no2), 1)


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


def lat_lon_to_zone(lat: float, lon: float, station_idx: int) -> str:
    for prefix, (lat_lo, lat_hi, lon_lo, lon_hi) in ZONE_GRID.items():
        if lat_lo <= lat <= lat_hi and lon_lo <= lon <= lon_hi:
            zones = BOROUGH_ZONES[prefix]
            return zones[station_idx % len(zones)]
    return f"MN-{(station_idx % 6) + 1:02d}"


def parse_row(row: dict, buffer: dict, idx: int) -> dict | None:
    """
    OpenAQ CSV has one measurement per row. We buffer pm25 and no2
    per station and emit when we have both.
    """
    try:
        station_id = (row.get("location") or row.get("station_id") or f"NYC-{idx % 30:03d}").strip()
        parameter  = (row.get("parameter") or "").strip().lower()
        value_str  = row.get("value") or row.get("measurement") or "0"
        value      = float(value_str)

        lat_str = row.get("latitude")  or row.get("lat")  or "40.7128"
        lon_str = row.get("longitude") or row.get("lon")  or "-74.0060"
        lat     = float(lat_str)
        lon     = float(lon_str)

        if station_id not in buffer:
            buffer[station_id] = {"pm25": 0.0, "no2": 0.0, "lat": lat, "lon": lon}

        if "pm25" in parameter or "pm2.5" in parameter:
            buffer[station_id]["pm25"] = max(0.0, value)
        elif "no2" in parameter:
            buffer[station_id]["no2"] = max(0.0, value)
        else:
            return None   # skip other parameters

        entry  = buffer[station_id]
        pm25   = entry["pm25"]
        no2    = entry["no2"]
        aqi    = compute_aqi(pm25, no2)
        zone   = lat_lon_to_zone(lat, lon, idx)

        return {
            "station_id": station_id,
            "zone_id":    zone,
            "pm25":       round(pm25, 2),
            "no2":        round(no2,  2),
            "aqi":        aqi,
            "timestamp":  datetime.now(timezone.utc).isoformat(),
            "lat":        round(lat, 6),
            "lon":        round(lon, 6),
            "source_ts":  row.get("date_utc") or row.get("date") or "",
        }
    except (ValueError, KeyError) as exc:
        log.debug("Skipping row %d: %s", idx, exc)
        return None


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not DATA_FILE.exists():
        log.error(
            "Data file not found: %s\n"
            "Download from: https://openaq.org/data/ (filter NYC, pm25, no2)",
            DATA_FILE,
        )
        sys.exit(1)

    log.info("Connecting to Kafka broker: %s", KAFKA_BROKER)
    ensure_topic(KAFKA_BROKER, TOPIC, NUM_PARTITIONS)

    producer = Producer({
        "bootstrap.servers":  KAFKA_BROKER,
        "queue.buffering.max.messages": 50000,
        "batch.num.messages":           500,
        "linger.ms":                    10,
        "compression.type":             "snappy",
    })

    interval     = 1.0 / REPLAY_SPEED
    total_sent   = 0
    start_time   = time.time()
    window_sent  = 0
    window_start = time.time()
    buffer: dict = {}

    log.info("Starting pollution replay at %d rows/sec → topic '%s'", REPLAY_SPEED, TOPIC)

    while True:
        with open(DATA_FILE, newline="", encoding="utf-8", errors="replace") as fh:
            reader = csv.DictReader(fh)
            for idx, row in enumerate(reader):
                msg = parse_row(row, buffer, idx)
                if msg is None:
                    time.sleep(interval)
                    continue

                payload = json.dumps(msg).encode()
                producer.produce(
                    TOPIC,
                    key=msg["station_id"].encode(),
                    value=payload,
                    callback=delivery_report,
                )
                producer.poll(0)

                total_sent  += 1
                window_sent += 1

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

        log.info("Rewinding CSV – looping (total sent: %d)", total_sent)
        producer.flush()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Pollution producer stopped by user.")
