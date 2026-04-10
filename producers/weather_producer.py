#!/usr/bin/env python3
"""
UrbanStream – Weather Producer
Reads weather_nyc.csv (from Open-Meteo) and streams to: weather_stream
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
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",    "localhost:9092")
TOPIC          = os.getenv("WEATHER_TOPIC",   "weather_stream")
REPLAY_SPEED   = int(os.getenv("REPLAY_SPEED", "10"))   # rows / second
DATA_FILE      = Path(os.getenv("DATA_FILE",   "data/weather_nyc.csv"))
NUM_PARTITIONS = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WEATHER-PRODUCER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

NYC_LAT = 40.7128
NYC_LON = -74.0060


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


def parse_row(row: dict, idx: int) -> dict | None:
    """
    Open-Meteo CSV columns (after download):
      time, temperature_2m (°C), relativehumidity_2m (%), windspeed_10m (km/h)
    We handle minor column name variations.
    """
    try:
        temp_keys  = ["temperature_2m", "temperature_2m (°C)", "temperature", "temp"]
        humid_keys = ["relativehumidity_2m", "relativehumidity_2m (%)", "humidity", "relative_humidity"]
        wind_keys  = ["windspeed_10m", "windspeed_10m (km/h)", "windspeed", "wind_speed"]
        time_keys  = ["time", "date", "datetime", "timestamp"]

        def get_val(keys):
            for k in keys:
                if k in row and row[k].strip():
                    return float(row[k])
            return None

        temp      = get_val(temp_keys)
        humidity  = get_val(humid_keys)
        windspeed = get_val(wind_keys)

        if temp is None:
            return None

        source_ts = ""
        for k in time_keys:
            if k in row:
                source_ts = row[k]
                break

        # Classify weather condition
        condition = "Clear"
        if humidity is not None and humidity > 80:
            condition = "Humid"
        if windspeed is not None and windspeed > 30:
            condition = "Windy"
        if temp is not None and temp < 5:
            condition = "Cold"
        if temp is not None and temp > 30:
            condition = "Hot"

        return {
            "temperature":  round(temp, 1)     if temp      is not None else None,
            "humidity":     round(humidity, 1)  if humidity  is not None else None,
            "windspeed":    round(windspeed, 1) if windspeed is not None else None,
            "condition":    condition,
            "timestamp":    datetime.now(timezone.utc).isoformat(),
            "source_ts":    source_ts,
            "lat":          NYC_LAT,
            "lon":          NYC_LON,
        }
    except (ValueError, KeyError) as exc:
        log.debug("Skipping row %d: %s", idx, exc)
        return None


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not DATA_FILE.exists():
        log.error(
            "Data file not found: %s\n"
            "Download hourly data for NYC (lat=40.7128, lon=-74.0060) from:\n"
            "  https://open-meteo.com/\n"
            "Parameters: temperature_2m, relativehumidity_2m, windspeed_10m",
            DATA_FILE,
        )
        sys.exit(1)

    log.info("Connecting to Kafka broker: %s", KAFKA_BROKER)
    ensure_topic(KAFKA_BROKER, TOPIC, NUM_PARTITIONS)

    producer = Producer({
        "bootstrap.servers":  KAFKA_BROKER,
        "queue.buffering.max.messages": 10000,
        "batch.num.messages":           100,
        "linger.ms":                    20,
        "compression.type":             "snappy",
    })

    interval     = 1.0 / REPLAY_SPEED
    total_sent   = 0
    start_time   = time.time()
    window_sent  = 0
    window_start = time.time()

    log.info("Starting weather replay at %d rows/sec → topic '%s'", REPLAY_SPEED, TOPIC)

    while True:
        with open(DATA_FILE, newline="", encoding="utf-8", errors="replace") as fh:
            # Open-Meteo CSV often has header/metadata rows — skip non-data rows
            lines = fh.readlines()

        # Find the actual header row (contains "time" or "temperature")
        header_idx = 0
        for i, line in enumerate(lines):
            if "time" in line.lower() or "temperature" in line.lower():
                header_idx = i
                break

        import io
        data_text = "".join(lines[header_idx:])
        reader    = csv.DictReader(io.StringIO(data_text))

        for idx, row in enumerate(reader):
            msg = parse_row(row, idx)
            if msg is None:
                continue

            payload = json.dumps(msg).encode()
            producer.produce(
                TOPIC,
                key=b"nyc-weather",
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

        log.info("Rewinding weather CSV (total sent: %d)", total_sent)
        producer.flush()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Weather producer stopped by user.")
