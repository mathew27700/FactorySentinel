"""FactorySentinel sensor event simulator.

Replays the Azure PdM telemetry CSV into Kafka as JSON events, one simulated hour at a time.
SIM_SPEED=3600 means one hour of data per real second.
"""

import json
import os
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

SCHEMA_VERSION = 1
SENSOR_FIELDS = ("volt", "rotate", "pressure", "vibration")
EVENT_FIELDS = (
    "event_id",
    "event_type",
    "machine_id",
    "event_time",
    "ingest_time",
    "schema_version",
    "source",
) + SENSOR_FIELDS


def _iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_event(row: dict) -> dict:
    """Turn one telemetry row (datetime, machineID, sensors) into a schema v1 reading event."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "reading",
        "machine_id": int(row["machineID"]),
        "event_time": _iso(pd.Timestamp(row["datetime"])),
        "ingest_time": _iso(datetime.now(UTC)),
        "schema_version": SCHEMA_VERSION,
        "source": "dataset",
        **{name: float(row[name]) for name in SENSOR_FIELDS},
    }


def main() -> int:
    from kafka import KafkaProducer

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
    topic = os.getenv("KAFKA_TOPIC", "sensor.raw")
    speed = float(os.getenv("SIM_SPEED", "3600"))
    max_hours = int(os.getenv("SIM_MAX_HOURS", "0"))  # 0 = replay everything
    csv_path = Path(os.getenv("TELEMETRY_CSV", "data/raw/PdM_telemetry.csv"))

    df = pd.read_csv(csv_path, parse_dates=["datetime"]).sort_values("datetime")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: str(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    sent = 0
    try:
        for hours, (_, group) in enumerate(df.groupby("datetime"), start=1):
            for row in group.to_dict("records"):
                event = build_event(row)
                producer.send(topic, key=event["machine_id"], value=event)
                sent += 1
            producer.flush()
            if max_hours and hours >= max_hours:
                break
            time.sleep(3600 / speed)
    finally:
        producer.flush()
        producer.close()
    print(f"sent {sent} events to {topic}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
