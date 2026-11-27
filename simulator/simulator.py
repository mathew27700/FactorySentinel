"""FactorySentinel sensor event simulator.

Replays the Azure PdM telemetry CSV into Kafka as JSON events, one simulated hour at a time.
SIM_SPEED=3600 means one hour of data per real second (100 machines = 100 events per second).

Faults can be injected while it runs: scripts/inject_fault.py writes a small control file, and
the simulator reads it before every simulated hour.
"""

import json
import os
import random
import signal
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
FAULT_TYPES = ("spike", "drift", "malformed", "late", "duplicate")


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


def active_faults(path: Path, now: datetime | None = None) -> dict[int, str]:
    """Read the control file. Returns {machine_id: fault type} for faults that have not expired."""
    try:
        faults = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    stamp = _iso(now or datetime.now(UTC))
    return {int(f["machine_id"]): f["type"] for f in faults if f["until"] > stamp}


def apply_fault(event: dict, fault: str | None) -> list[bytes]:
    """Return the Kafka messages to send for this event. Usually one; a fault changes that."""
    if fault == "spike":  # one extreme reading: caught by the per-reading alert rules
        event.update(
            source="injected", volt=event["volt"] * 1.5, vibration=event["vibration"] * 1.7
        )
    elif fault == "drift":  # a sustained shift: only the 24-hour average gives it away
        event.update(source="injected", vibration=event["vibration"] * 1.35)
    elif fault == "malformed":  # breaks the schema on purpose: must end up in the dead-letter topic
        return [
            random.choice(
                [
                    b"not json at all",
                    json.dumps({"machine_id": event["machine_id"]}).encode(),
                    json.dumps(event)[:-20].encode(),
                    json.dumps({**event, "volt": "high"}).encode(),
                ]
            )
        ]
    payload = json.dumps(event).encode()
    return [payload, payload] if fault == "duplicate" else [payload]


def hour_messages(
    rows: list[dict], faults: dict[int, str], held: dict[int, list[bytes]]
) -> list[tuple[int, bytes]]:
    """All Kafka messages (key, value) for one simulated hour, with the active faults applied.

    `held` keeps the events of machines under a "late" fault; they are sent when the fault ends.
    """
    out = []
    for machine_id in [m for m in held if faults.get(m) != "late"]:
        out += [(machine_id, payload) for payload in held.pop(machine_id)]
    for row in rows:
        event = build_event(row)
        fault = faults.get(event["machine_id"])
        if fault == "late":
            event["source"] = "injected"
            held.setdefault(event["machine_id"], []).append(json.dumps(event).encode())
            continue
        out += [(event["machine_id"], payload) for payload in apply_fault(event, fault)]
    return out


def main() -> int:
    from kafka import KafkaProducer

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
    topic = os.getenv("KAFKA_TOPIC", "sensor.raw")
    speed = float(os.getenv("SIM_SPEED", "3600"))
    max_hours = int(os.getenv("SIM_MAX_HOURS", "0"))  # 0 = replay everything
    start = os.getenv("SIM_START", "")  # e.g. 2015-07-01; empty = start of the file
    csv_path = Path(os.getenv("TELEMETRY_CSV", "data/raw/PdM_telemetry.csv"))
    fault_file = Path(os.getenv("FAULT_FILE", "runtime/faults.json"))

    df = pd.read_csv(csv_path, parse_dates=["datetime"]).sort_values("datetime")
    if start:
        df = df[df["datetime"] >= pd.Timestamp(start)]

    settings = {"bootstrap_servers": bootstrap, "acks": "all", "retries": 5}
    if os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT") != "PLAINTEXT":  # Event Hubs / Fabric
        settings.update(
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username="$ConnectionString",  # literally this text
            sasl_plain_password=os.environ["KAFKA_SASL_PASSWORD"],
        )
    producer = KafkaProducer(key_serializer=lambda k: str(k).encode(), **settings)

    running = True

    def stop(*_) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    sent = 0
    held: dict[int, list[bytes]] = {}  # events kept back by a "late" fault, per machine
    try:
        for hours, (stamp, group) in enumerate(df.groupby("datetime"), start=1):
            faults = active_faults(fault_file)
            for key, payload in hour_messages(group.to_dict("records"), faults, held):
                producer.send(topic, key=key, value=payload)
                sent += 1
            producer.flush()
            if hours % 24 == 0:
                print(f"dataset time {stamp}, sent {sent} events", flush=True)
            if not running or (max_hours and hours >= max_hours):
                break
            time.sleep(3600 / speed)
    finally:
        producer.flush()
        producer.close()
    print(f"sent {sent} events to {topic}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
