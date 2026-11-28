"""Validation rules from docs/contract.md, section 3. Plain Python, so it is easy to unit-test."""

import json
from datetime import datetime, timedelta, timezone

NUMBER = (int, float)
REQUIRED = {
    "event_id": str,
    "event_type": str,
    "machine_id": int,
    "event_time": str,
    "ingest_time": str,
    "schema_version": int,
    "source": str,
    "volt": NUMBER,
    "rotate": NUMBER,
    "pressure": NUMBER,
    "vibration": NUMBER,
}
RANGES = {"volt": (0, 500), "rotate": (0, 1000), "pressure": (0, 300), "vibration": (0, 150)}
SOURCES = {"dataset", "injected"}


def _parse_time(text: str) -> datetime | None:
    try:
        ts = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)  # noqa: UP017 (container runs Python 3.10, no datetime.UTC)


def reject_reason(raw: str | None) -> str | None:
    """Return None if the event is valid, otherwise a short reason (the first rule that fails)."""
    try:
        event = json.loads(raw)
    except (TypeError, ValueError):
        return "not valid JSON"
    if not isinstance(event, dict):
        return "not valid JSON"

    for field, kind in REQUIRED.items():
        if event.get(field) is None:
            return f"missing required field: {field}"
        if isinstance(event[field], bool) or not isinstance(event[field], kind):
            return f"wrong type for field: {field}"

    if event["event_type"] != "reading":
        return f"unsupported event_type: {event['event_type']}"
    if not 1 <= event["machine_id"] <= 100:
        return f"machine_id not allowed: {event['machine_id']}"

    event_time, ingest_time = _parse_time(event["event_time"]), _parse_time(event["ingest_time"])
    if event_time is None or ingest_time is None:
        return "event_time or ingest_time cannot be parsed"
    if event_time > ingest_time + timedelta(minutes=10):
        return "event_time is later than ingest_time"

    for field, (low, high) in RANGES.items():
        if not low <= event[field] <= high:
            return f"{field} out of range: {event[field]}"

    if event["source"] not in SOURCES:
        return f"source not allowed: {event['source']}"
    return None
