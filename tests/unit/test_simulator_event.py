import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "simulator"))

from simulator import EVENT_FIELDS, build_event  # noqa: E402

ROW = {
    "datetime": "2015-01-01 06:00:00",
    "machineID": 7,
    "volt": 176.2,
    "rotate": 418.5,
    "pressure": 113.1,
    "vibration": 45.1,
}


def test_event_has_every_schema_field():
    assert set(EVENT_FIELDS) == set(build_event(ROW))


def test_event_values():
    event = build_event(ROW)
    assert event["event_type"] == "reading"
    assert event["machine_id"] == 7
    assert event["event_time"] == "2015-01-01T06:00:00Z"
    assert event["schema_version"] == 1
    assert event["source"] == "dataset"


def test_event_ids_are_unique():
    assert build_event(ROW)["event_id"] != build_event(ROW)["event_id"]
