import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "simulator"))

from simulator import (  # noqa: E402
    EVENT_FIELDS,
    active_faults,
    apply_fault,
    build_event,
    hour_messages,
)

SENSORS = ("volt", "rotate", "pressure", "vibration")


def row(machine_id, hour=6):
    return {
        "datetime": f"2015-07-01 {hour:02d}:00:00",
        "machineID": machine_id,
        "volt": 170.0,
        "rotate": 420.0,
        "pressure": 110.0,
        "vibration": 40.0,
    }


def is_valid_reading(payload):
    """True when the bytes are a JSON object with every field and numeric sensors."""
    try:
        data = json.loads(payload)
    except ValueError:
        return False
    return (
        isinstance(data, dict)
        and set(EVENT_FIELDS) <= set(data)
        and all(isinstance(data[name], int | float) for name in SENSORS)
    )


# active_faults: the control file


def test_missing_file_means_no_faults(tmp_path):
    assert active_faults(tmp_path / "nope.json") == {}


def test_half_written_file_means_no_faults(tmp_path):
    path = tmp_path / "faults.json"
    path.write_text('[{"machine_id": 3, "ty')
    assert active_faults(path) == {}


def test_expired_faults_are_ignored(tmp_path):
    path = tmp_path / "faults.json"
    path.write_text(
        json.dumps(
            [
                {"machine_id": 3, "type": "spike", "until": "2026-11-27T12:00:30Z"},
                {"machine_id": 4, "type": "drift", "until": "2026-11-27T11:59:00Z"},
            ]
        )
    )
    now = datetime(2026, 11, 27, 12, 0, 0, tzinfo=UTC)
    assert active_faults(path, now) == {3: "spike"}


# apply_fault: what one event turns into


def test_no_fault_sends_one_unchanged_message():
    event = build_event(row(1))
    messages = apply_fault(dict(event), None)
    assert len(messages) == 1
    assert json.loads(messages[0]) == event


def test_spike_multiplies_volt_and_vibration():
    event = apply_fault(build_event(row(1)), "spike")
    data = json.loads(event[0])
    assert data["volt"] == 170.0 * 1.5
    assert data["vibration"] == 40.0 * 1.7
    assert data["source"] == "injected"


def test_drift_only_changes_vibration():
    data = json.loads(apply_fault(build_event(row(1)), "drift")[0])
    assert data["vibration"] == 40.0 * 1.35
    assert data["volt"] == 170.0
    assert data["source"] == "injected"


def test_duplicate_sends_the_same_bytes_twice():
    messages = apply_fault(build_event(row(1)), "duplicate")
    assert len(messages) == 2
    assert messages[0] == messages[1]


def test_malformed_never_produces_a_valid_reading():
    for _ in range(100):  # the broken shape is chosen at random
        messages = apply_fault(build_event(row(1)), "malformed")
        assert len(messages) == 1
        assert not is_valid_reading(messages[0])


# hour_messages: everything for one simulated hour, no Kafka needed


def test_normal_hour_sends_one_message_per_machine_keyed_by_machine():
    out = hour_messages([row(1), row(2), row(3)], {}, {})
    assert [key for key, _ in out] == [1, 2, 3]
    assert all(is_valid_reading(payload) for _, payload in out)


def test_late_fault_holds_events_back_until_it_ends():
    held = {}
    first = hour_messages([row(1, 6), row(2, 6)], {2: "late"}, held)
    assert [key for key, _ in first] == [1]  # machine 2 is held back
    assert len(held[2]) == 1

    second = hour_messages([row(1, 7), row(2, 7)], {}, held)  # fault over
    assert [key for key, _ in second] == [2, 1, 2]  # the held one first, then the new hour
    assert held == {}
    # The held event keeps its original event_time (06:00), it is simply sent late.
    assert json.loads(second[0][1])["event_time"] == "2015-07-01T06:00:00Z"


def test_duplicate_fault_doubles_only_that_machine():
    out = hour_messages([row(1), row(2)], {2: "duplicate"}, {})
    assert [key for key, _ in out] == [1, 2, 2]
