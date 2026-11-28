import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "streaming" / "jobs"))
sys.path.insert(0, str(ROOT / "simulator"))

from validate import reject_reason  # noqa: E402

from simulator import apply_fault, build_event  # noqa: E402

GOOD = {
    "event_id": "5b1d6c0e-3f1a-4c1e-9a55-0d3f6a2b7c11",
    "event_type": "reading",
    "machine_id": 1,
    "event_time": "2015-01-01T06:00:00Z",
    "ingest_time": "2026-11-07T20:00:00Z",
    "schema_version": 1,
    "source": "dataset",
    "volt": 176.2,
    "rotate": 418.5,
    "pressure": 113.1,
    "vibration": 45.1,
}
ROW = {
    "datetime": "2015-07-01 06:00:00",
    "machineID": 7,
    "volt": 170.0,
    "rotate": 446.0,
    "pressure": 100.0,
    "vibration": 40.0,
}


def changed(**fields) -> str:
    return json.dumps({**GOOD, **fields})


def test_good_events_pass():
    assert reject_reason(json.dumps(GOOD)) is None
    assert reject_reason(changed(volt=260)) is None  # abnormal, but still a valid reading
    assert reject_reason(changed(volt=176)) is None  # an integer is a number too


def test_every_rule_has_a_reason():
    assert reject_reason("not json at all") == "not valid JSON"
    assert reject_reason("[1, 2]") == "not valid JSON"
    assert reject_reason(None) == "not valid JSON"
    assert reject_reason('{"machine_id": 3}') == "missing required field: event_id"
    assert reject_reason(changed(volt="high")) == "wrong type for field: volt"
    assert reject_reason(changed(machine_id="7")) == "wrong type for field: machine_id"
    assert reject_reason(changed(event_type="failure")) == "unsupported event_type: failure"
    assert reject_reason(changed(machine_id=101)) == "machine_id not allowed: 101"
    assert reject_reason(changed(event_time="yesterday")).endswith("cannot be parsed")
    future = changed(event_time="2099-01-01T00:00:00Z")
    assert reject_reason(future).startswith("event_time is later")
    assert reject_reason(changed(volt=999)) == "volt out of range: 999"
    assert reject_reason(changed(rotate=-1)) == "rotate out of range: -1"
    assert reject_reason(changed(source="guess")) == "source not allowed: guess"


def test_simulator_events_follow_the_contract():
    for fault in (None, "spike", "drift", "duplicate"):
        for payload in apply_fault(build_event(ROW), fault):
            assert reject_reason(payload.decode()) is None, fault


def test_faults_do_what_they_say():
    spike = json.loads(apply_fault(build_event(ROW), "spike")[0])
    assert spike["volt"] > 240 and spike["vibration"] > 65 and spike["source"] == "injected"
    drift = json.loads(apply_fault(build_event(ROW), "drift")[0])
    assert 46 < drift["vibration"] < 65  # under the per-reading limit, over the 24-hour limit
    assert len(apply_fault(build_event(ROW), "duplicate")) == 2
    for _ in range(20):
        (payload,) = apply_fault(build_event(ROW), "malformed")
        assert reject_reason(payload.decode()) is not None
