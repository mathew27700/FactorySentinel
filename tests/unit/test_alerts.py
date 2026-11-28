"""Unit tests for the 24-hour alert rules (plain Python, no Spark session, no database)."""

import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "streaming" / "jobs"))

# job.py imports psycopg2 at the top; the rules under test never touch the database.
if "psycopg2" not in sys.modules:
    pg = types.ModuleType("psycopg2")
    extras = types.ModuleType("psycopg2.extras")
    extras.execute_values = lambda *args, **kwargs: None
    pg.extras = extras
    sys.modules["psycopg2"] = pg
    sys.modules["psycopg2.extras"] = extras

from job import DAY_RULES, MIN_READINGS_PER_DAY, READING_RULES, day_alerts  # noqa: E402


def day(**over):
    row = {
        "machine_id": 7,
        "window_start": "2015-07-01T00:00:00Z",
        "reading_count": 24,
        "avg_volt": 170.0,
        "avg_rotate": 446.0,
        "avg_pressure": 100.0,
        "avg_vibration": 40.0,
    }
    row.update(over)
    return row


def test_normal_day_raises_nothing():
    assert day_alerts([day()]) == []


def test_each_day_rule_fires_on_its_own():
    cases = {
        "VOLT_HIGH_24H": {"avg_volt": 190.0},
        "ROTATION_LOW_24H": {"avg_rotate": 400.0},
        "PRESSURE_HIGH_24H": {"avg_pressure": 115.0},
        "VIBRATION_HIGH_24H": {"avg_vibration": 50.0},
    }
    for rule, over in cases.items():
        alerts = day_alerts([day(**over)])
        assert [a["rule"] for a in alerts] == [rule]
        assert alerts[0]["scope"] == "day"
        assert alerts[0]["severity"] == "critical"
        assert alerts[0]["event_time"] == "2015-07-01T00:00:00Z"


def test_incomplete_day_is_not_judged():
    short = day(reading_count=MIN_READINGS_PER_DAY - 1, avg_volt=999.0)
    assert day_alerts([short]) == []
    enough = day(reading_count=MIN_READINGS_PER_DAY, avg_volt=999.0)
    assert [a["rule"] for a in day_alerts([enough])] == ["VOLT_HIGH_24H"]


def test_alert_id_is_stable_and_unique_per_machine_rule_day():
    a1 = day_alerts([day(avg_volt=190.0)])[0]["alert_id"]
    a2 = day_alerts([day(avg_volt=250.0)])[0]["alert_id"]  # same key, different value
    other_machine = day_alerts([day(machine_id=8, avg_volt=190.0)])[0]["alert_id"]
    other_day = day_alerts([day(window_start="2015-07-02T00:00:00Z", avg_volt=190.0)])[0][
        "alert_id"
    ]
    assert a1 == a2
    assert len({a1, other_machine, other_day}) == 3


def test_rules_match_the_contract():
    assert {r[0]: (r[2], r[3]) for r in READING_RULES} == {
        "HIGH_VOLT": (">", 240.0),
        "LOW_ROTATION": ("<", 210.0),
        "HIGH_PRESSURE": (">", 150.0),
        "HIGH_VIBRATION": (">", 65.0),
    }
    assert {r[0]: (r[2], r[3]) for r in DAY_RULES} == {
        "VOLT_HIGH_24H": (">", 185.0),
        "ROTATION_LOW_24H": ("<", 410.0),
        "PRESSURE_HIGH_24H": (">", 112.0),
        "VIBRATION_HIGH_24H": (">", 46.0),
    }
