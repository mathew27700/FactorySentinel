"""Unit tests for the Spark schema of sensor.raw events (no JVM needed)."""

import importlib.util
from pathlib import Path

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_FILE = ROOT / "streaming" / "schemas" / "sensor_event.py"

EXPECTED_TYPES = {
    "event_id": StringType,
    "event_type": StringType,
    "machine_id": IntegerType,
    "event_time": StringType,
    "ingest_time": StringType,
    "schema_version": IntegerType,
    "source": StringType,
    "volt": DoubleType,
    "rotate": DoubleType,
    "pressure": DoubleType,
    "vibration": DoubleType,
    "error_id": StringType,
    "component": StringType,
}

# The example "reading" event from docs/event-schema.md.
READING_EXAMPLE = {
    "event_id": "5b1d6c0e-3f1a-4c1e-9a55-0d3f6a2b7c11",
    "event_type": "reading",
    "machine_id": 1,
    "event_time": "2015-01-01T06:00:00Z",
    "ingest_time": "2026-11-07T20:00:00Z",
    "schema_version": 1,
    "source": "dataset",
    "volt": 176.217853,
    "rotate": 418.504078,
    "pressure": 113.077935,
    "vibration": 45.087686,
}


def load_schema() -> StructType:
    spec = importlib.util.spec_from_file_location("sensor_event", SCHEMA_FILE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.sensor_event_schema


def test_field_names_and_types_match_schema_doc() -> None:
    schema = load_schema()
    actual = {field.name: type(field.dataType) for field in schema.fields}
    assert actual == EXPECTED_TYPES


def test_all_fields_are_nullable() -> None:
    schema = load_schema()
    assert all(field.nullable for field in schema.fields)


def test_reading_example_only_uses_known_fields() -> None:
    schema = load_schema()
    assert set(READING_EXAMPLE) <= set(schema.fieldNames())
