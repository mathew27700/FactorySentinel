"""Spark schema for the JSON events on the Kafka topic ``sensor.raw``.

Source of truth: ``docs/event-schema.md`` (event schema v1). Keep this file and that document
in sync. A schema change goes through a pull request reviewed by the whole team.

All fields are nullable on purpose. A field that is missing from the JSON becomes ``null`` in
the parsed row, and the validation step (week 3) decides which events are invalid and sends
them to the dead-letter topic with a reason. ``error`` and ``failure`` events also omit the
sensor fields, so those fields are legitimately null there. How wrongly typed values and
broken JSON behave in Spark 4.2 still has to be tested in week 3.

``event_time`` and ``ingest_time`` stay strings here (ISO-8601 UTC, for example
``2015-01-01T06:00:00Z``). They are converted with ``to_timestamp`` after parsing, so one badly
formatted timestamp does not hide the other fields of the event.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

sensor_event_schema = StructType(
    [
        # Common fields: present on every event.
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),  # reading | error | failure
        StructField("machine_id", IntegerType(), True),
        StructField("event_time", StringType(), True),
        StructField("ingest_time", StringType(), True),
        StructField("schema_version", IntegerType(), True),
        StructField("source", StringType(), True),  # dataset | injected
        # Only on "reading" events.
        StructField("volt", DoubleType(), True),
        StructField("rotate", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("vibration", DoubleType(), True),
        # Only on "error" events.
        StructField("error_id", StringType(), True),
        # Only on "failure" events.
        StructField("component", StringType(), True),
    ]
)
