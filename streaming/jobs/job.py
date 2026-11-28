"""FactorySentinel stream job.

Step A5: Kafka (sensor.raw) -> validate -> Postgres (readings, rejects, alerts, daily windows).
The lake comes in A6.
Everything it needs to know about names and rules is in docs/contract.md.
"""

import hashlib
import json
import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from validate import reject_reason

CKPT = os.environ.get("CHECKPOINT_PATH", "/data/checkpoints")
LAKE = os.environ.get("LAKE_PATH", "/data/lake")
ISO = "yyyy-MM-dd'T'HH:mm:ss'Z'"  # how timestamps are handed to Postgres and Kafka

# [read]
SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("machine_id", T.IntegerType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("ingest_time", T.StringType()),
        T.StructField("schema_version", T.IntegerType()),
        T.StructField("source", T.StringType()),
        T.StructField("volt", T.DoubleType()),
        T.StructField("rotate", T.DoubleType()),
        T.StructField("pressure", T.DoubleType()),
        T.StructField("vibration", T.DoubleType()),
    ]
)


def kafka_options(prefix="KAFKA"):
    """Connection settings. Local Kafka needs only the address; a cloud endpoint adds SASL."""
    opts = {"kafka.bootstrap.servers": os.environ[f"{prefix}_BOOTSTRAP"]}
    if os.environ.get(f"{prefix}_SECURITY_PROTOCOL", "PLAINTEXT") != "PLAINTEXT":
        opts["kafka.security.protocol"] = "SASL_SSL"
        opts["kafka.sasl.mechanism"] = "PLAIN"
        opts["kafka.sasl.jaas.config"] = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            'username="$ConnectionString" '  # literally this text, for Fabric and Event Hubs
            f'password="{os.environ[f"{prefix}_SASL_PASSWORD"]}";'
        )
    return opts


def read_raw(spark):
    """Every Kafka message as text, together with its address (partition, offset)."""
    return (
        spark.readStream.format("kafka")
        .options(**kafka_options())
        .option("subscribe", os.environ.get("TOPIC_RAW", "sensor.raw"))
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 5000)
        .option("failOnDataLoss", "false")
        .load()
        .select("partition", "offset", F.col("value").cast("string").alias("raw"))
    )


def parse(df):
    """Text -> typed columns. event_ts is dataset time (2015); ingest_ts is the real clock."""
    return (
        df.withColumn("e", F.from_json("raw", SCHEMA))
        .select("partition", "offset", "e.*")
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withColumn("ingest_ts", F.to_timestamp("ingest_time"))
    )


# [/read]


# [postgres]


def pg_connect():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        sslmode=os.environ.get("POSTGRES_SSLMODE", "prefer"),
    )


def upsert(sql, rows):
    """Run one INSERT ... ON CONFLICT for all rows, in a single transaction."""
    if not rows:
        return
    conn = pg_connect()
    try:
        with conn, conn.cursor() as cur:  # commits on success, rolls back on any error
            execute_values(cur, sql, rows)
    finally:
        conn.close()


SQL_READINGS = """
    INSERT INTO live_readings
        (machine_id, event_time, event_id, ingest_time, source, volt, rotate, pressure, vibration)
    VALUES %s
    ON CONFLICT (machine_id, event_time) DO UPDATE SET
        event_id = EXCLUDED.event_id, ingest_time = EXCLUDED.ingest_time, source = EXCLUDED.source,
        volt = EXCLUDED.volt, rotate = EXCLUDED.rotate,
        pressure = EXCLUDED.pressure, vibration = EXCLUDED.vibration, stored_at = now()"""


def write_readings(valid):
    rows = (
        valid.dropDuplicates(["machine_id", "event_time"])  # same reading twice in one batch
        .select(
            "machine_id",
            F.date_format("event_ts", ISO),
            "event_id",
            F.date_format("ingest_ts", ISO),
            "source",
            "volt",
            "rotate",
            "pressure",
            "vibration",
        )
        .collect()
    )
    upsert(SQL_READINGS, [tuple(r) for r in rows])


# [/postgres]


# [dlq]
reason_udf = F.udf(reject_reason, T.StringType())

SQL_REJECTED = """
    INSERT INTO rejected_events (source_partition, source_offset, reason, raw_payload)
    VALUES %s
    ON CONFLICT (source_partition, source_offset) DO NOTHING"""


def to_kafka(df, topic, prefix="KAFKA"):
    """Write a batch (one JSON string per row, in a column called value) to a Kafka topic."""
    df.write.format("kafka").options(**kafka_options(prefix)).option("topic", topic).save()


def write_rejected(rejected):
    rows = rejected.select("partition", "offset", "reason", "raw").collect()
    if not rows:
        return
    upsert(SQL_REJECTED, [tuple(r) for r in rows])
    message = F.to_json(
        F.struct("reason", "raw", F.date_format(F.current_timestamp(), ISO).alias("rejected_at"))
    )
    to_kafka(rejected.select(message.alias("value")), os.environ.get("TOPIC_DLQ", "sensor.dlq"))


# [/dlq]


# [alerts]
# rule, column, comparison, threshold, severity (docs/contract.md, section 4)
READING_RULES = [
    ("HIGH_VOLT", "volt", ">", 240.0, "warning"),
    ("LOW_ROTATION", "rotate", "<", 210.0, "warning"),
    ("HIGH_PRESSURE", "pressure", ">", 150.0, "warning"),
    ("HIGH_VIBRATION", "vibration", ">", 65.0, "warning"),
]
DAY_RULES = [
    ("VOLT_HIGH_24H", "avg_volt", ">", 185.0, "critical"),
    ("ROTATION_LOW_24H", "avg_rotate", "<", 410.0, "critical"),
    ("PRESSURE_HIGH_24H", "avg_pressure", ">", 112.0, "critical"),
    ("VIBRATION_HIGH_24H", "avg_vibration", ">", 46.0, "critical"),
]
MIN_READINGS_PER_DAY = 20  # a day with fewer readings is too incomplete to judge

SQL_ALERTS = """
    INSERT INTO alerts
        (alert_id, machine_id, rule, severity, scope, metric, value, threshold, event_time)
    VALUES %s
    ON CONFLICT (alert_id) DO NOTHING"""
ALERT_COLUMNS = [
    "alert_id",
    "machine_id",
    "rule",
    "severity",
    "scope",
    "metric",
    "value",
    "threshold",
    "event_time",
]


def reading_alerts(valid):
    """One row per (reading, broken rule)."""
    out = None
    for rule, metric, op, threshold, severity in READING_RULES:
        broken = F.col(metric) > threshold if op == ">" else F.col(metric) < threshold
        part = valid.filter(broken).select(
            F.md5(F.concat_ws("|", "machine_id", F.lit(rule), "event_time")).alias("alert_id"),
            "machine_id",
            F.lit(rule).alias("rule"),
            F.lit(severity).alias("severity"),
            F.lit("reading").alias("scope"),
            F.lit(metric).alias("metric"),
            F.col(metric).alias("value"),
            F.lit(threshold).alias("threshold"),
            F.date_format("event_ts", ISO).alias("event_time"),
        )
        out = part if out is None else out.unionByName(part)
    return out.dropDuplicates(["alert_id"])


def write_alerts(alerts_df):
    rows = alerts_df.collect()
    if not rows:
        return
    upsert(SQL_ALERTS, [tuple(r[c] for c in ALERT_COLUMNS) for r in rows])
    message = F.to_json(F.struct(*ALERT_COLUMNS)).alias("value")
    to_kafka(alerts_df.select(message), os.environ.get("TOPIC_ALERTS", "sensor.alerts"))


# [/alerts]


# [batch]
def process_batch(batch, batch_id):
    """Runs once per micro-batch: split good from bad, then write everything."""
    batch = batch.withColumn("reason", reason_udf("raw")).persist()
    try:
        write_rejected(batch.filter("reason IS NOT NULL"))
        valid = parse(batch.filter("reason IS NULL"))
        write_readings(valid)
        write_alerts(reading_alerts(valid))
        if os.environ.get("CLOUD_BOOTSTRAP"):  # optional: copy clean events to Fabric / Event Hubs
            clean = valid.drop("partition", "offset", "event_ts", "ingest_ts")
            to_kafka(
                clean.select(F.to_json(F.struct("*")).alias("value")),
                os.environ["CLOUD_TOPIC"],
                prefix="CLOUD",
            )
    finally:
        batch.unpersist()


# [/batch]


# [windows]
SQL_AGG = """
    INSERT INTO agg_machine_day
        (machine_id, window_start, window_end, reading_count,
         avg_volt, avg_rotate, avg_pressure, avg_vibration,
         max_volt, min_rotate, max_pressure, max_vibration)
    VALUES %s
    ON CONFLICT (machine_id, window_start) DO UPDATE SET
        window_end = EXCLUDED.window_end, reading_count = EXCLUDED.reading_count,
        avg_volt = EXCLUDED.avg_volt, avg_rotate = EXCLUDED.avg_rotate,
        avg_pressure = EXCLUDED.avg_pressure, avg_vibration = EXCLUDED.avg_vibration,
        max_volt = EXCLUDED.max_volt, min_rotate = EXCLUDED.min_rotate,
        max_pressure = EXCLUDED.max_pressure, max_vibration = EXCLUDED.max_vibration,
        updated_at = now()"""
AGG_COLUMNS = [
    "machine_id",
    "window_start",
    "window_end",
    "reading_count",
    "avg_volt",
    "avg_rotate",
    "avg_pressure",
    "avg_vibration",
    "max_volt",
    "min_rotate",
    "max_pressure",
    "max_vibration",
]


def day_windows(valid):
    """One row per machine per finished dataset day (finished = the watermark has passed it)."""
    return (
        valid.withWatermark("event_ts", "2 hours")
        .dropDuplicates(["machine_id", "event_ts"])  # a reading sent twice must count once
        .groupBy(F.window("event_ts", "1 day"), "machine_id")
        .agg(
            F.count("*").alias("reading_count"),
            F.avg("volt").alias("avg_volt"),
            F.avg("rotate").alias("avg_rotate"),
            F.avg("pressure").alias("avg_pressure"),
            F.avg("vibration").alias("avg_vibration"),
            F.max("volt").alias("max_volt"),
            F.min("rotate").alias("min_rotate"),
            F.max("pressure").alias("max_pressure"),
            F.max("vibration").alias("max_vibration"),
        )
        .withColumn("window_start", F.date_format("window.start", ISO))
        .withColumn("window_end", F.date_format("window.end", ISO))
        .select(*AGG_COLUMNS)
    )


def day_alerts(rows):
    """Apply the 24-hour rules to finished days. Plain Python: there are at most 100 rows a day."""
    alerts = []
    for r in rows:
        if r["reading_count"] < MIN_READINGS_PER_DAY:
            continue
        for rule, metric, op, threshold, severity in DAY_RULES:
            if (r[metric] > threshold) if op == ">" else (r[metric] < threshold):
                key = f"{r['machine_id']}|{rule}|{r['window_start']}"
                alerts.append(
                    {
                        "alert_id": hashlib.md5(key.encode()).hexdigest(),
                        "machine_id": r["machine_id"],
                        "rule": rule,
                        "severity": severity,
                        "scope": "day",
                        "metric": metric,
                        "value": r[metric],
                        "threshold": threshold,
                        "event_time": r["window_start"],
                    }
                )
    return alerts


def write_windows(batch, batch_id):
    rows = batch.collect()
    upsert(SQL_AGG, [tuple(r[c] for c in AGG_COLUMNS) for r in rows])
    alerts = day_alerts(rows)
    if alerts:
        upsert(SQL_ALERTS, [tuple(a[c] for c in ALERT_COLUMNS) for a in alerts])
        values = [(json.dumps(a),) for a in alerts]
        messages = batch.sparkSession.createDataFrame(values, "value string")
        to_kafka(messages, os.environ.get("TOPIC_ALERTS", "sensor.alerts"))


# [/windows]


# [start]
def start_queries(raw):
    """Two streaming queries so far (the lake query comes in A6), each with its own checkpoint."""
    valid = parse(raw.filter(reason_udf("raw").isNull()))

    main = (
        raw.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", f"{CKPT}/main")
        .trigger(processingTime="2 seconds")
        .start()
    )
    windows = (
        day_windows(valid)
        .writeStream.outputMode("append")
        .foreachBatch(write_windows)
        .option("checkpointLocation", f"{CKPT}/windows")
        .trigger(processingTime="5 seconds")
        .start()
    )
    return main, windows


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("factorysentinel")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    start_queries(read_raw(spark))
    spark.streams.awaitAnyTermination()
# [/start]
