"""FactorySentinel stream job.

Step A4: Kafka (sensor.raw) -> validate -> Postgres live_readings, rejected_events and sensor.dlq.
Later steps add alerts, windows and the lake.
Everything it needs to know about names and rules is in docs/contract.md.
"""

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


def process_batch(batch, batch_id):
    """Runs once per micro-batch: split good from bad, then write everything."""
    batch = batch.withColumn("reason", reason_udf("raw")).persist()
    try:
        write_rejected(batch.filter("reason IS NOT NULL"))
        valid = parse(batch.filter("reason IS NULL"))
        write_readings(valid)
    finally:
        batch.unpersist()


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("factorysentinel")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    query = (
        read_raw(spark)
        .writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", f"{CKPT}/main")
        .trigger(processingTime="2 seconds")
        .start()
    )
    query.awaitTermination()
