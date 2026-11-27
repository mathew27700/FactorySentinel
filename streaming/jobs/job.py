"""FactorySentinel stream job.

Step A2: Kafka (sensor.raw) -> typed columns -> console.
Later steps add Postgres, validation, alerts and the lake.
Everything it needs to know about names and rules is in docs/contract.md.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

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


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("factorysentinel")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    query = (
        parse(read_raw(spark))
        .writeStream.format("console")
        .option("numRows", 10)
        .option("truncate", "false")
        .option("checkpointLocation", f"{CKPT}/console")
        .start()
    )
    query.awaitTermination()
