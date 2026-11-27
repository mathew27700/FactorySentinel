"""Placeholder for the Spark job (step 0.4). The real job is written in steps A2 to A6."""

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("factorysentinel").getOrCreate()
    print(f"spark {spark.version} ready")
    spark.stop()
