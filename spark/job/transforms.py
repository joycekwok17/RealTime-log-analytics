from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from .schema import LOG_SCHEMA

def parse_kafka_json(df) -> DataFrame:
    """Kafka raw -> parsed JSON columns + proper timestamp"""
    parsed = df.selectExpr("CAST(value AS STRING) as json") \               .select(F.from_json("json", LOG_SCHEMA).alias("data")) \               .select("data.*")
    parsed = parsed.withColumn("event_time", F.to_timestamp("timestamp"))
    return parsed.drop("timestamp")

def agg_active_users(df: DataFrame, window_minutes: int = 5) -> DataFrame:
    """Active users per action per tumbling window"""
    windowed = df.groupBy(
        F.window("event_time", f"{window_minutes} minutes").alias("w"),
        F.col("action")
    ).agg(F.countDistinct("user_id").alias("active_users"))
    return windowed.select(
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "action",
        "active_users"
    )
