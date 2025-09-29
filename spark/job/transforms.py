# Transform functions for Spark Structured Streaming
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from spark.job.schema import LOG_SCHEMA

def parse_kafka_json(df) -> DataFrame:
    """Kafka raw -> parsed JSON columns + proper timestamp"""
    parsed = (
        df.selectExpr("CAST(value AS STRING) as json")
        .select(F.from_json("json", LOG_SCHEMA).alias("data"))
        .select("data.*")
    )
    parsed = parsed.withColumn("event_time", F.to_timestamp("timestamp"))
    return parsed.drop("timestamp")

# Active users aggregation transform function for Spark Structured Streaming
def agg_active_users(df: DataFrame, window_minutes: int = 5) -> DataFrame:
    """Active users per action per tumbling window"""
    windowed = df.groupBy(
        F.window("event_time", f"{window_minutes} minutes").alias("w"),
        F.col("action") 
    ).agg(F.approx_count_distinct("user_id").alias("active_users")) # distinct user_ids per action per window
    return windowed.select(
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "action",
        "active_users"
    )
