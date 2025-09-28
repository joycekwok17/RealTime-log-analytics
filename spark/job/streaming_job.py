import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transforms import parse_kafka_json, agg_active_users

KAFKA_BOOT = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-logs")

PG_URL  = os.getenv("PG_URL",  "jdbc:postgresql://localhost:5432/analytics")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")
PG_TABLE = os.getenv("PG_TABLE", "active_users_5m")

WINDOW_MIN = int(os.getenv("WINDOW_MIN", "5"))
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/active-users")

def write_batch_to_pg(df, epoch_id: int):
    df.withColumn("ingest_ts", F.current_timestamp()) \      .write \      .mode("append") \      .format("jdbc") \      .option("url", PG_URL) \      .option("dbtable", PG_TABLE) \      .option("user", PG_USER) \      .option("password", PG_PASS) \      .option("driver", "org.postgresql.Driver") \      .save()

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("active-users-stream")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOT)
           .option("subscribe", KAFKA_TOPIC)
           .option("startingOffsets", "latest")
           .load())

    parsed = parse_kafka_json(raw).na.drop(subset=["event_time", "action", "user_id"])
    agged  = agg_active_users(parsed, WINDOW_MIN)

    (agged.writeStream
          .foreachBatch(write_batch_to_pg)
          .outputMode("update")
          .option("checkpointLocation", CHECKPOINT)
          .trigger(processingTime="15 seconds")
          .start()
          .awaitTermination())
