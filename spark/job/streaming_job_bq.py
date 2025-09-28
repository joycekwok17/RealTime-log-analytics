import os
from pyspark.sql import SparkSession
from transforms import parse_kafka_json, agg_active_users

KAFKA_BOOT = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-logs")
BQ_TABLE = f"{os.getenv('GCP_PROJECT')}.{os.getenv('BQ_DATASET')}.{os.getenv('BQ_TABLE','active_users')}"
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/bq")
WINDOW_MIN = int(os.getenv("WINDOW_MIN", "5"))

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("active-users-stream-bq")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOT)
           .option("subscribe", KAFKA_TOPIC)
           .option("startingOffsets", "latest").load())
    parsed = parse_kafka_json(raw)
    agged = agg_active_users(parsed, WINDOW_MIN)

    # Write the results to BigQuery
    (agged.writeStream
         .format("bigquery")
         .option("table", BQ_TABLE)
         .option("checkpointLocation", CHECKPOINT)
         .outputMode("append")
         .start()
         .awaitTermination())
