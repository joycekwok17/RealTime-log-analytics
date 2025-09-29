# spark streaming job that reads user activity logs from Kafka, 
# processes them to compute active users in time windows, 
# and writes the results to PostgreSQL
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from spark.job.transforms import parse_kafka_json, agg_active_users
from config.loader import load_config

config = load_config()

KAFKA_BOOT = config["kafka"]["bootstrap_servers"]
KAFKA_TOPIC = config["kafka"]["topic"]

# PostgreSQL connection parameters 
PG_URL  = config["db"]["url"]
PG_USER = config["db"]["user"]
PG_PASS = config["db"]["password"]
PG_TABLE = config["db"]["table"]

WINDOW_MIN = int(config["spark"]["window_duration"].split(" ")[0])
CHECKPOINT = config["spark"]["checkpoint_location"]

# write each micro-batch to PostgreSQL
def write_batch_to_pg(df, epoch_id: int):
    df.withColumn("ingest_ts", F.current_timestamp()) \
      .write \
      .mode("append") \
      .format("jdbc") \
      .option("url", PG_URL) \
      .option("dbtable", PG_TABLE) \
      .option("user", PG_USER) \
      .option("password", PG_PASS) \
      .option("driver", "org.postgresql.Driver") \
      .save()

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("active-users-stream")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka 
    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOT)
           .option("subscribe", KAFKA_TOPIC)
           .option("startingOffsets", "latest")
           .load())
    
    # parses JSON into columns
    parsed = parse_kafka_json(raw).na.drop(subset=["event_time", "action", "user_id"])
    agged  = agg_active_users(parsed, WINDOW_MIN)

    # Write the results to PostgreSQL
    (agged.writeStream
          .foreachBatch(write_batch_to_pg)
          .outputMode("update")
          .option("checkpointLocation", CHECKPOINT)
          .trigger(processingTime="15 seconds")
          .start()
          .awaitTermination())
