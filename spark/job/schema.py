from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType

LOG_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", LongType(), False),
    StructField("action", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("metadata", MapType(StringType(), StringType()), True),
])
