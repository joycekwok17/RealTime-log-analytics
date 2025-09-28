import pytest
from pyspark.sql import SparkSession, Row
from spark.job.transforms import parse_kafka_json, agg_active_users

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .master("local[2]")
            .appName("tests")
            .getOrCreate())

def test_parse_and_agg(spark):
    payloads = [
        '{"event_id":"e1","user_id":1,"action":"view","timestamp":"2024-01-01T00:00:10Z"}',
        '{"event_id":"e2","user_id":2,"action":"view","timestamp":"2024-01-01T00:01:10Z"}',
        '{"event_id":"e3","user_id":1,"action":"click","timestamp":"2024-01-01T00:02:10Z"}'
    ]
    df = spark.createDataFrame([Row(value=p.encode("utf-8")) for p in payloads])

    parsed = parse_kafka_json(df)
    agged = agg_active_users(parsed, window_minutes=5).orderBy("action")

    rows = [r.asDict() for r in agged.collect()]
    by_action = {r["action"]: r["active_users"] for r in rows}
    assert by_action["click"] == 1
    assert by_action["view"] == 2
