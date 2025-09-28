# kafka producer that generates synthetic user activity logs and sends them to a Kafka topic
import json, os, random, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

TOPIC = os.getenv("KAFKA_TOPIC", "user-logs")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

actions = ["view", "click", "purchase", "signup", "like"]

# Generate a synthetic user activity event as a JSON object with random values,
# for user_id, action, and timestamp fields
def make_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 10000),
        "action": random.choice(actions),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metadata": {"page": random.choice(["home", "search", "product", "cart"])},
    }

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP, # broker address
        value_serializer=lambda v: json.dumps(v).encode("utf-8"), # serialize to JSON 
        retries=3, # retry up to 3 times on failure
    )
    print(f"Producing to {BOOTSTRAP} topic={TOPIC} ... Ctrl+C to stop")

    while True:
        producer.send(TOPIC, make_event())  # send event to Kafka
        producer.flush() # ensure all messages are sent, not buffered
        time.sleep(0.05)  # ~20 msgs/sec
