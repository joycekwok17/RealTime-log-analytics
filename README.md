# Real-Time Log Analytics (Kafka → Spark → Postgres/BigQuery → FastAPI)

An end-to-end demo that streams JSON logs into Kafka, aggregates active users with Spark Structured Streaming, stores results in Postgres (or BigQuery), and serves them with a FastAPI microservice.

## Quickstart

### 0) Prereqs
- Docker + Docker Compose
- Python 3.10+
- (Optional) Java/JDK for local Spark

### 1) Bring up infra
```bash
docker compose up -d
```

### 2) Start the producer
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r producer/requirements.txt
python producer/produce_logs.py
```

### 3) Run the Spark streaming job
```bash
pip install -r spark/requirements.txt
PYTHONPATH=$(pwd) spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:42.7.3 \
  spark/job/streaming_job.py
```

### 4) Run the API
```bash
pip install -r api/requirements.txt
cp .env.example .env
uvicorn api.main:app --reload --port 8000
```

Query:
```bash
curl -X POST http://127.0.0.1:8000/active_users -H "Content-Type: application/json" -d '{"minutes":5,"limit":5}'
```

### 5) Tests
```bash
pip install -r spark/requirements.txt
pytest -q
```

### 6) (Optional) BigQuery sink
See `spark/job/streaming_job_bq.py` and the README instructions in the code comments.

## Repo Layout
```
realtime-log-analytics/
├─ docker-compose.yml
├─ .env.example
├─ producer/
│  ├─ requirements.txt
│  └─ produce_logs.py
├─ spark/
│  ├─ requirements.txt
│  ├─ job/
│  │  ├─ schema.py
│  │  ├─ transforms.py
│  │  ├─ streaming_job.py
│  │  └─ streaming_job_bq.py
│  └─ tests/
│     └─ test_transforms.py
├─ api/
│  ├─ requirements.txt
│  └─ main.py
└─ .github/workflows/
   └─ python-ci.yml
```

## Notes
- Uses Redpanda for Kafka API compatibility (no ZooKeeper).
- Default sink is Postgres; switch to BigQuery with the BQ variant.
- Clean code & tests included; CI via GitHub Actions.
