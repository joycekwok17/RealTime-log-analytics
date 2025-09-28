import os
import psycopg2
from fastapi import FastAPI
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

PG_CONN = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    dbname=os.getenv("PG_DB", "analytics"),
    user=os.getenv("PG_USER", "postgres"),
    password=os.getenv("PG_PASS", "postgres"),
)

app = FastAPI(title="Active Users Analytics API")

class WindowRequest(BaseModel):
    minutes: int = 5
    limit: int = 20

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/active_users")
def active_users(req: WindowRequest):
    q = """
    SELECT window_start, window_end, action, active_users
    FROM active_users_5m
    WHERE window_end IS NOT NULL
    ORDER BY window_end DESC
    LIMIT %s
    """
    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (req.limit,))
            rows = cur.fetchall()
    return [
        {
            "window_start": str(r[0]),
            "window_end": str(r[1]),
            "action": r[2],
            "active_users": int(r[3]),
        }
        for r in rows
    ]
