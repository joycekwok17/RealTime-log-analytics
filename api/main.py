# main.py: FastAPI application that provides endpoints to check service health and retrieve active user analytics from PostgreSQL
import psycopg 
from fastapi import FastAPI
from pydantic import BaseModel
from config.loader import load_config


config = load_config()

PG_CONN = dict(
    host=config["db"]["host"],
    port=int(config["db"]["port"]),
    dbname=config["db"]["dbname"],
    user=config["db"]["user"],
    password=config["db"]["password"],
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
    FROM active_users
    WHERE window_end IS NOT NULL
    ORDER BY window_end DESC
    LIMIT %s
    """

    with psycopg.connect(**PG_CONN) as conn:
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
