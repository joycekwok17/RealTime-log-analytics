CREATE TABLE IF NOT EXISTS active_users (
  window_start TIMESTAMP,
  window_end   TIMESTAMP,
  action       TEXT,
  active_users BIGINT,
  ingest_ts    TIMESTAMP
);
