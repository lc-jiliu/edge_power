"""Centralised configuration — all values from environment variables."""
import os

SITE_ID        = os.getenv("SITE_ID", "site-a")
REDIS_URL      = os.getenv("REDIS_URL", "redis://localhost:6379")
CLOUD_WS_URL   = os.getenv("CLOUD_WS_URL", "ws://localhost:8000/edge/site-a")
DB_PATH        = os.getenv("DB_PATH", "/data/edge.db")
SIM_RATE       = float(os.getenv("SIM_RATE", "0.1"))      # seconds per tick
BATCH_INTERVAL = float(os.getenv("BATCH_INTERVAL", "60")) # seconds between WARNING batches
MODEL_PATH     = os.getenv("MODEL_PATH", "models/isolation_forest.pkl")

# Redis stream names
STREAM_RAW       = "raw-events"
STREAM_PRIORITY  = "prioritized-events"
GROUP_DETECTOR   = "detector-group"
GROUP_FORWARDER  = "forwarder-group"
CONSUMER_NAME    = "worker-1"
