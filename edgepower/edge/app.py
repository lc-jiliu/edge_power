"""
EdgePower — Edge Node Application Entry Point

Runs three asyncio services concurrently in a single process:
  1. FastAPI (uvicorn) — ingestion endpoint + health + stats
  2. Detection consumer — reads raw-events, classifies, writes prioritized-events
  3. Forwarder — reads prioritized-events, persists SQLite, sends to cloud via WebSocket

All three share the same event loop (asyncio) and a single Redis connection.
"""
import asyncio
import logging

from contextlib import asynccontextmanager
from fastapi import FastAPI
from redis.asyncio import Redis

import config
from ingestion.routes import log as ingestion_router
from ingestion.simulator import run_simulator
from detection.consumer import run_detection_consumer
from forwarder.consumer import run_forwarder_consumer
from forwarder.store import EdgeStore
from forwarder.transport import CloudTransport

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("edge")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start background tasks on startup; cleanly cancel on shutdown."""
    logger.info("Edge node starting — site_id=%s", config.SITE_ID)

    # ── Shared infrastructure ──────────────────────────────────────────
    redis     = Redis.from_url(config.REDIS_URL, decode_responses=False)
    store     = EdgeStore(config.DB_PATH)
    transport = CloudTransport(store)

    app.state.redis     = redis
    app.state.store     = store
    app.state.transport = transport

    # ── Launch background services ─────────────────────────────────────
    tasks = [
        asyncio.create_task(run_simulator("http://localhost:8000"),   name="simulator"),
        asyncio.create_task(run_detection_consumer(redis),            name="detector"),
        asyncio.create_task(transport.run(),                          name="transport"),
        asyncio.create_task(
            run_forwarder_consumer(redis, store, transport),
            name="forwarder"
        ),
    ]

    logger.info("All edge background services launched")
    yield

    # ── Graceful shutdown ──────────────────────────────────────────────
    logger.info("Shutting down edge node...")
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await redis.aclose()
    store.close()
    logger.info("Edge node stopped")


app = FastAPI(
    title=f"EdgePower Edge Node — {config.SITE_ID}",
    description="Edge telemetry ingestion, anomaly detection, and bandwidth-aware forwarding",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(ingestion_router)
