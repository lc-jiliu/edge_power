"""FastAPI routes: POST /events, GET /health, GET /stats."""
import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from redis.asyncio import Redis

from config import SITE_ID, STREAM_RAW
from ingestion.models import SensorEvent, IngestResponse

log = APIRouter()
logger = logging.getLogger(__name__)


@log.post("/events", response_model=IngestResponse)
async def ingest_event(event: SensorEvent, request: Request):
    redis: Redis = request.app.state.redis

    # Stamp site_id if not provided
    if not event.site_id:
        event.site_id = SITE_ID

    payload = event.model_dump_json()
    await redis.xadd(STREAM_RAW, {"data": payload}, maxlen=10_000, approximate=True)

    logger.debug("Queued event %s [%s]", event.event_id, event.sensor_type)
    return IngestResponse(status="queued", event_id=event.event_id, site_id=event.site_id)


@log.get("/health")
async def health(request: Request):
    redis: Redis = request.app.state.redis
    try:
        await redis.ping()
        redis_ok = True
    except Exception:
        redis_ok = False

    stream_len = 0
    try:
        stream_len = await redis.xlen(STREAM_RAW)
    except Exception:
        pass

    return {
        "status": "ok" if redis_ok else "degraded",
        "site_id": SITE_ID,
        "redis": redis_ok,
        "raw_stream_len": stream_len,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@log.get("/stats")
async def stats(request: Request):
    redis: Redis = request.app.state.redis
    raw_len      = await redis.xlen(STREAM_RAW)
    priority_len = await redis.xlen("prioritized-events") if await redis.exists("prioritized-events") else 0
    return {
        "site_id": SITE_ID,
        "raw_stream_len": raw_len,
        "priority_stream_len": priority_len,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
