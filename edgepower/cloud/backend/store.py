"""
PostgreSQL store — asyncpg-based async client.

Idempotency: all inserts use INSERT ... ON CONFLICT DO NOTHING on event_id.
This makes replay from edge nodes completely safe — reconnects can replay
hundreds of events without creating duplicates.
"""
import json
import logging
from typing import Optional

import asyncpg

from backend.models import EdgeEvent, AISummary

logger = logging.getLogger(__name__)

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS edge_events (
    id           SERIAL       PRIMARY KEY,
    event_id     TEXT         NOT NULL UNIQUE,
    site_id      TEXT         NOT NULL,
    equipment_id TEXT         NOT NULL,
    severity     TEXT         NOT NULL,
    sensor_type  TEXT         NOT NULL,
    value        DOUBLE PRECISION NOT NULL,
    unit         TEXT         NOT NULL,
    payload      JSONB        NOT NULL,
    event_ts     TIMESTAMPTZ  NOT NULL,
    received_at  TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_site     ON edge_events (site_id);
CREATE INDEX IF NOT EXISTS idx_events_severity ON edge_events (severity);
CREATE INDEX IF NOT EXISTS idx_events_ts       ON edge_events (event_ts DESC);

CREATE TABLE IF NOT EXISTS ai_summaries (
    id         SERIAL      PRIMARY KEY,
    event_id   TEXT        NOT NULL UNIQUE REFERENCES edge_events(event_id),
    summary    TEXT        NOT NULL,
    action     TEXT        NOT NULL,
    model      TEXT        NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""

UPSERT_EVENT = """
INSERT INTO edge_events
    (event_id, site_id, equipment_id, severity, sensor_type, value, unit, payload, event_ts)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::timestamptz)
ON CONFLICT (event_id) DO NOTHING
"""

UPSERT_SUMMARY = """
INSERT INTO ai_summaries (event_id, summary, action, model)
VALUES ($1, $2, $3, $4)
ON CONFLICT (event_id) DO UPDATE
    SET summary = EXCLUDED.summary,
        action  = EXCLUDED.action
"""

RECENT_EVENTS = """
SELECT payload FROM edge_events
WHERE equipment_id = $1
ORDER BY event_ts DESC
LIMIT $2
"""


class CloudStore:
    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self, dsn: str) -> None:
        self._pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
        async with self._pool.acquire() as conn:
            await conn.execute(CREATE_TABLES)
        logger.info("PostgreSQL connected and schema ready")

    async def upsert_event(self, event: EdgeEvent) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                UPSERT_EVENT,
                event.event_id,
                event.site_id,
                event.equipment_id,
                event.severity,
                event.sensor_type,
                event.value,
                event.unit,
                json.dumps(event.model_dump()),
                event.timestamp,
            )

    async def save_summary(self, summary: AISummary) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                UPSERT_SUMMARY,
                summary.event_id,
                summary.summary,
                summary.action,
                summary.model,
            )

    async def get_recent_events(self, equipment_id: str, limit: int = 5) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(RECENT_EVENTS, equipment_id, limit)
        return [json.loads(r["payload"]) for r in rows]

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
