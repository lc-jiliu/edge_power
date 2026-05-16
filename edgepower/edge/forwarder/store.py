"""
SQLite persistence layer for the edge forwarder.

Every event that should be forwarded is persisted here BEFORE any network
operation, with sent=0. On reconnect, unsent events are replayed in
timestamp order (preserving causal ordering) then marked sent=1.

WAL journal mode allows concurrent reads (replay) while writes continue —
the consumer and transport can operate simultaneously without blocking.

Design decisions:
  - sqlite3 (sync) wrapped in asyncio.to_thread for non-blocking async use
  - PRAGMA synchronous=NORMAL: durability against power loss, ~5ms fsync
  - PRAGMA journal_mode=WAL: concurrent reader during replay
  - Unique constraint on event_id: idempotent inserts
"""
import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT    NOT NULL UNIQUE,
    payload     TEXT    NOT NULL,
    severity    TEXT    NOT NULL,
    event_ts    TEXT    NOT NULL,
    created_at  TEXT    NOT NULL,
    sent        INTEGER NOT NULL DEFAULT 0,
    sent_at     TEXT
);
CREATE INDEX IF NOT EXISTS idx_unsent ON events (sent, severity, event_ts);
"""

INSERT_SQL = """
INSERT OR IGNORE INTO events (event_id, payload, severity, event_ts, created_at)
VALUES (?, ?, ?, ?, ?)
"""

UNSENT_SQL = """
SELECT payload FROM events
WHERE sent = 0 AND severity != 'NORMAL'
ORDER BY event_ts ASC
LIMIT ?
"""

MARK_SENT_SQL = """
UPDATE events SET sent = 1, sent_at = ? WHERE event_id = ?
"""

COUNT_SQL = "SELECT COUNT(*) FROM events WHERE sent = 0 AND severity != 'NORMAL'"


class EdgeStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = asyncio.Lock()

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.executescript("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
            self._conn.executescript(CREATE_SQL)
            self._conn.commit()
            logger.info("SQLite opened: %s", self.db_path)
        return self._conn

    async def persist(self, event: dict) -> None:
        """Write event to SQLite. Always called before any network send."""
        now = datetime.now(timezone.utc).isoformat()
        args = (
            event["event_id"],
            json.dumps(event),
            event.get("severity", "UNKNOWN"),
            event.get("timestamp", now),
            now,
        )
        async with self._lock:
            await asyncio.to_thread(self._sync_insert, args)

    def _sync_insert(self, args: tuple) -> None:
        conn = self._get_conn()
        conn.execute(INSERT_SQL, args)
        conn.commit()

    async def get_unsent(self, limit: int = 500) -> list[dict]:
        """Return unsent non-NORMAL events in ascending timestamp order."""
        async with self._lock:
            rows = await asyncio.to_thread(self._sync_unsent, limit)
        return [json.loads(r[0]) for r in rows]

    def _sync_unsent(self, limit: int) -> list[tuple]:
        conn = self._get_conn()
        return conn.execute(UNSENT_SQL, (limit,)).fetchall()

    async def mark_sent(self, event_id: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        async with self._lock:
            await asyncio.to_thread(self._sync_mark_sent, now, event_id)

    def _sync_mark_sent(self, now: str, event_id: str) -> None:
        conn = self._get_conn()
        conn.execute(MARK_SENT_SQL, (now, event_id))
        conn.commit()

    async def unsent_count(self) -> int:
        async with self._lock:
            return await asyncio.to_thread(self._sync_count)

    def _sync_count(self) -> int:
        conn = self._get_conn()
        return conn.execute(COUNT_SQL).fetchone()[0]

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
