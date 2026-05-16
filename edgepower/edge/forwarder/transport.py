"""
WebSocket transport — edge-to-cloud event delivery.

On each (re)connect:
  1. Replay all unsent events from SQLite in timestamp order.
  2. Switch to live forwarding from the in-memory send queue.

Reconnect uses exponential backoff, capped at 60 seconds.
The transport is fully async; it never blocks the event loop.

Why WebSocket over gRPC:
  gRPC streaming is synchronous RPC — it fails poorly under intermittent
  satellite links. WebSocket with client-managed reconnect lets us own the
  retry and ordering logic explicitly in ~30 lines of asyncio Python.
"""
import asyncio
import json
import logging

import websockets
from websockets.exceptions import ConnectionClosed

from config import CLOUD_WS_URL, SITE_ID
from forwarder.store import EdgeStore

logger = logging.getLogger(__name__)

REPLAY_BATCH  = 500    # max events replayed per reconnect
MIN_BACKOFF   = 1.0    # seconds
MAX_BACKOFF   = 60.0   # seconds


class CloudTransport:
    def __init__(self, store: EdgeStore):
        self.store       = store
        self._ws         = None
        self._send_queue: asyncio.Queue = asyncio.Queue()
        self._connected  = asyncio.Event()

    async def send(self, event: dict) -> None:
        """Enqueue an event for sending. Non-blocking; used by consumer."""
        await self._send_queue.put(event)

    async def run(self) -> None:
        """Main transport loop — runs forever, reconnecting on failure."""
        backoff = MIN_BACKOFF

        while True:
            try:
                logger.info("Connecting to cloud: %s", CLOUD_WS_URL)
                async with websockets.connect(
                    CLOUD_WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    open_timeout=10,
                ) as ws:
                    self._ws = ws
                    self._connected.set()
                    backoff = MIN_BACKOFF   # reset on successful connect
                    logger.info("Connected to cloud backend")

                    # Step 1: replay any unsent events from SQLite
                    await self._replay_unsent(ws)

                    # Step 2: live forwarding from the send queue
                    await self._live_loop(ws)

            except asyncio.CancelledError:
                logger.info("Transport shutting down")
                break
            except Exception as exc:
                self._connected.clear()
                self._ws = None
                logger.warning(
                    "Cloud connection lost (%s). Reconnecting in %.0fs...", exc, backoff
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)

    async def _replay_unsent(self, ws) -> None:
        """Send all persisted-but-unsent events in causal timestamp order."""
        unsent = await self.store.get_unsent(limit=REPLAY_BATCH)
        if unsent:
            logger.info("Replaying %d unsent events from SQLite...", len(unsent))
        for event in unsent:
            await ws.send(json.dumps(event))
            await self.store.mark_sent(event["event_id"])
        if unsent:
            logger.info("Replay complete")

    async def _live_loop(self, ws) -> None:
        """Forward live events from the in-memory send queue."""
        while True:
            try:
                # Wait up to 1s for a queued event; keep-alive otherwise
                event = await asyncio.wait_for(self._send_queue.get(), timeout=1.0)
                await ws.send(json.dumps(event))
                await self.store.mark_sent(event["event_id"])
            except asyncio.TimeoutError:
                # No events — check connection is still alive
                try:
                    await ws.ping()
                except ConnectionClosed:
                    raise

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and not self._ws.closed
