"""
Forwarder consumer.

Reads classified events from the prioritized-events Redis stream,
applies the forwarding triage policy, persists every eligible event
to SQLite for durability, then enqueues for the transport layer.

CRITICAL  → persist + enqueue immediately
WARNING   → persist + add to batch buffer (flushed every BATCH_INTERVAL seconds)
NORMAL    → persist locally only (never forwarded)
"""
import asyncio
import json
import logging
import time

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from config import STREAM_PRIORITY, GROUP_FORWARDER, CONSUMER_NAME, BATCH_INTERVAL
from forwarder.policy import policy
from forwarder.store import EdgeStore
from forwarder.transport import CloudTransport

logger = logging.getLogger(__name__)


async def _ensure_group(redis: Redis) -> None:
    try:
        await redis.xgroup_create(STREAM_PRIORITY, GROUP_FORWARDER, "$", mkstream=True)
        logger.info("Created consumer group '%s' on '%s'", GROUP_FORWARDER, STREAM_PRIORITY)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def _flush_batch(batch: list[dict], transport: CloudTransport, store: EdgeStore) -> None:
    """Send a batch of WARNING events to the cloud."""
    if batch:
        logger.info("Flushing batch of %d WARNING events", len(batch))
        for event in batch:
            await transport.send(event)
        batch.clear()


async def run_forwarder_consumer(redis: Redis, store: EdgeStore, transport: CloudTransport) -> None:
    """Main forwarder loop. Runs forever as an asyncio background task."""
    await _ensure_group(redis)

    warning_batch: list[dict] = []
    last_batch_flush = time.monotonic()

    logger.info("Forwarder consumer started on stream '%s'", STREAM_PRIORITY)

    while True:
        try:
            messages = await redis.xreadgroup(
                GROUP_FORWARDER,
                CONSUMER_NAME,
                {STREAM_PRIORITY: ">"},
                count=20,
                block=500,
            )

            now = time.monotonic()

            # ── Flush WARNING batch on interval ───────────────────────
            if now - last_batch_flush >= BATCH_INTERVAL:
                await _flush_batch(warning_batch, transport, store)
                last_batch_flush = now

            if not messages:
                continue

            for _stream, events in messages:
                for msg_id, fields in events:
                    try:
                        raw = fields.get(b"data") or fields.get("data", b"")
                        event    = json.loads(raw)
                        severity = event.get("severity", "NORMAL")

                        should_forward, immediate = policy.evaluate(severity)

                        # Always persist (even NORMAL, for local diagnostics)
                        await store.persist(event)

                        if should_forward:
                            if immediate:
                                # CRITICAL — send right away
                                await transport.send(event)
                            else:
                                # WARNING — add to batch
                                warning_batch.append(event)

                        await redis.xack(STREAM_PRIORITY, GROUP_FORWARDER, msg_id)

                    except Exception as exc:
                        logger.error("Forwarder error on %s: %s", msg_id, exc)

        except asyncio.CancelledError:
            # Flush any remaining WARNING batch before shutdown
            await _flush_batch(warning_batch, transport, store)
            logger.info("Forwarder consumer shutting down")
            break
        except Exception as exc:
            logger.error("Forwarder consumer error: %s — retrying in 2s", exc)
            await asyncio.sleep(2)
