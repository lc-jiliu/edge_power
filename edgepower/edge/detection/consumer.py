"""
Anomaly detection consumer.

Reads raw sensor events from Redis Streams (raw-events) via a consumer group,
classifies each with IsolationForest, enriches the payload, and writes the
result to the prioritized-events stream for the forwarder.

Consumer group semantics ensure at-least-once delivery:
- Unacknowledged messages are redelivered after a crash/restart.
- XACK is only called after the enriched event is successfully written.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from config import (
    STREAM_RAW, STREAM_PRIORITY,
    GROUP_DETECTOR, CONSUMER_NAME,
)
from detection.classifier import AnomalyClassifier

logger = logging.getLogger(__name__)


async def _ensure_group(redis: Redis) -> None:
    """Create the consumer group — idempotent (silently ignores if already exists)."""
    try:
        await redis.xgroup_create(STREAM_RAW, GROUP_DETECTOR, "$", mkstream=True)
        logger.info("Created consumer group '%s' on stream '%s'", GROUP_DETECTOR, STREAM_RAW)
    except ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            pass  # already exists — normal on restart
        else:
            raise


async def run_detection_consumer(redis: Redis) -> None:
    """
    Main detection loop. Runs forever as an asyncio background task.
    Reads from raw-events, classifies, writes to prioritized-events.
    """
    await _ensure_group(redis)
    classifier = AnomalyClassifier()
    logger.info("Detection consumer started on stream '%s'", STREAM_RAW)

    while True:
        try:
            # Block up to 500ms waiting for new messages
            messages = await redis.xreadgroup(
                GROUP_DETECTOR,
                CONSUMER_NAME,
                {STREAM_RAW: ">"},   # ">" = only undelivered messages
                count=20,
                block=500,
            )

            if not messages:
                continue

            for _stream, events in messages:
                for msg_id, fields in events:
                    try:
                        raw = fields.get(b"data") or fields.get("data", b"")
                        event = json.loads(raw)

                        severity, score = classifier.classify(event)

                        enriched = {
                            **event,
                            "severity":           severity,
                            "anomaly_score":      round(score, 4),
                            "classified_at":      datetime.now(timezone.utc).isoformat(),
                            "recommended_action": _recommend(severity, event),
                        }

                        # Write to priority stream — forwarder reads from here
                        await redis.xadd(
                            STREAM_PRIORITY,
                            {"data": json.dumps(enriched)},
                            maxlen=5_000,
                            approximate=True,
                        )

                        # Acknowledge — message will not be redelivered
                        await redis.xack(STREAM_RAW, GROUP_DETECTOR, msg_id)

                        if severity != "NORMAL":
                            logger.info(
                                "[%s] %s → %s (score=%.3f)",
                                event.get("site_id", "?"),
                                event.get("equipment_id", "?"),
                                severity,
                                score,
                            )

                    except Exception as exc:
                        logger.error("Failed to process event %s: %s", msg_id, exc)
                        # Do NOT xack — message will be redelivered

        except asyncio.CancelledError:
            logger.info("Detection consumer shutting down")
            break
        except Exception as exc:
            logger.error("Detection consumer error: %s — retrying in 2s", exc)
            await asyncio.sleep(2)


def _recommend(severity: str, event: dict) -> str:
    if severity == "NORMAL":
        return "none"
    sensor = event.get("sensor_type", "unknown")
    eq     = event.get("equipment_id", "unknown")
    if severity == "CRITICAL":
        return f"immediate_inspection:{eq}:{sensor}"
    return f"schedule_inspection:{eq}:{sensor}"
