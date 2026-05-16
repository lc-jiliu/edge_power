"""
Sensor simulator — generates realistic industrial telemetry for all equipment.
Runs as a background asyncio task, posting events to the local /events endpoint.

Normal events follow Gaussian distributions around equipment baselines.
Anomaly spikes are injected every ANOMALY_EVERY ticks per equipment item.
"""
import asyncio
import json
import logging
import random
import uuid
from datetime import datetime, timezone

import httpx

from config import SITE_ID, SIM_RATE

logger = logging.getLogger(__name__)

ANOMALY_EVERY = 50  # inject an anomaly spike every N normal events per equipment

# Equipment definitions: id → (sensor_type, unit, normal_mean, normal_std)
EQUIPMENT: dict[str, tuple[str, str, float, float]] = {
    "COMP-C4": ("temperature", "celsius",  85.0,  5.0),
    "PUMP-P2": ("pressure",    "bar",     120.0,  8.0),
    "HEAT-H1": ("temperature", "celsius",  72.0,  4.0),
    "GEN-G3":  ("power",       "kW",      450.0, 20.0),
    "SENS-S5": ("vibration",   "mm/s",      0.05,  0.01),
}

ZONES = {
    "COMP-C4": "sector-2",
    "PUMP-P2": "sector-1",
    "HEAT-H1": "sector-3",
    "GEN-G3":  "sector-1",
    "SENS-S5": "sector-2",
}

ASSET_CLASS = {
    "COMP-C4": "compressor",
    "PUMP-P2": "pump",
    "HEAT-H1": "heat-exchanger",
    "GEN-G3":  "generator",
    "SENS-S5": "vibration-sensor",
}


def _build_event(eq_id: str, value: float) -> dict:
    sensor_type, unit, _, _ = EQUIPMENT[eq_id]
    return {
        "event_id":     str(uuid.uuid4()),
        "site_id":      SITE_ID,
        "equipment_id": eq_id,
        "sensor_type":  sensor_type,
        "value":        round(value, 4),
        "unit":         unit,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "metadata": {
            "zone":        ZONES[eq_id],
            "asset_class": ASSET_CLASS[eq_id],
            "simulated":   True,
        },
    }


async def run_simulator(base_url: str = "http://localhost:8000") -> None:
    """Post simulated sensor events to the local ingestion endpoint."""
    counters: dict[str, int] = {eq: 0 for eq in EQUIPMENT}
    logger.info("Simulator starting — posting to %s/events every %.2fs", base_url, SIM_RATE)

    # Small startup delay so FastAPI is ready
    await asyncio.sleep(2.0)

    async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
        while True:
            for eq_id, (_, _, mean, std) in EQUIPMENT.items():
                counters[eq_id] += 1

                is_anomaly = (counters[eq_id] % ANOMALY_EVERY == 0)
                multiplier = random.uniform(5.0, 10.0) if is_anomaly else 1.0
                value = mean + random.gauss(0, std * multiplier)

                event = _build_event(eq_id, value)

                try:
                    resp = await client.post("/events", json=event)
                    if resp.status_code != 200:
                        logger.warning("Ingest returned %s for %s", resp.status_code, eq_id)
                except Exception as exc:
                    logger.warning("Simulator post failed: %s", exc)

            await asyncio.sleep(SIM_RATE)
