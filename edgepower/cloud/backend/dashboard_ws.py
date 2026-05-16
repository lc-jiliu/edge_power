"""
Dashboard WebSocket broadcast manager.

Maintains a set of connected dashboard clients and broadcasts
event notifications + AI summaries to all of them in real time.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import WebSocket

from backend.models import EdgeEvent, AISummary, NodeStatus

logger = logging.getLogger(__name__)


class DashboardManager:
    def __init__(self):
        self._clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._clients.add(ws)
        logger.info("Dashboard client connected (%d total)", len(self._clients))

    def disconnect(self, ws: WebSocket) -> None:
        self._clients.discard(ws)
        logger.info("Dashboard client disconnected (%d remaining)", len(self._clients))

    async def broadcast(self, msg: dict) -> None:
        """Send a JSON message to all connected dashboard clients."""
        if not self._clients:
            return
        data = json.dumps(msg)
        dead = set()
        for ws in list(self._clients):
            try:
                await ws.send_text(data)
            except Exception:
                dead.add(ws)
        for ws in dead:
            self._clients.discard(ws)

    async def broadcast_event(self, event: EdgeEvent) -> None:
        await self.broadcast({
            "type": "event",
            "data": {
                "event_id":     event.event_id,
                "site_id":      event.site_id,
                "equipment_id": event.equipment_id,
                "severity":     event.severity,
                "sensor_type":  event.sensor_type,
                "value":        event.value,
                "unit":         event.unit,
                "anomaly_score": event.anomaly_score,
                "timestamp":    event.timestamp,
                "recommended_action": event.recommended_action,
            },
        })

    async def broadcast_summary(self, summary: AISummary) -> None:
        await self.broadcast({
            "type": "summary",
            "data": {
                "event_id": summary.event_id,
                "summary":  summary.summary,
                "action":   summary.action,
                "model":    summary.model,
            },
        })

    async def broadcast_node_status(self, status: NodeStatus) -> None:
        await self.broadcast({
            "type": "node_status",
            "data": {
                "site_id":    status.site_id,
                "status":     status.status,
                "last_seen":  status.last_seen,
                "event_rate": status.event_rate,
            },
        })
