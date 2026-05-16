"""
EdgePower Cloud Backend — FastAPI Application

Endpoints:
  WS  /edge/{site_id}   — Receives classified events from edge nodes
  WS  /dashboard        — Real-time broadcast to dashboard clients
  GET /                 — Serves the operational dashboard HTML
  GET /health           — Health check
  GET /nodes            — Node status summary
  GET /events           — Recent event list (REST fallback)
"""
import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from backend.config import DATABASE_URL, HEARTBEAT_YELLOW_S, HEARTBEAT_RED_S
from backend.dashboard_ws import DashboardManager
from backend.intelligence import analyze_event
from backend.models import EdgeEvent, NodeStatus
from backend.store import CloudStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("cloud")

# ── Shared state ──────────────────────────────────────────────────────────────
store     = CloudStore()
dashboard = DashboardManager()

# Node registry: site_id → last seen timestamp + recent event count
_node_registry: dict[str, dict] = {}
_event_counts:  dict[str, int]  = {}


def _update_node(site_id: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    _node_registry[site_id] = {"last_seen": now}
    _event_counts[site_id]  = _event_counts.get(site_id, 0) + 1


def _node_status(site_id: str) -> str:
    info = _node_registry.get(site_id)
    if not info:
        return "offline"
    last = datetime.fromisoformat(info["last_seen"])
    age  = (datetime.now(timezone.utc) - last).total_seconds()
    if age < HEARTBEAT_YELLOW_S:
        return "online"
    if age < HEARTBEAT_RED_S:
        return "degraded"
    return "offline"


async def _heartbeat_loop() -> None:
    """Broadcast node status to dashboard every 10 seconds."""
    while True:
        await asyncio.sleep(10)
        for site_id, info in _node_registry.items():
            status = NodeStatus(
                site_id=site_id,
                status=_node_status(site_id),
                last_seen=info.get("last_seen"),
                event_rate=float(_event_counts.get(site_id, 0)),
            )
            await dashboard.broadcast_node_status(status)
        # Reset per-interval counters
        for site_id in _event_counts:
            _event_counts[site_id] = 0


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Cloud backend starting...")
    await store.connect(DATABASE_URL)
    heartbeat_task = asyncio.create_task(_heartbeat_loop())
    yield
    heartbeat_task.cancel()
    await asyncio.gather(heartbeat_task, return_exceptions=True)
    await store.close()
    logger.info("Cloud backend stopped")


app = FastAPI(
    title="EdgePower Cloud Backend",
    description="Aggregation, AI governance, and real-time operational intelligence",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Edge WebSocket receiver ───────────────────────────────────────────────────

@app.websocket("/edge/{site_id}")
async def edge_receiver(ws: WebSocket, site_id: str):
    """Receives classified events from an edge node."""
    await ws.accept()
    logger.info("Edge node connected: %s", site_id)

    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)

            try:
                event = EdgeEvent(**data)
            except Exception as exc:
                logger.warning("Bad event payload from %s: %s", site_id, exc)
                continue

            _update_node(site_id)

            # Idempotent upsert — safe to replay
            await store.upsert_event(event)

            # Real-time broadcast to dashboard clients
            await dashboard.broadcast_event(event)

            # Trigger AI analysis for CRITICAL events (non-blocking)
            if event.severity == "CRITICAL":
                recent = await store.get_recent_events(event.equipment_id, limit=5)
                asyncio.create_task(
                    analyze_event(event, recent, store, dashboard)
                )

    except WebSocketDisconnect:
        logger.info("Edge node disconnected: %s", site_id)
    except Exception as exc:
        logger.error("Edge receiver error for %s: %s", site_id, exc)


# ── Dashboard WebSocket ───────────────────────────────────────────────────────

@app.websocket("/dashboard")
async def dashboard_ws(ws: WebSocket):
    """Real-time broadcast to dashboard browser clients."""
    await dashboard.connect(ws)

    # Send current node states on connect
    for site_id, info in _node_registry.items():
        status = NodeStatus(
            site_id=site_id,
            status=_node_status(site_id),
            last_seen=info.get("last_seen"),
        )
        await dashboard.broadcast_node_status(status)

    try:
        while True:
            await ws.receive_text()   # keep connection alive (ping from client)
    except WebSocketDisconnect:
        dashboard.disconnect(ws)


# ── REST endpoints ────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    """Serve the operational dashboard."""
    html_path = Path(__file__).parent.parent / "dashboard" / "index.html"
    if html_path.exists():
        return HTMLResponse(html_path.read_text())
    return HTMLResponse("<h1>Dashboard not found</h1>", status_code=404)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "nodes": {
            site_id: _node_status(site_id)
            for site_id in _node_registry
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/nodes")
async def get_nodes():
    return {
        site_id: {
            "status":    _node_status(site_id),
            "last_seen": info.get("last_seen"),
        }
        for site_id, info in _node_registry.items()
    }
