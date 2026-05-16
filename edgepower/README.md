# EdgePower — Edge-Native Operational Intelligence Platform

Distributed edge AI system for operational monitoring of remote industrial
infrastructure in bandwidth-constrained environments.

## Architecture

```
Site A                              Cloud Hub
─────────────────────────────────   ─────────────────────────────────────
edge-node-1                         cloud-backend
  ├─ Ingestion (FastAPI POST /events)    ├─ WS /edge/{site_id}  ← edge nodes
  ├─ Simulator (10 events/sec)           ├─ WS /dashboard       → browser
  ├─ Detection (IsolationForest)         ├─ GET /               → dashboard HTML
  └─ Forwarder (SQLite + WebSocket)      └─ GET /health
      │                               lobster-trap (AI governance proxy)
redis-1 (local event queue)         postgres (event store + AI summaries)

Site B (identical to Site A)
```

**Data flow:**
1. Simulator → POST /events → Redis Streams (`raw-events`)
2. Detector reads `raw-events` → IsolationForest → `prioritized-events`
3. Forwarder reads `prioritized-events` → SQLite → WebSocket → cloud
4. Cloud → Lobster Trap → Gemini → AI summary → dashboard

## Quick Start

```bash
# 1. Clone and enter the project
cd edgepower

# 2. Set your Gemini API key
cp .env.example .env
# Edit .env: GEMINI_API_KEY=your-key-here

# 3. Start everything
docker-compose up --build

# 4. Open the dashboard
open http://localhost:8000

# 5. Watch the logs
docker-compose logs -f cloud-backend edge-node-1
```

## Demo: Network Partition & Replay

```bash
# Simulate satellite link going down for Site A
docker network disconnect edgepower_cloud-net edgepower_edge-node-1_1

# Watch SQLite accumulating unsent events
docker exec edgepower_edge-node-1_1 \
  sqlite3 /data/site-a.db 'SELECT COUNT(*) FROM events WHERE sent=0 AND severity!="NORMAL"'

# Restore connectivity
docker network connect edgepower_cloud-net edgepower_edge-node-1_1

# Events replay in timestamp order — dashboard shows Site A coming back online
```

## Inject a CRITICAL Event Manually

```bash
curl -X POST http://localhost:8001/events \
  -H "Content-Type: application/json" \
  -d '{
    "site_id":      "site-a",
    "equipment_id": "COMP-C4",
    "sensor_type":  "temperature",
    "value":        142.7,
    "unit":         "celsius",
    "timestamp":    "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
  }'
```

## Key Architectural Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Edge event queue | Redis Streams | Consumer groups + offset tracking at <50MB RAM; Kafka JVM too heavy for edge |
| Local inference | IsolationForest | Sub-1ms CPU inference, explainable, no GPU required |
| Edge persistence | SQLite + WAL | Embedded ACID store; survives power loss; WAL allows concurrent r/w |
| Edge-to-cloud transport | WebSocket | Own reconnect + replay logic; gRPC fails under intermittent satellite links |
| AI governance | Lobster Trap (Veea) | Policy enforcement independent of app code; auditable; OpenAI-compatible |

## Forwarding Policy

| Severity | Behaviour |
|---|---|
| CRITICAL | Persist to SQLite + forward immediately |
| WARNING | Persist to SQLite + batch forward every 60s |
| NORMAL | Persist to SQLite locally only (never forwarded) |

~97% bandwidth reduction vs raw streaming.

## Environment Variables

### Edge Node
| Variable | Default | Description |
|---|---|---|
| `SITE_ID` | `site-a` | Unique site identifier |
| `REDIS_URL` | `redis://redis-1:6379` | Local Redis URL |
| `CLOUD_WS_URL` | `ws://cloud-backend:8000/edge/site-a` | Cloud WebSocket |
| `DB_PATH` | `/data/edge.db` | SQLite file path |
| `SIM_RATE` | `0.1` | Seconds between simulator ticks |
| `BATCH_INTERVAL` | `60` | Seconds between WARNING batch flushes |

### Cloud Backend
| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql://...` | PostgreSQL connection string |
| `LOBSTER_TRAP_URL` | `http://lobster-trap:8080` | Lobster Trap proxy |
| `GEMINI_API_KEY` | — | Required for AI diagnostics |
| `GEMINI_MODEL` | `gemini-2.0-flash` | Gemini model name |

## Project Structure

```
edgepower/
  edge/
    app.py              # Entry point: FastAPI + background task launcher
    config.py           # All config via env vars
    ingestion/          # Component 1: POST /events, sensor simulator
    detection/          # Component 2: IsolationForest classifier + consumer
    forwarder/          # Component 3: SQLite store + triage + WebSocket transport
  cloud/
    backend/            # Component 4: Cloud aggregation + Lobster Trap + Gemini
    dashboard/          # Component 5: Real-time operational dashboard (HTML)
  lobster-trap/         # AI governance proxy config
  docker-compose.yml
  .env.example
```
