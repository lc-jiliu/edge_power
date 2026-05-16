"""Cloud backend configuration — all from environment variables."""
import os

DATABASE_URL     = os.getenv("DATABASE_URL", "postgresql://edgepower:edgepower@localhost:5432/edgepower")
LOBSTER_TRAP_URL = os.getenv("LOBSTER_TRAP_URL", "http://localhost:8080")
GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL     = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

HEARTBEAT_YELLOW_S = int(os.getenv("HEARTBEAT_YELLOW_S", "30"))
HEARTBEAT_RED_S    = int(os.getenv("HEARTBEAT_RED_S", "120"))
