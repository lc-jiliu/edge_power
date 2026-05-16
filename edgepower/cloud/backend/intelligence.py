"""
AI intelligence layer — Lobster Trap + Gemini.

For each CRITICAL event:
  1. Build a structured diagnostic prompt with event context + recent history.
  2. POST to Lobster Trap (inline proxy) — it inspects the prompt against
     security policies and forwards clean prompts to Gemini.
  3. Parse Gemini's response into structured summary + recommended action.
  4. Persist the summary and broadcast to the dashboard.

Lobster Trap enforces:
  - No credential leakage in prompts
  - No PII in prompts
  - No prompt injection attempts
  - Structured audit log of every interaction

Why Lobster Trap over app-level sanitisation:
  It's a policy enforcement layer independent of application code,
  producing an auditable trail. The security team can verify every
  prompt that left the system without trusting developer discipline.
"""
import asyncio
import json
import logging
import re
from typing import Optional

import httpx

from backend.config import LOBSTER_TRAP_URL, GEMINI_API_KEY, GEMINI_MODEL
from backend.models import EdgeEvent, AISummary

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are an industrial equipment diagnostic assistant.
You analyse sensor anomaly data from remote industrial sites and provide
concise operational assessments. Always respond in this exact JSON format:
{
  "summary": "one or two sentence diagnosis",
  "likely_cause": "most probable root cause",
  "urgency": "immediate | within_24h | within_week",
  "recommended_action": "specific action to take",
  "confidence": "high | medium | low"
}
Be factual and concise. No preamble."""


def _build_prompt(event: EdgeEvent, recent_history: list[dict]) -> str:
    history_lines = []
    for h in recent_history[:5]:
        history_lines.append(
            f"  - {h.get('timestamp','?')[:19]}  {h.get('equipment_id','?')}  "
            f"{h.get('sensor_type','?')}={h.get('value','?')} {h.get('unit','')}  "
            f"[{h.get('severity','?')}]"
        )
    history_str = "\n".join(history_lines) if history_lines else "  (no prior history)"

    return f"""Industrial equipment anomaly detected — requires diagnosis.

SITE:        {event.site_id}
EQUIPMENT:   {event.equipment_id}
SENSOR:      {event.sensor_type}
VALUE:       {event.value} {event.unit}
SEVERITY:    {event.severity}
SCORE:       {event.anomaly_score:.4f}  (CRITICAL threshold: 0.50)
ZONE:        {event.metadata.get('zone', 'unknown')}
ASSET CLASS: {event.metadata.get('asset_class', 'unknown')}
TIMESTAMP:   {event.timestamp}

RECENT HISTORY FOR THIS EQUIPMENT (last 5 readings):
{history_str}

Provide a structured diagnostic assessment in the specified JSON format."""


async def analyze_event(
    event: EdgeEvent,
    recent_history: list[dict],
    store,
    dashboard,
) -> Optional[AISummary]:
    """
    Run Gemini diagnosis via Lobster Trap for a CRITICAL event.
    Returns an AISummary or None if analysis fails.
    """
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY not set — skipping AI analysis")
        return None

    prompt = _build_prompt(event, recent_history)

    payload = {
        "model": GEMINI_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens":  512,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{LOBSTER_TRAP_URL}/v1/chat/completions",
                json=payload,
                headers={"Authorization": f"Bearer {GEMINI_API_KEY}"},
            )
            resp.raise_for_status()

        content = resp.json()["choices"][0]["message"]["content"]
        result  = _parse_response(content)

        summary = AISummary(
            event_id=event.event_id,
            summary=result.get("summary", content[:200]),
            action=result.get("recommended_action", result.get("urgency", "inspect")),
            model=GEMINI_MODEL,
        )

        await store.save_summary(summary)
        await dashboard.broadcast_summary(summary)

        logger.info(
            "AI summary for %s: %s [urgency=%s]",
            event.equipment_id,
            summary.summary[:80],
            result.get("urgency", "?"),
        )
        return summary

    except Exception as exc:
        logger.error("AI analysis failed for event %s: %s", event.event_id, exc)
        return None


def _parse_response(content: str) -> dict:
    """Extract JSON from model response, tolerating markdown fences."""
    # Strip markdown fences if present
    cleaned = re.sub(r"```(?:json)?\s*", "", content).strip().rstrip("`").strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        # Best-effort: return content as summary
        return {"summary": content[:300], "recommended_action": "manual_review"}
