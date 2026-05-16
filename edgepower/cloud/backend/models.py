"""Pydantic schemas for cloud-side data."""
from pydantic import BaseModel
from typing import Optional


class EdgeEvent(BaseModel):
    event_id:           str
    site_id:            str
    equipment_id:       str
    sensor_type:        str
    value:              float
    unit:               str
    timestamp:          str
    metadata:           Optional[dict] = {}
    severity:           str
    anomaly_score:      float
    classified_at:      str
    recommended_action: Optional[str] = None


class AISummary(BaseModel):
    event_id: str
    summary:  str
    action:   str
    model:    str


class NodeStatus(BaseModel):
    site_id:    str
    status:     str   # online | degraded | offline
    last_seen:  Optional[str] = None
    event_rate: float = 0.0
