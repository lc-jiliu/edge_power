"""Pydantic models for sensor event ingestion."""
from pydantic import BaseModel, Field
from typing import Optional
import uuid


class SensorEvent(BaseModel):
    event_id:     str = Field(default_factory=lambda: str(uuid.uuid4()))
    site_id:      str
    equipment_id: str
    sensor_type:  str   # temperature | vibration | pressure | power | motion
    value:        float
    unit:         str
    timestamp:    str   # ISO-8601
    metadata:     Optional[dict] = {}


class IngestResponse(BaseModel):
    status:   str
    event_id: str
    site_id:  str
