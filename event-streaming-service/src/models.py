"""Pydantic schemas for event-streaming-service."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, HttpUrl


class WebhookCreate(BaseModel):
    name: str
    consumer_system: str
    url: str
    event_types: list[str]
    secret: str | None = None


class WebhookUpdate(BaseModel):
    name: str | None = None
    url: str | None = None
    event_types: list[str] | None = None
    secret: str | None = None
    is_active: bool | None = None


class WebhookResponse(BaseModel):
    subscription_id: str
    name: str
    consumer_system: str
    url: str
    event_types: list[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime
    last_delivery_at: datetime | None = None
    failure_count: int


class EventResponse(BaseModel):
    event_id: str
    event_type: str
    event_source: str
    entity_id: str | None = None
    entity_type: str | None = None
    payload: dict[str, Any]
    kafka_topic: str | None = None
    published_at: datetime
    schema_version: str | None = None


class DeliveryResponse(BaseModel):
    delivery_id: str
    subscription_id: str
    event_id: str
    status: str
    attempt_count: int
    last_attempt_at: datetime | None = None
    next_retry_at: datetime | None = None
    response_status: int | None = None
    error_message: str | None = None
    created_at: datetime
