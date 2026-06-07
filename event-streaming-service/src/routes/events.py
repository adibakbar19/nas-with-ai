"""Event log query routes."""
from __future__ import annotations

import sqlalchemy as sa
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy import text

from config import settings
from src.models import EventResponse

router = APIRouter(prefix="/events", tags=["events"])


def _require_api_key(x_api_key: str = Header(default="")):
    keys = [k.strip() for k in settings.api_keys.split(",") if k.strip()]
    if keys and x_api_key not in keys:
        raise HTTPException(status_code=401, detail="Invalid API key")


_schema = settings.postgres_schema


def _engine() -> sa.Engine:
    return sa.create_engine(settings.postgres_dsn, pool_pre_ping=True)


@router.get("", response_model=list[EventResponse],
            dependencies=[Depends(_require_api_key)])
async def list_events(
    event_type: str | None = None,
    entity_id: str | None = None,
    entity_type: str | None = None,
    from_time: str | None = None,
    to_time: str | None = None,
    limit: int = Query(20, le=100),
    offset: int = 0,
):
    clauses = ["1=1"]
    params: dict = {"limit": limit, "offset": offset}
    if event_type:
        clauses.append("event_type = :event_type")
        params["event_type"] = event_type
    if entity_id:
        clauses.append("entity_id = :entity_id")
        params["entity_id"] = entity_id
    if entity_type:
        clauses.append("entity_type = :entity_type")
        params["entity_type"] = entity_type
    if from_time:
        clauses.append("published_at >= :from_time")
        params["from_time"] = from_time
    if to_time:
        clauses.append("published_at <= :to_time")
        params["to_time"] = to_time

    where = " AND ".join(clauses)
    engine = _engine()
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT * FROM "{_schema}".event_log
            WHERE {where}
            ORDER BY published_at DESC
            LIMIT :limit OFFSET :offset
        """), params).mappings().all()
    return [_row_to_response(r) for r in rows]


@router.get("/{event_id}", response_model=EventResponse,
            dependencies=[Depends(_require_api_key)])
async def get_event(event_id: str):
    engine = _engine()
    with engine.connect() as conn:
        row = conn.execute(text(f"""
            SELECT * FROM "{_schema}".event_log WHERE event_id=:id
        """), {"id": event_id}).mappings().one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return _row_to_response(row)


def _row_to_response(row) -> EventResponse:
    return EventResponse(
        event_id=row["event_id"],
        event_type=row["event_type"],
        event_source=row["event_source"],
        entity_id=row.get("entity_id"),
        entity_type=row.get("entity_type"),
        payload=row["payload"] if isinstance(row["payload"], dict) else {},
        kafka_topic=row.get("kafka_topic"),
        published_at=row["published_at"],
        schema_version=row.get("schema_version"),
    )
