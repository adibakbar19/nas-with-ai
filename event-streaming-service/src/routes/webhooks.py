"""Webhook subscription CRUD routes."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

import sqlalchemy as sa
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy import text

from config import settings
from src.models import WebhookCreate, WebhookResponse, WebhookUpdate

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


def _require_api_key(x_api_key: str = Header(default="")):
    keys = [k.strip() for k in settings.api_keys.split(",") if k.strip()]
    if keys and x_api_key not in keys:
        raise HTTPException(status_code=401, detail="Invalid API key")


def _engine() -> sa.Engine:
    return sa.create_engine(settings.postgres_dsn, pool_pre_ping=True)


_schema = settings.postgres_schema


@router.post("", response_model=WebhookResponse, status_code=201,
             dependencies=[Depends(_require_api_key)])
async def create_webhook(body: WebhookCreate):
    sub_id = uuid.uuid4().hex
    now = datetime.now(timezone.utc)
    engine = _engine()
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO "{_schema}".webhook_subscription
              (subscription_id, name, consumer_system, url,
               event_types, secret, is_active, created_at, updated_at)
            VALUES (:id, :name, :cs, :url, :types, :secret, true, :now, :now)
        """), {
            "id": sub_id, "name": body.name, "cs": body.consumer_system,
            "url": body.url, "types": body.event_types,
            "secret": body.secret, "now": now,
        })
    return _get_sub(sub_id, engine)


@router.get("", response_model=list[WebhookResponse],
            dependencies=[Depends(_require_api_key)])
async def list_webhooks(limit: int = Query(20, le=100), offset: int = 0):
    engine = _engine()
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT * FROM "{_schema}".webhook_subscription
            ORDER BY created_at DESC LIMIT :limit OFFSET :offset
        """), {"limit": limit, "offset": offset}).mappings().all()
    return [_row_to_response(r) for r in rows]


@router.get("/{sub_id}", response_model=WebhookResponse,
            dependencies=[Depends(_require_api_key)])
async def get_webhook(sub_id: str):
    return _get_sub(sub_id, _engine())


@router.patch("/{sub_id}", response_model=WebhookResponse,
              dependencies=[Depends(_require_api_key)])
async def update_webhook(sub_id: str, body: WebhookUpdate):
    engine = _engine()
    updates = body.model_dump(exclude_none=True)
    if not updates:
        return _get_sub(sub_id, engine)
    updates["updated_at"] = datetime.now(timezone.utc)
    set_clause = ", ".join(f'"{k}"=:{k}' for k in updates)
    updates["sub_id"] = sub_id
    with engine.begin() as conn:
        conn.execute(text(f"""
            UPDATE "{_schema}".webhook_subscription
            SET {set_clause} WHERE subscription_id=:sub_id
        """), updates)
    return _get_sub(sub_id, engine)


@router.delete("/{sub_id}", status_code=204,
               dependencies=[Depends(_require_api_key)])
async def delete_webhook(sub_id: str):
    engine = _engine()
    with engine.begin() as conn:
        conn.execute(text(f"""
            UPDATE "{_schema}".webhook_subscription
            SET is_active=false, updated_at=NOW()
            WHERE subscription_id=:id
        """), {"id": sub_id})


def _get_sub(sub_id: str, engine: sa.Engine) -> WebhookResponse:
    with engine.connect() as conn:
        row = conn.execute(text(f"""
            SELECT * FROM "{_schema}".webhook_subscription
            WHERE subscription_id=:id
        """), {"id": sub_id}).mappings().one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="Webhook not found")
    return _row_to_response(row)


def _row_to_response(row) -> WebhookResponse:
    return WebhookResponse(
        subscription_id=row["subscription_id"],
        name=row["name"],
        consumer_system=row["consumer_system"],
        url=row["url"],
        event_types=row["event_types"],
        is_active=row["is_active"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        last_delivery_at=row.get("last_delivery_at"),
        failure_count=row["failure_count"],
    )
