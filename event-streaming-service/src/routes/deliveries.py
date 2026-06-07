"""Webhook delivery history routes."""
from __future__ import annotations

import sqlalchemy as sa
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy import text

from config import settings
from src.models import DeliveryResponse

router = APIRouter(prefix="/deliveries", tags=["deliveries"])


def _require_api_key(x_api_key: str = Header(default="")):
    keys = [k.strip() for k in settings.api_keys.split(",") if k.strip()]
    if keys and x_api_key not in keys:
        raise HTTPException(status_code=401, detail="Invalid API key")


_schema = settings.postgres_schema


def _engine() -> sa.Engine:
    return sa.create_engine(settings.postgres_dsn, pool_pre_ping=True)


@router.get("", response_model=list[DeliveryResponse],
            dependencies=[Depends(_require_api_key)])
async def list_deliveries(
    subscription_id: str | None = None,
    status: str | None = None,
    limit: int = Query(20, le=100),
    offset: int = 0,
):
    clauses = ["1=1"]
    params: dict = {"limit": limit, "offset": offset}
    if subscription_id:
        clauses.append("d.subscription_id = :sub_id")
        params["sub_id"] = subscription_id
    if status:
        clauses.append("d.status = :status")
        params["status"] = status

    where = " AND ".join(clauses)
    engine = _engine()
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT d.* FROM "{_schema}".webhook_delivery d
            WHERE {where}
            ORDER BY d.created_at DESC
            LIMIT :limit OFFSET :offset
        """), params).mappings().all()
    return [_row_to_response(r) for r in rows]


def _row_to_response(row) -> DeliveryResponse:
    return DeliveryResponse(
        delivery_id=row["delivery_id"],
        subscription_id=row["subscription_id"],
        event_id=row["event_id"],
        status=row["status"],
        attempt_count=row["attempt_count"],
        last_attempt_at=row.get("last_attempt_at"),
        next_retry_at=row.get("next_retry_at"),
        response_status=row.get("response_status"),
        error_message=row.get("error_message"),
        created_at=row["created_at"],
    )
