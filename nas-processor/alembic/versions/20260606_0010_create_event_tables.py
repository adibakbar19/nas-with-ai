"""Create event_log, webhook_subscription, webhook_delivery tables.

Supports Kafka/Redpanda event streaming and outbound webhook delivery
with retry, HMAC signing, and dead-letter tracking.

Revision ID: 20260606_0010
Revises: 20260606_0009
Create Date: 2026-06-06 00:00:00
"""
from __future__ import annotations

import re

from alembic import op

revision = "20260606_0010"
down_revision = "20260606_0009"
branch_labels = None
depends_on = None

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _schema() -> str:
    schema = op.get_context().config.get_main_option("nas_schema") or "nas"
    if not _IDENT_RE.fullmatch(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return schema


def upgrade() -> None:
    schema = _schema()
    s = f'"{schema}"'

    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {s}.event_log (
            event_id        TEXT PRIMARY KEY,
            event_type      TEXT NOT NULL,
            event_source    TEXT NOT NULL,
            entity_id       TEXT,
            entity_type     TEXT,
            payload         JSONB NOT NULL DEFAULT '{{}}',
            kafka_topic     TEXT,
            kafka_offset    BIGINT,
            published_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            schema_version  TEXT DEFAULT '1.0'
        )
    """)
    op.execute(f"""
        CREATE INDEX IF NOT EXISTS event_log_type_time_idx
        ON {s}.event_log (event_type, published_at DESC)
    """)
    op.execute(f"""
        CREATE INDEX IF NOT EXISTS event_log_entity_idx
        ON {s}.event_log (entity_id, entity_type)
        WHERE entity_id IS NOT NULL
    """)
    op.execute(f"""
        CREATE INDEX IF NOT EXISTS event_log_published_idx
        ON {s}.event_log (published_at DESC)
    """)

    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {s}.webhook_subscription (
            subscription_id  TEXT PRIMARY KEY,
            name             TEXT NOT NULL,
            consumer_system  TEXT NOT NULL,
            url              TEXT NOT NULL,
            event_types      TEXT[] NOT NULL,
            secret           TEXT,
            is_active        BOOLEAN NOT NULL DEFAULT true,
            created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_delivery_at TIMESTAMPTZ,
            failure_count    INTEGER NOT NULL DEFAULT 0
        )
    """)
    op.execute(f"""
        CREATE INDEX IF NOT EXISTS webhook_sub_active_idx
        ON {s}.webhook_subscription (is_active)
    """)

    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {s}.webhook_delivery (
            delivery_id      TEXT PRIMARY KEY,
            subscription_id  TEXT NOT NULL REFERENCES {s}.webhook_subscription(subscription_id),
            event_id         TEXT NOT NULL REFERENCES {s}.event_log(event_id),
            status           TEXT NOT NULL DEFAULT 'pending',
            attempt_count    INTEGER NOT NULL DEFAULT 0,
            last_attempt_at  TIMESTAMPTZ,
            next_retry_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            response_status  INTEGER,
            response_body    TEXT,
            error_message    TEXT,
            created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    op.execute(f"""
        CREATE INDEX IF NOT EXISTS webhook_delivery_status_retry_idx
        ON {s}.webhook_delivery (status, next_retry_at)
        WHERE status IN ('pending', 'retry')
    """)
    op.execute(f"""
        CREATE INDEX IF NOT EXISTS webhook_delivery_sub_time_idx
        ON {s}.webhook_delivery (subscription_id, created_at DESC)
    """)


def downgrade() -> None:
    schema = _schema()
    s = f'"{schema}"'
    op.execute(f"DROP TABLE IF EXISTS {s}.webhook_delivery CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {s}.webhook_subscription CASCADE")
    op.execute(f"DROP TABLE IF EXISTS {s}.event_log CASCADE")
