"""Add queue-service columns to ingest_job; create dead_letter_job table.

Extends the existing ingest.ingest_job table with columns needed for
queue-service ownership: retry tracking, heartbeat, priority, worker identity.
All new columns have safe defaults so existing rows are unaffected.

Also creates ingest.dead_letter_job for permanently-failed jobs.

Revision ID: 20260607_0012
Revises: 20260606_0010
Create Date: 2026-06-07 00:00:00
"""
from __future__ import annotations

from alembic import op

revision = "20260607_0012"
down_revision = "20260606_0010"
branch_labels = None
depends_on = None

_INGEST = "ingest"


def upgrade() -> None:
    # ── Extend ingest_job ─────────────────────────────────────────────────────
    op.execute(f"""
        ALTER TABLE {_INGEST}.ingest_job
          ADD COLUMN IF NOT EXISTS job_type TEXT NOT NULL DEFAULT 'bulk_ingest',
          ADD COLUMN IF NOT EXISTS queue_name TEXT NOT NULL DEFAULT 'bulk_ingest_events',
          ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 5,
          ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0,
          ADD COLUMN IF NOT EXISTS max_retries INTEGER NOT NULL DEFAULT 3,
          ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ,
          ADD COLUMN IF NOT EXISTS claimed_by TEXT,
          ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ,
          ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ,
          ADD COLUMN IF NOT EXISTS worker_id TEXT,
          ADD COLUMN IF NOT EXISTS source_system TEXT DEFAULT 'ingestion-api',
          ADD COLUMN IF NOT EXISTS error_detail TEXT,
          ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ
    """)

    # Index for claim_next_job: queued/retry jobs ordered by priority then age
    op.execute(f"""
        CREATE INDEX IF NOT EXISTS ingest_job_queue_idx
        ON {_INGEST}.ingest_job (status, priority DESC, created_at ASC)
        WHERE status IN ('queued', 'pending')
    """)

    # Index for retry scheduler: jobs due for retry
    op.execute(f"""
        CREATE INDEX IF NOT EXISTS ingest_job_retry_idx
        ON {_INGEST}.ingest_job (next_retry_at)
        WHERE status = 'retry' AND next_retry_at IS NOT NULL
    """)

    # Index for heartbeat monitor: running jobs by last heartbeat
    op.execute(f"""
        CREATE INDEX IF NOT EXISTS ingest_job_heartbeat_idx
        ON {_INGEST}.ingest_job (heartbeat_at)
        WHERE status = 'running'
    """)

    # ── Backfill typed columns from existing JSONB data ──────────────────────
    # Existing rows have all state packed into the `data` JSONB blob.
    # Promote key fields to typed columns so queue-service can query them
    # efficiently. Uses COALESCE so rows already having column values are
    # not overwritten.
    op.execute(f"""
        UPDATE {_INGEST}.ingest_job SET
            job_type = COALESCE(
                NULLIF(data->>'job_type', ''), job_type, 'bulk_ingest'
            ),
            source_system = COALESCE(
                NULLIF(data->>'source_system', ''), source_system, 'ingestion-api'
            ),
            claimed_by = COALESCE(
                claimed_by, NULLIF(data->>'claimed_by', '')
            ),
            heartbeat_at = COALESCE(
                heartbeat_at,
                CASE
                    WHEN data->>'claimed_at' IS NOT NULL
                         AND data->>'claimed_at' <> ''
                    THEN (data->>'claimed_at')::timestamptz
                    ELSE NULL
                END
            ),
            retry_count = COALESCE(
                CASE
                    WHEN data->>'retry_count' ~ '^[0-9]+$'
                    THEN (data->>'retry_count')::integer
                    ELSE NULL
                END,
                retry_count,
                0
            )
        WHERE true
    """)

    # ── Dead letter queue ─────────────────────────────────────────────────────
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS {_INGEST}.dead_letter_job (
            dlq_id          TEXT PRIMARY KEY,
            job_id          TEXT NOT NULL,
            job_type        TEXT NOT NULL,
            source_system   TEXT,
            original_data   JSONB NOT NULL DEFAULT '{{}}',
            failure_reason  TEXT,
            retry_count     INTEGER NOT NULL DEFAULT 0,
            moved_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            reviewed_at     TIMESTAMPTZ,
            reviewed_by     TEXT,
            resolution      TEXT,
            requeued_job_id TEXT
        )
    """)
    op.execute(f"""
        CREATE INDEX IF NOT EXISTS dlq_job_id_idx
        ON {_INGEST}.dead_letter_job (job_id)
    """)
    op.execute(f"""
        CREATE INDEX IF NOT EXISTS dlq_moved_at_idx
        ON {_INGEST}.dead_letter_job (moved_at DESC)
    """)


def downgrade() -> None:
    op.execute(f"DROP TABLE IF EXISTS {_INGEST}.dead_letter_job")
    op.execute(f"DROP INDEX IF EXISTS {_INGEST}.ingest_job_heartbeat_idx")
    op.execute(f"DROP INDEX IF EXISTS {_INGEST}.ingest_job_retry_idx")
    op.execute(f"DROP INDEX IF EXISTS {_INGEST}.ingest_job_queue_idx")
    for col in [
        "completed_at", "error_detail", "source_system", "worker_id",
        "heartbeat_at", "claimed_at", "claimed_by", "next_retry_at",
        "max_retries", "retry_count", "priority", "queue_name", "job_type",
    ]:
        op.execute(f"ALTER TABLE {_INGEST}.ingest_job DROP COLUMN IF EXISTS {col}")
