"""All DB operations on ingest.ingest_job.

Design note: ingest_job stores its detailed state (progress_pct, log lines,
file paths, etc.) as a JSONB blob in the `data` column — a document-store
pattern inherited from the original implementation. The new typed columns
(heartbeat_at, claimed_by, retry_count, etc.) are stored at the top level
for fast indexed queries while the full state dict lives in `data`.

update_job() always merges provided kwargs into `data` as well as writing
the appropriate typed columns, so the existing consumer.py and ingestion-api
response format remain intact.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy import text

logger = logging.getLogger(__name__)

# Fields that map to top-level columns in ingest_job (not just data blob)
_TOP_LEVEL_COLS = frozenset({
    "status", "agency_id", "job_type", "queue_name", "priority",
    "retry_count", "max_retries", "next_retry_at", "claimed_by",
    "claimed_at", "heartbeat_at", "worker_id", "source_system",
    "error_detail", "completed_at",
})

BACKOFF_SECONDS = [60, 300, 900]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


class JobRepository:
    def __init__(self, engine: sa.Engine, schema: str = "ingest") -> None:
        self._engine = engine
        self._schema = schema
        self._tbl = f'"{schema}"."ingest_job"'
        self._dlq = f'"{schema}"."dead_letter_job"'

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _row_to_dict(self, row) -> dict[str, Any]:
        """Convert a DB row mapping to the flat job dict that callers expect.

        The data JSONB blob is merged with the top-level columns so existing
        consumers (ingestion-api response, worker get_job) see a unified dict.
        """
        if row is None:
            return {}
        d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
        blob = d.pop("data", {}) or {}
        if isinstance(blob, str):
            try:
                blob = json.loads(blob)
            except Exception:
                blob = {}
        # Top-level typed columns win over anything in the blob
        merged = dict(blob)
        for k, v in d.items():
            if v is not None or k not in merged:
                merged[k] = v
        # Serialize datetimes
        for k, v in list(merged.items()):
            if isinstance(v, datetime):
                merged[k] = v.isoformat()
        return merged

    def _build_data_blob(self, existing_blob: dict, updates: dict) -> dict:
        """Merge updates into the existing blob, return updated blob."""
        result = dict(existing_blob or {})
        result.update(updates)
        return result

    # ── Core CRUD ─────────────────────────────────────────────────────────────

    def create_job(
        self,
        job_id: str,
        job_type: str,
        source_system: str,
        data: dict,
        priority: int = 5,
        queue_name: str = "bulk_ingest_events",
        agency_id: str | None = None,
    ) -> dict[str, Any]:
        now = _now()
        payload = dict(data)
        payload.setdefault("job_id", job_id)
        payload.setdefault("status", "pending")
        payload.setdefault("created_at", now.isoformat())

        with self._engine.begin() as conn:
            row = conn.execute(text(f"""
                INSERT INTO {self._tbl}
                  (job_id, agency_id, status, created_at, updated_at, data,
                   job_type, queue_name, priority, source_system,
                   retry_count, max_retries)
                VALUES
                  (:job_id, :agency_id, 'pending', :created_at, :created_at,
                   CAST(:data AS jsonb), :job_type, :queue_name, :priority,
                   :source_system, 0, 3)
                ON CONFLICT (job_id) DO NOTHING
                RETURNING *
            """), {
                "job_id":        job_id,
                "agency_id":     agency_id or payload.get("agency_id"),
                "created_at":    now,
                "data":          json.dumps(payload),
                "job_type":      job_type,
                "queue_name":    queue_name,
                "priority":      priority,
                "source_system": source_system,
            }).fetchone()
        if row is None:
            return self.get_job(job_id) or {}
        return self._row_to_dict(row)

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                text(f"SELECT * FROM {self._tbl} WHERE job_id = :id"),
                {"id": job_id},
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def list_jobs(
        self,
        status: str | None = None,
        source_system: str | None = None,
        job_type: str | None = None,
        agency_id: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        clauses = ["1=1"]
        params: dict = {"limit": limit, "offset": offset}
        if status:
            clauses.append("status = :status")
            params["status"] = status
        if source_system:
            clauses.append("source_system = :source_system")
            params["source_system"] = source_system
        if job_type:
            clauses.append("job_type = :job_type")
            params["job_type"] = job_type
        if agency_id:
            clauses.append("agency_id = :agency_id")
            params["agency_id"] = agency_id
        where = " AND ".join(clauses)

        with self._engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT *, COUNT(*) OVER() AS _total
                FROM {self._tbl}
                WHERE {where}
                ORDER BY created_at DESC NULLS LAST
                LIMIT :limit OFFSET :offset
            """), params).fetchall()

        total = int(rows[0]._mapping["_total"]) if rows else 0
        result = []
        for row in rows:
            d = self._row_to_dict(row)
            d.pop("_total", None)
            result.append(d)
        return result, total

    def update_job(self, job_id: str, **fields: Any) -> dict[str, Any] | None:
        """Update ingest_job. Fields go into typed columns AND into data blob."""
        if not fields:
            return self.get_job(job_id)

        # Fetch current blob
        with self._engine.connect() as conn:
            cur = conn.execute(
                text(f"SELECT data FROM {self._tbl} WHERE job_id = :id"),
                {"id": job_id},
            ).fetchone()
        if cur is None:
            return None

        existing_blob = cur._mapping.get("data") or {}
        if isinstance(existing_blob, str):
            try:
                existing_blob = json.loads(existing_blob)
            except Exception:
                existing_blob = {}

        new_blob = self._build_data_blob(existing_blob, fields)

        # Build SET clause for typed top-level columns
        set_parts = ["updated_at = NOW()", "data = CAST(:_data AS jsonb)"]
        params: dict[str, Any] = {"_data": json.dumps(new_blob), "_id": job_id}

        for col in _TOP_LEVEL_COLS:
            if col in fields:
                set_parts.append(f'"{col}" = :{col}')
                params[col] = fields[col]

        set_clause = ", ".join(set_parts)
        with self._engine.begin() as conn:
            row = conn.execute(text(f"""
                UPDATE {self._tbl}
                SET {set_clause}
                WHERE job_id = :_id
                RETURNING *
            """), params).fetchone()
        return self._row_to_dict(row) if row else None

    # ── Job lifecycle ─────────────────────────────────────────────────────────

    def mark_queued(self, job_id: str, stream_msg_id: str | None = None) -> dict[str, Any] | None:
        """Set status=queued after stream push succeeds."""
        updates: dict[str, Any] = {"status": "queued", "stream_msg_id": stream_msg_id}
        return self.update_job(job_id, **updates)

    def claim_next_job(
        self,
        worker_id: str,
        queue_name: str,
        job_types: list[str],
    ) -> dict[str, Any] | None:
        """Atomically claim the next available job. FOR UPDATE SKIP LOCKED."""
        now = _now()
        with self._engine.begin() as conn:
            row = conn.execute(text(f"""
                UPDATE {self._tbl}
                SET status = 'claimed',
                    claimed_by = :worker_id,
                    claimed_at = :now,
                    heartbeat_at = :now,
                    worker_id = :worker_id,
                    updated_at = :now
                WHERE job_id = (
                    SELECT job_id FROM {self._tbl}
                    WHERE status IN ('queued', 'retry')
                      AND queue_name = :queue_name
                      AND (next_retry_at IS NULL OR next_retry_at <= :now)
                      AND job_type = ANY(:job_types)
                    ORDER BY priority DESC, created_at ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING *
            """), {
                "worker_id": worker_id,
                "now":       now,
                "queue_name": queue_name,
                "job_types":  job_types,
            }).fetchone()
        return self._row_to_dict(row) if row else None

    def heartbeat(self, job_id: str, worker_id: str) -> bool:
        with self._engine.begin() as conn:
            result = conn.execute(text(f"""
                UPDATE {self._tbl}
                SET heartbeat_at = NOW(), updated_at = NOW()
                WHERE job_id = :id AND claimed_by = :worker_id
            """), {"id": job_id, "worker_id": worker_id})
        return result.rowcount > 0

    def complete_job(
        self,
        job_id: str,
        worker_id: str,
        result_data: dict,
    ) -> dict[str, Any] | None:
        # Fetch and merge current blob
        with self._engine.connect() as conn:
            cur = conn.execute(
                text(f"SELECT data FROM {self._tbl} WHERE job_id = :id"),
                {"id": job_id},
            ).fetchone()
        if cur is None:
            return None
        existing_blob = cur._mapping.get("data") or {}
        if isinstance(existing_blob, str):
            try:
                existing_blob = json.loads(existing_blob)
            except Exception:
                existing_blob = {}
        new_blob = self._build_data_blob(existing_blob, {**result_data, "status": "completed"})

        with self._engine.begin() as conn:
            row = conn.execute(text(f"""
                UPDATE {self._tbl}
                SET status = 'completed',
                    completed_at = NOW(),
                    updated_at = NOW(),
                    data = CAST(:data AS jsonb)
                WHERE job_id = :id AND claimed_by = :worker_id
                RETURNING *
            """), {
                "data":      json.dumps(new_blob),
                "id":        job_id,
                "worker_id": worker_id,
            }).fetchone()
        return self._row_to_dict(row) if row else None

    def fail_job(
        self,
        job_id: str,
        worker_id: str,
        error: str,
        retry: bool = True,
        backoff_seconds: list[int] | None = None,
    ) -> dict[str, Any] | None:
        backoff = backoff_seconds or BACKOFF_SECONDS

        with self._engine.connect() as conn:
            cur = conn.execute(
                text(f"SELECT data, retry_count, max_retries FROM {self._tbl} WHERE job_id = :id"),
                {"id": job_id},
            ).fetchone()
        if cur is None:
            return None

        row_map = cur._mapping
        retry_count = int(row_map.get("retry_count") or 0) + 1
        max_retries = int(row_map.get("max_retries") or 3)

        existing_blob = row_map.get("data") or {}
        if isinstance(existing_blob, str):
            try:
                existing_blob = json.loads(existing_blob)
            except Exception:
                existing_blob = {}

        if retry and retry_count <= max_retries:
            new_status = "retry"
            secs = backoff[min(retry_count - 1, len(backoff) - 1)]
            new_blob = self._build_data_blob(existing_blob, {
                "status": "retry", "error": error, "retry_count": retry_count,
            })
            with self._engine.begin() as conn:
                row = conn.execute(text(f"""
                    UPDATE {self._tbl}
                    SET status = 'retry',
                        retry_count = :retry_count,
                        next_retry_at = NOW() + :secs * INTERVAL '1 second',
                        error_detail = :error,
                        claimed_by = NULL,
                        updated_at = NOW(),
                        data = CAST(:data AS jsonb)
                    WHERE job_id = :id
                    RETURNING *
                """), {
                    "retry_count": retry_count,
                    "secs":        secs,
                    "error":       error[:500],
                    "data":        json.dumps(new_blob),
                    "id":          job_id,
                }).fetchone()
        else:
            new_blob = self._build_data_blob(existing_blob, {
                "status": "failed", "error": error, "retry_count": retry_count,
            })
            with self._engine.begin() as conn:
                row = conn.execute(text(f"""
                    UPDATE {self._tbl}
                    SET status = 'failed',
                        retry_count = :retry_count,
                        error_detail = :error,
                        completed_at = NOW(),
                        updated_at = NOW(),
                        data = CAST(:data AS jsonb)
                    WHERE job_id = :id
                    RETURNING *
                """), {
                    "retry_count": retry_count,
                    "error":       error[:500],
                    "data":        json.dumps(new_blob),
                    "id":          job_id,
                }).fetchone()
        return self._row_to_dict(row) if row else None

    def find_stale_jobs(self, timeout_seconds: int) -> list[dict[str, Any]]:
        with self._engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT * FROM {self._tbl}
                WHERE status = 'running'
                  AND (
                    heartbeat_at IS NULL
                    OR heartbeat_at < NOW() - :timeout * INTERVAL '1 second'
                  )
                  AND updated_at < NOW() - :timeout * INTERVAL '1 second'
            """), {"timeout": timeout_seconds}).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def find_stuck_queued_jobs(self, stuck_after_seconds: int = 300) -> list[dict[str, Any]]:
        """Find queued jobs whose stream message was consumed but worker never claimed them."""
        with self._engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT * FROM {self._tbl}
                WHERE status = 'queued'
                  AND updated_at < NOW() - :timeout * INTERVAL '1 second'
            """), {"timeout": stuck_after_seconds}).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def requeue_job(self, job_id: str) -> dict[str, Any] | None:
        """Reset a stale or failed job back to queued for re-processing."""
        with self._engine.begin() as conn:
            row = conn.execute(text(f"""
                UPDATE {self._tbl}
                SET status = 'queued',
                    claimed_by = NULL,
                    claimed_at = NULL,
                    heartbeat_at = NULL,
                    next_retry_at = NULL,
                    updated_at = NOW()
                WHERE job_id = :id
                RETURNING *
            """), {"id": job_id}).fetchone()
        return self._row_to_dict(row) if row else None

    # ── Queue stats ───────────────────────────────────────────────────────────

    def get_stats(self) -> dict[str, Any]:
        with self._engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT status, COUNT(*) as cnt
                FROM {self._tbl}
                GROUP BY status
            """)).fetchall()
            oldest = conn.execute(text(f"""
                SELECT EXTRACT(EPOCH FROM (NOW() - MIN(created_at))) as age
                FROM {self._tbl}
                WHERE status IN ('queued', 'pending')
            """)).fetchone()
            avg_proc = conn.execute(text(f"""
                SELECT AVG(
                    EXTRACT(EPOCH FROM (completed_at - created_at))
                ) as avg_secs
                FROM {self._tbl}
                WHERE status = 'completed'
                  AND completed_at IS NOT NULL
                  AND created_at IS NOT NULL
            """)).fetchone()
            dlq_count = conn.execute(text(f"""
                SELECT COUNT(*) FROM {self._dlq}
            """)).fetchone()

        counts = {r._mapping["status"]: int(r._mapping["cnt"]) for r in rows}
        return {
            "queued":                  counts.get("queued", 0),
            "pending":                 counts.get("pending", 0),
            "running":                 counts.get("running", 0),
            "completed":               counts.get("completed", 0),
            "failed":                  counts.get("failed", 0),
            "retry":                   counts.get("retry", 0),
            "dead_letter":             int(dlq_count._mapping["count"]) if dlq_count else 0,
            "oldest_queued_age_seconds": float(oldest._mapping["age"] or 0) if oldest and oldest._mapping["age"] else None,
            "avg_processing_seconds":  float(avg_proc._mapping["avg_secs"] or 0) if avg_proc and avg_proc._mapping["avg_secs"] else None,
        }
