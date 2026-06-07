"""Dead letter queue repository."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy import text


class DlqRepository:
    def __init__(self, engine: sa.Engine, schema: str = "ingest") -> None:
        self._engine = engine
        self._tbl = f'"{schema}"."dead_letter_job"'

    def move_to_dlq(
        self,
        job_id: str,
        job_type: str,
        source_system: str | None,
        original_data: dict,
        failure_reason: str,
        retry_count: int,
    ) -> dict[str, Any]:
        dlq_id = uuid.uuid4().hex
        with self._engine.begin() as conn:
            row = conn.execute(text(f"""
                INSERT INTO {self._tbl}
                  (dlq_id, job_id, job_type, source_system,
                   original_data, failure_reason, retry_count, moved_at)
                VALUES
                  (:dlq_id, :job_id, :job_type, :source_system,
                   CAST(:data AS jsonb), :reason, :retries, NOW())
                RETURNING *
            """), {
                "dlq_id":       dlq_id,
                "job_id":       job_id,
                "job_type":     job_type,
                "source_system": source_system,
                "data":         json.dumps(original_data),
                "reason":       failure_reason[:1000] if failure_reason else "",
                "retries":      retry_count,
            }).fetchone()
        return dict(row._mapping) if row else {}

    def list_dlq(self, limit: int = 20, offset: int = 0) -> tuple[list[dict], int]:
        with self._engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT *, COUNT(*) OVER() AS _total
                FROM {self._tbl}
                ORDER BY moved_at DESC
                LIMIT :limit OFFSET :offset
            """), {"limit": limit, "offset": offset}).fetchall()
        total = int(rows[0]._mapping["_total"]) if rows else 0
        result = []
        for row in rows:
            d = dict(row._mapping)
            d.pop("_total", None)
            for k, v in list(d.items()):
                if isinstance(v, datetime):
                    d[k] = v.isoformat()
            result.append(d)
        return result, total

    def get_dlq_entry(self, dlq_id: str) -> dict | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                text(f"SELECT * FROM {self._tbl} WHERE dlq_id = :id"),
                {"id": dlq_id},
            ).fetchone()
        if row is None:
            return None
        d = dict(row._mapping)
        for k, v in list(d.items()):
            if isinstance(v, datetime):
                d[k] = v.isoformat()
        return d

    def mark_requeued(self, dlq_id: str, new_job_id: str) -> bool:
        with self._engine.begin() as conn:
            result = conn.execute(text(f"""
                UPDATE {self._tbl}
                SET requeued_job_id = :new_job_id,
                    resolution = 'requeued'
                WHERE dlq_id = :id
            """), {"new_job_id": new_job_id, "id": dlq_id})
        return result.rowcount > 0
