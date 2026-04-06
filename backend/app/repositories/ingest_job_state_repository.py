import json
from datetime import datetime, timedelta, timezone
from typing import Any

try:
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional import for local minimal setup
    psycopg = None
    sql = None
    dict_row = None


class IngestJobStateRepository:
    def __init__(self, *, dsn: str, schema: str) -> None:
        self._dsn = dsn
        self._schema = schema

    def _connect(self):
        if psycopg is None or dict_row is None:
            raise RuntimeError("psycopg is required for IngestJobStateRepository")
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def _tbl(self):
        assert sql is not None
        return sql.SQL("{}.{}").format(sql.Identifier(self._schema), sql.Identifier("ingest_job"))

    @staticmethod
    def _parse_timestamp(raw: Any) -> datetime | None:
        if not raw:
            return None
        if isinstance(raw, datetime):
            return raw
        try:
            return datetime.fromisoformat(str(raw))
        except Exception:
            return None

    @staticmethod
    def _normalize_data(row: dict[str, Any]) -> dict[str, Any]:
        data = row.get("data") or {}
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                data = {}
        result = dict(data) if isinstance(data, dict) else {}
        result["job_id"] = str(row.get("job_id") or result.get("job_id") or "")
        if row.get("status") and not result.get("status"):
            result["status"] = row["status"]
        created_at = row.get("created_at")
        if created_at and not result.get("created_at"):
            try:
                result["created_at"] = created_at.isoformat()
            except Exception:
                result["created_at"] = str(created_at)
        updated_at = row.get("updated_at")
        if updated_at and not result.get("updated_at"):
            try:
                result["updated_at"] = updated_at.isoformat()
            except Exception:
                result["updated_at"] = str(updated_at)
        return result

    def ensure_table(self) -> None:
        assert sql is not None
        schema_ident = sql.Identifier(self._schema)
        table_ident = sql.Identifier("ingest_job")
        status_idx = sql.Identifier("ingest_job_status_idx")
        created_idx = sql.Identifier("ingest_job_created_at_idx")
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema_ident))
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                      job_id TEXT PRIMARY KEY,
                      status TEXT NOT NULL DEFAULT 'unknown',
                      created_at TIMESTAMPTZ NULL,
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      data JSONB NOT NULL DEFAULT '{{}}'::jsonb
                    )
                    """
                ).format(self._tbl())
            )
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (status)").format(status_idx, self._tbl())
            )
            cur.execute(
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {} ON {} (created_at DESC NULLS LAST, updated_at DESC)"
                ).format(created_idx, self._tbl())
            )
            conn.commit()

    def get_job(self, *, job_id: str) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL("SELECT job_id, status, created_at, updated_at, data FROM {} WHERE job_id = %s LIMIT 1").format(
            self._tbl()
        )
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (job_id,))
            row = cur.fetchone()
            return self._normalize_data(row) if row else None

    def list_jobs(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        assert sql is not None
        limit_sql = sql.SQL(" LIMIT %s") if limit else sql.SQL("")
        stmt = sql.SQL(
            "SELECT job_id, status, created_at, updated_at, data FROM {} ORDER BY created_at DESC NULLS LAST, updated_at DESC"
        ).format(self._tbl()) + limit_sql
        with self._connect() as conn, conn.cursor() as cur:
            if limit:
                cur.execute(stmt, (limit,))
            else:
                cur.execute(stmt)
            rows = cur.fetchall()
            return [self._normalize_data(row) for row in rows]

    def save_job(self, *, job_id: str, state: dict[str, Any]) -> None:
        assert sql is not None
        payload = dict(state)
        payload["job_id"] = job_id
        status = str(payload.get("status") or "").strip() or "unknown"
        created_at = self._parse_timestamp(payload.get("created_at"))
        stmt = sql.SQL(
            """
            INSERT INTO {} (job_id, status, created_at, updated_at, data)
            VALUES (%s, %s, %s, NOW(), %s::jsonb)
            ON CONFLICT (job_id) DO UPDATE SET
              status = EXCLUDED.status,
              created_at = COALESCE({tbl}.created_at, EXCLUDED.created_at),
              updated_at = NOW(),
              data = EXCLUDED.data
            """
        ).format(self._tbl(), tbl=self._tbl())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (job_id, status, created_at, json.dumps(payload, ensure_ascii=True)))
            conn.commit()

    def claim_job(
        self,
        *,
        job_id: str,
        worker_id: str,
        expected_statuses: tuple[str, ...] = ("queued",),
        queue_event_id: str | None = None,
        queue_message_id: str | None = None,
    ) -> dict[str, Any] | None:
        assert sql is not None
        select_stmt = sql.SQL(
            "SELECT job_id, status, created_at, updated_at, data FROM {} WHERE job_id = %s FOR UPDATE"
        ).format(self._tbl())
        update_stmt = sql.SQL(
            """
            UPDATE {}
            SET status = %s,
                created_at = COALESCE(created_at, %s),
                updated_at = NOW(),
                data = %s::jsonb
            WHERE job_id = %s
            RETURNING job_id, status, created_at, updated_at, data
            """
        ).format(self._tbl())

        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(select_stmt, (job_id,))
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return None

            current = self._normalize_data(row)
            current_status = str(row.get("status") or current.get("status") or "").strip().lower()
            allowed = {status.strip().lower() for status in expected_statuses}
            if current_status not in allowed:
                conn.rollback()
                return None

            claimed_at = datetime.utcnow().isoformat() + "Z"
            current.update(
                {
                    "job_id": job_id,
                    "status": "running",
                    "claimed_by": worker_id,
                    "claimed_at": claimed_at,
                    "queue_event_id": queue_event_id,
                    "queue_message_id": queue_message_id,
                }
            )
            if not current.get("started_at"):
                current["started_at"] = claimed_at

            created_at = self._parse_timestamp(current.get("created_at")) or row.get("created_at")
            cur.execute(
                update_stmt,
                (
                    "running",
                    created_at,
                    json.dumps(current, ensure_ascii=True),
                    job_id,
                ),
            )
            updated = cur.fetchone()
            conn.commit()
            return self._normalize_data(updated) if updated else None

    def requeue_stale_claims(
        self,
        *,
        stale_after_seconds: int,
        statuses: tuple[str, ...] = ("running", "pausing"),
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        assert sql is not None
        if stale_after_seconds <= 0 or limit <= 0:
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=stale_after_seconds)
        select_stmt = sql.SQL(
            """
            SELECT job_id, status, created_at, updated_at, data
            FROM {}
            WHERE status = ANY(%s)
              AND updated_at < %s
            ORDER BY updated_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT %s
            """
        ).format(self._tbl())
        update_stmt = sql.SQL(
            """
            UPDATE {}
            SET status = %s,
                updated_at = NOW(),
                data = %s::jsonb
            WHERE job_id = %s
            RETURNING job_id, status, created_at, updated_at, data
            """
        ).format(self._tbl())

        normalized_statuses = [str(item).strip().lower() for item in statuses if str(item).strip()]
        recovered: list[dict[str, Any]] = []
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(select_stmt, (normalized_statuses, cutoff, limit))
            rows = cur.fetchall()
            for row in rows:
                current = self._normalize_data(row)
                recovery_count = int(current.get("recovery_count") or 0) + 1
                current.update(
                    {
                        "job_id": current.get("job_id") or row.get("job_id"),
                        "status": "queued",
                        "progress_stage": "queued",
                        "pause_requested": False,
                        "claimed_by": None,
                        "claimed_at": None,
                        "recovered_from_stale_claim_at": datetime.now(timezone.utc).isoformat(),
                        "recovery_count": recovery_count,
                        "last_log_line": "Recovered stale worker claim and re-queued",
                    }
                )
                if current.get("load_to_db"):
                    current["load_status"] = "pending"
                cur.execute(
                    update_stmt,
                    (
                        "queued",
                        json.dumps(current, ensure_ascii=True),
                        str(row["job_id"]),
                    ),
                )
                updated = cur.fetchone()
                if updated:
                    recovered.append(self._normalize_data(updated))
            conn.commit()
        return recovered
