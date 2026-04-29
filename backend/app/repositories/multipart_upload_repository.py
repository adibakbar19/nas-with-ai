import json
from typing import Any

try:
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional import for local minimal setup
    psycopg = None
    sql = None
    dict_row = None


class MultipartUploadRepository:
    def __init__(self, *, dsn: str, schema: str) -> None:
        self._dsn = dsn
        self._schema = schema

    def _connect(self):
        if psycopg is None or dict_row is None:
            raise RuntimeError("psycopg is required for MultipartUploadRepository")
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def _tbl(self):
        assert sql is not None
        return sql.SQL("{}.{}").format(sql.Identifier(self._schema), sql.Identifier("multipart_upload_session"))

    def ensure_table(self) -> None:
        assert sql is not None
        schema_ident = sql.Identifier(self._schema)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema_ident))
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                      session_id TEXT PRIMARY KEY,
                      agency_id TEXT NULL,
                      status TEXT NOT NULL DEFAULT 'initiated',
                      bucket TEXT NOT NULL,
                      object_name TEXT NOT NULL,
                      upload_id TEXT NOT NULL,
                      file_name TEXT NOT NULL,
                      content_type TEXT NULL,
                      content_bytes BIGINT NOT NULL,
                      part_size INTEGER NOT NULL,
                      job_id TEXT NULL,
                      data JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                ).format(self._tbl())
            )
            cur.execute(
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {} ON {} (status, updated_at DESC)"
                ).format(sql.Identifier("multipart_upload_status_idx"), self._tbl())
            )
            cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS agency_id TEXT NULL").format(self._tbl()))
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (agency_id, status, updated_at DESC)").format(
                    sql.Identifier("multipart_upload_agency_status_idx"),
                    self._tbl(),
                )
            )
            conn.commit()

    @staticmethod
    def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
        data = row.get("data") or {}
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                data = {}
        result = dict(data) if isinstance(data, dict) else {}
        result.update(
            {
                "session_id": row.get("session_id"),
                "agency_id": row.get("agency_id"),
                "status": row.get("status"),
                "bucket": row.get("bucket"),
                "object_name": row.get("object_name"),
                "upload_id": row.get("upload_id"),
                "file_name": row.get("file_name"),
                "content_type": row.get("content_type"),
                "content_bytes": row.get("content_bytes"),
                "part_size": row.get("part_size"),
                "job_id": row.get("job_id"),
            }
        )
        return result

    def create_session(self, *, session: dict[str, Any]) -> None:
        assert sql is not None
        payload = dict(session)
        stmt = sql.SQL(
            """
            INSERT INTO {} (
              session_id, agency_id, status, bucket, object_name, upload_id, file_name,
              content_type, content_bytes, part_size, job_id, data, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
            ON CONFLICT (session_id) DO UPDATE SET
              agency_id = COALESCE(EXCLUDED.agency_id, {}.agency_id),
              status = EXCLUDED.status,
              bucket = EXCLUDED.bucket,
              object_name = EXCLUDED.object_name,
              upload_id = EXCLUDED.upload_id,
              file_name = EXCLUDED.file_name,
              content_type = EXCLUDED.content_type,
              content_bytes = EXCLUDED.content_bytes,
              part_size = EXCLUDED.part_size,
              job_id = EXCLUDED.job_id,
              data = EXCLUDED.data,
              updated_at = NOW()
            """
        ).format(self._tbl(), self._tbl())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                stmt,
                (
                    payload["session_id"],
                    str(payload.get("agency_id") or "").strip() or None,
                    payload.get("status", "initiated"),
                    payload["bucket"],
                    payload["object_name"],
                    payload["upload_id"],
                    payload["file_name"],
                    payload.get("content_type"),
                    int(payload["content_bytes"]),
                    int(payload["part_size"]),
                    payload.get("job_id"),
                    json.dumps(payload, ensure_ascii=True),
                ),
            )
            conn.commit()

    def get_session(self, *, session_id: str, agency_id: str | None = None) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT session_id, agency_id, status, bucket, object_name, upload_id, file_name,
                   content_type, content_bytes, part_size, job_id, data
            FROM {}
            WHERE session_id = %s
            """
        ).format(self._tbl())
        params: list[Any] = [session_id]
        if agency_id:
            stmt += sql.SQL(" AND agency_id = %s")
            params.append(agency_id)
        stmt += sql.SQL(" LIMIT 1")
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, tuple(params))
            row = cur.fetchone()
            return self._normalize_row(row) if row else None

    def update_session(self, *, session_id: str, **changes: Any) -> dict[str, Any] | None:
        current = self.get_session(session_id=session_id)
        if not current:
            return None
        current.update(changes)
        self.create_session(session=current)
        return current
