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


class ApiIdempotencyRepository:
    def __init__(self, *, dsn: str, schema: str) -> None:
        self._dsn = dsn
        self._schema = schema

    def _connect(self):
        if psycopg is None or dict_row is None:
            raise RuntimeError("psycopg is required for ApiIdempotencyRepository")
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def _tbl(self):
        assert sql is not None
        return sql.SQL("{}.{}").format(sql.Identifier(self._schema), sql.Identifier("api_idempotency_request"))

    @staticmethod
    def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
        response = row.get("response")
        if isinstance(response, str):
            try:
                response = json.loads(response)
            except json.JSONDecodeError:
                response = None
        return {
            "agency_id": row.get("agency_id"),
            "operation": row.get("operation"),
            "idempotency_key": row.get("idempotency_key"),
            "request_fingerprint": row.get("request_fingerprint"),
            "status": row.get("status"),
            "resource_type": row.get("resource_type"),
            "resource_id": row.get("resource_id"),
            "response": response if isinstance(response, dict) else None,
        }

    def ensure_table(self) -> None:
        assert sql is not None
        schema_ident = sql.Identifier(self._schema)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema_ident))
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                      agency_id TEXT NOT NULL,
                      operation TEXT NOT NULL,
                      idempotency_key TEXT NOT NULL,
                      request_fingerprint TEXT NOT NULL,
                      status TEXT NOT NULL DEFAULT 'pending',
                      resource_type TEXT NULL,
                      resource_id TEXT NULL,
                      response JSONB NULL,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      PRIMARY KEY (agency_id, operation, idempotency_key)
                    )
                    """
                ).format(self._tbl())
            )
            cur.execute(
                sql.SQL(
                    "CREATE INDEX IF NOT EXISTS {} ON {} (agency_id, updated_at DESC)"
                ).format(sql.Identifier("api_idempotency_agency_updated_idx"), self._tbl())
            )
            conn.commit()

    def get_record(self, *, agency_id: str, operation: str, idempotency_key: str) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT agency_id, operation, idempotency_key, request_fingerprint,
                   status, resource_type, resource_id, response
            FROM {}
            WHERE agency_id = %s AND operation = %s AND idempotency_key = %s
            LIMIT 1
            """
        ).format(self._tbl())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (agency_id, operation, idempotency_key))
            row = cur.fetchone()
            return self._normalize_row(row) if row else None

    def claim_request(
        self,
        *,
        agency_id: str,
        operation: str,
        idempotency_key: str,
        request_fingerprint: str,
    ) -> tuple[bool, dict[str, Any]]:
        assert sql is not None
        insert_stmt = sql.SQL(
            """
            INSERT INTO {} (
              agency_id, operation, idempotency_key, request_fingerprint,
              status, resource_type, resource_id, response, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, 'pending', NULL, NULL, NULL, NOW(), NOW())
            ON CONFLICT (agency_id, operation, idempotency_key) DO NOTHING
            RETURNING agency_id, operation, idempotency_key, request_fingerprint,
                      status, resource_type, resource_id, response
            """
        ).format(self._tbl())
        select_stmt = sql.SQL(
            """
            SELECT agency_id, operation, idempotency_key, request_fingerprint,
                   status, resource_type, resource_id, response
            FROM {}
            WHERE agency_id = %s AND operation = %s AND idempotency_key = %s
            LIMIT 1
            """
        ).format(self._tbl())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(insert_stmt, (agency_id, operation, idempotency_key, request_fingerprint))
            row = cur.fetchone()
            if row:
                conn.commit()
                return True, self._normalize_row(row)
            cur.execute(select_stmt, (agency_id, operation, idempotency_key))
            existing = cur.fetchone()
            conn.commit()
            if not existing:
                raise RuntimeError("failed to claim idempotency request")
            return False, self._normalize_row(existing)

    def complete_request(
        self,
        *,
        agency_id: str,
        operation: str,
        idempotency_key: str,
        resource_type: str,
        resource_id: str,
        response: dict[str, Any],
    ) -> None:
        assert sql is not None
        stmt = sql.SQL(
            """
            UPDATE {}
            SET status = 'completed',
                resource_type = %s,
                resource_id = %s,
                response = %s::jsonb,
                updated_at = NOW()
            WHERE agency_id = %s AND operation = %s AND idempotency_key = %s
            """
        ).format(self._tbl())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                stmt,
                (
                    resource_type,
                    resource_id,
                    json.dumps(response, ensure_ascii=True),
                    agency_id,
                    operation,
                    idempotency_key,
                ),
            )
            conn.commit()

    def delete_request(self, *, agency_id: str, operation: str, idempotency_key: str) -> None:
        assert sql is not None
        stmt = sql.SQL("DELETE FROM {} WHERE agency_id = %s AND operation = %s AND idempotency_key = %s").format(
            self._tbl()
        )
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (agency_id, operation, idempotency_key))
            conn.commit()
