import json
from datetime import datetime
from typing import Any

try:
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional import for local minimal setup
    psycopg = None
    sql = None
    dict_row = None


class AgencyApiKeyRepository:
    def __init__(self, *, dsn: str, schema: str) -> None:
        self._dsn = dsn
        self._schema = schema

    def _connect(self):
        if psycopg is None or dict_row is None:
            raise RuntimeError("psycopg is required for AgencyApiKeyRepository")
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def _tbl(self):
        assert sql is not None
        return sql.SQL("{}.{}").format(sql.Identifier(self._schema), sql.Identifier("agency_api_key"))

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
    def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
        metadata = row.get("metadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
        result = dict(metadata) if isinstance(metadata, dict) else {}
        for key in (
            "key_id",
            "agency_id",
            "label",
            "secret_preview",
            "status",
            "created_by",
            "revoked_reason",
        ):
            result[key] = row.get(key)
        for key in ("created_at", "updated_at", "last_used_at", "expires_at", "revoked_at"):
            raw_value = row.get(key)
            if raw_value is None:
                result[key] = None
            else:
                try:
                    result[key] = raw_value.isoformat()
                except Exception:
                    result[key] = str(raw_value)
        result["_secret_hash"] = row.get("secret_hash")
        return result

    def ensure_table(self) -> None:
        assert sql is not None
        schema_ident = sql.Identifier(self._schema)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema_ident))
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                      key_id TEXT PRIMARY KEY,
                      agency_id TEXT NOT NULL,
                      label TEXT NULL,
                      secret_hash TEXT NOT NULL,
                      secret_preview TEXT NOT NULL,
                      status TEXT NOT NULL DEFAULT 'active',
                      created_by TEXT NULL,
                      revoked_reason TEXT NULL,
                      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      last_used_at TIMESTAMPTZ NULL,
                      expires_at TIMESTAMPTZ NULL,
                      revoked_at TIMESTAMPTZ NULL
                    )
                    """
                ).format(self._tbl())
            )
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (agency_id, status, created_at DESC)").format(
                    sql.Identifier("agency_api_key_agency_status_created_idx"),
                    self._tbl(),
                )
            )
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (status, expires_at)").format(
                    sql.Identifier("agency_api_key_status_expires_idx"),
                    self._tbl(),
                )
            )
            conn.commit()

    def create_key(self, *, record: dict[str, Any]) -> dict[str, Any]:
        assert sql is not None
        payload = dict(record)
        metadata = dict(payload.get("metadata") or {})
        expires_at = self._parse_timestamp(payload.get("expires_at"))
        stmt = sql.SQL(
            """
            INSERT INTO {} (
              key_id, agency_id, label, secret_hash, secret_preview, status,
              created_by, revoked_reason, metadata, created_at, updated_at,
              last_used_at, expires_at, revoked_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW(), NULL, %s, NULL)
            RETURNING key_id, agency_id, label, secret_hash, secret_preview, status,
                      created_by, revoked_reason, metadata, created_at, updated_at,
                      last_used_at, expires_at, revoked_at
            """
        ).format(self._tbl())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                stmt,
                (
                    payload["key_id"],
                    payload["agency_id"],
                    payload.get("label"),
                    payload["secret_hash"],
                    payload["secret_preview"],
                    payload.get("status", "active"),
                    payload.get("created_by"),
                    payload.get("revoked_reason"),
                    json.dumps(metadata, ensure_ascii=True),
                    expires_at,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            if not row:
                raise RuntimeError("failed to insert agency api key")
            return self._normalize_row(row)

    def get_key(self, *, key_id: str) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT key_id, agency_id, label, secret_hash, secret_preview, status,
                   created_by, revoked_reason, metadata, created_at, updated_at,
                   last_used_at, expires_at, revoked_at
            FROM {}
            WHERE key_id = %s
            LIMIT 1
            """
        ).format(self._tbl())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (key_id,))
            row = cur.fetchone()
            return self._normalize_row(row) if row else None

    def list_keys(self, *, agency_id: str) -> list[dict[str, Any]]:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT key_id, agency_id, label, secret_hash, secret_preview, status,
                   created_by, revoked_reason, metadata, created_at, updated_at,
                   last_used_at, expires_at, revoked_at
            FROM {}
            WHERE agency_id = %s
            ORDER BY created_at DESC, key_id DESC
            """
        ).format(self._tbl())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (agency_id,))
            rows = cur.fetchall()
            return [self._normalize_row(row) for row in rows]

    def revoke_key(self, *, key_id: str, revoked_reason: str | None = None) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            UPDATE {}
            SET status = 'revoked',
                revoked_reason = %s,
                revoked_at = NOW(),
                updated_at = NOW()
            WHERE key_id = %s
              AND status <> 'revoked'
            RETURNING key_id, agency_id, label, secret_hash, secret_preview, status,
                      created_by, revoked_reason, metadata, created_at, updated_at,
                      last_used_at, expires_at, revoked_at
            """
        ).format(self._tbl())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (revoked_reason, key_id))
            row = cur.fetchone()
            conn.commit()
            return self._normalize_row(row) if row else None

    def touch_last_used(self, *, key_id: str) -> None:
        assert sql is not None
        stmt = sql.SQL("UPDATE {} SET last_used_at = NOW(), updated_at = NOW() WHERE key_id = %s").format(self._tbl())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (key_id,))
            conn.commit()
