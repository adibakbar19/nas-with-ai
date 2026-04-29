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


class AgencyUserRepository:
    def __init__(self, *, dsn: str, schema: str) -> None:
        self._dsn = dsn
        self._schema = schema

    def _connect(self):
        if psycopg is None or dict_row is None:
            raise RuntimeError("psycopg is required for AgencyUserRepository")
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def _tbl(self):
        assert sql is not None
        return sql.SQL("{}.{}").format(sql.Identifier(self._schema), sql.Identifier("agency_user"))

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
            "user_id",
            "agency_id",
            "username",
            "display_name",
            "email",
            "role",
            "status",
            "created_by",
            "disabled_reason",
            "password_algo",
        ):
            result[key] = row.get(key)
        for key in ("created_at", "updated_at", "last_login_at", "disabled_at"):
            raw_value = row.get(key)
            if raw_value is None:
                result[key] = None
            else:
                try:
                    result[key] = raw_value.isoformat()
                except Exception:
                    result[key] = str(raw_value)
        result["_password_hash"] = row.get("password_hash")
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
                      user_id TEXT PRIMARY KEY,
                      agency_id TEXT NOT NULL,
                      username TEXT NOT NULL UNIQUE,
                      display_name TEXT NULL,
                      email TEXT NULL,
                      password_hash TEXT NOT NULL,
                      password_algo TEXT NOT NULL DEFAULT 'pbkdf2_sha256',
                      role TEXT NOT NULL,
                      status TEXT NOT NULL DEFAULT 'active',
                      created_by TEXT NULL,
                      disabled_reason TEXT NULL,
                      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      last_login_at TIMESTAMPTZ NULL,
                      disabled_at TIMESTAMPTZ NULL
                    )
                    """
                ).format(self._tbl())
            )
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (agency_id, role, status, created_at DESC)").format(
                    sql.Identifier("agency_user_agency_role_status_created_idx"),
                    self._tbl(),
                )
            )
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (status, username)").format(
                    sql.Identifier("agency_user_status_username_idx"),
                    self._tbl(),
                )
            )
            conn.commit()

    def create_user(self, *, record: dict[str, Any]) -> dict[str, Any]:
        assert sql is not None
        payload = dict(record)
        metadata = dict(payload.get("metadata") or {})
        stmt = sql.SQL(
            """
            INSERT INTO {} (
              user_id, agency_id, username, display_name, email, password_hash,
              password_algo, role, status, created_by, disabled_reason, metadata,
              created_at, updated_at, last_login_at, disabled_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW(), NULL, NULL)
            RETURNING user_id, agency_id, username, display_name, email, password_hash,
                      password_algo, role, status, created_by, disabled_reason, metadata,
                      created_at, updated_at, last_login_at, disabled_at
            """
        ).format(self._tbl())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                stmt,
                (
                    payload["user_id"],
                    payload["agency_id"],
                    payload["username"],
                    payload.get("display_name"),
                    payload.get("email"),
                    payload["password_hash"],
                    payload.get("password_algo", "pbkdf2_sha256"),
                    payload["role"],
                    payload.get("status", "active"),
                    payload.get("created_by"),
                    payload.get("disabled_reason"),
                    json.dumps(metadata, ensure_ascii=True),
                ),
            )
            row = cur.fetchone()
            conn.commit()
            if not row:
                raise RuntimeError("failed to insert agency user")
            return self._normalize_row(row)

    def get_user_by_username(self, *, username: str) -> dict[str, Any] | None:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT user_id, agency_id, username, display_name, email, password_hash,
                   password_algo, role, status, created_by, disabled_reason, metadata,
                   created_at, updated_at, last_login_at, disabled_at
            FROM {}
            WHERE lower(username) = lower(%s)
            LIMIT 1
            """
        ).format(self._tbl())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (username,))
            row = cur.fetchone()
            return self._normalize_row(row) if row else None

    def list_users(self, *, agency_id: str) -> list[dict[str, Any]]:
        assert sql is not None
        stmt = sql.SQL(
            """
            SELECT user_id, agency_id, username, display_name, email, password_hash,
                   password_algo, role, status, created_by, disabled_reason, metadata,
                   created_at, updated_at, last_login_at, disabled_at
            FROM {}
            WHERE agency_id = %s
            ORDER BY created_at DESC, user_id DESC
            """
        ).format(self._tbl())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (agency_id,))
            rows = cur.fetchall()
            return [self._normalize_row(row) for row in rows]

    def touch_last_login(self, *, user_id: str) -> None:
        assert sql is not None
        stmt = sql.SQL("UPDATE {} SET last_login_at = NOW(), updated_at = NOW() WHERE user_id = %s").format(self._tbl())
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(stmt, (user_id,))
            conn.commit()
