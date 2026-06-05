"""Database connection string builders.

Consolidates the PG* env var → DSN construction logic that was duplicated
across backend/app/db/migration_settings.py, backend/app/core/settings.py,
etl/load/postgres.py, and etl/pipeline/loader.py.
"""

from __future__ import annotations

import os


def build_dsn(
    *,
    driver: str | None = None,
    env: dict[str, str] | None = None,
) -> str:
    """Build a PostgreSQL DSN from environment variables.

    Resolution order:
      1. POSTGRES_DSN (if set)
      2. DATABASE_URL (if set)
      3. Constructed from PGUSER, PGPASSWORD, PGHOST, PGPORT, PGDATABASE

    Args:
        driver: Optional SQLAlchemy driver suffix (e.g. "psycopg").
                If provided, the returned URL uses "postgresql+{driver}://..."
                instead of "postgresql://...".
        env: Optional env dict override (defaults to os.environ).

    Returns:
        A postgresql:// or postgresql+driver:// connection string.
    """
    ctx = env if env is not None else os.environ

    # Check explicit DSN env vars first
    for key in ("POSTGRES_DSN", "DATABASE_URL"):
        raw = str(ctx.get(key, "")).strip()
        if raw:
            return _apply_driver(raw, driver)

    # Construct from PG* parts
    user = str(ctx.get("PGUSER", "postgres")).strip() or "postgres"
    password = str(ctx.get("PGPASSWORD", "postgres")).strip() or "postgres"
    host = str(ctx.get("PGHOST", "localhost")).strip() or "localhost"
    port = str(ctx.get("PGPORT", "5432")).strip() or "5432"
    database = str(ctx.get("PGDATABASE", "postgres")).strip() or "postgres"

    scheme = f"postgresql+{driver}" if driver else "postgresql"
    return f"{scheme}://{user}:{password}@{host}:{port}/{database}"


def _apply_driver(dsn: str, driver: str | None) -> str:
    """Ensure the DSN uses the requested driver prefix."""
    if not driver:
        return dsn
    target = f"postgresql+{driver}://"
    if dsn.startswith(target):
        return dsn
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", target, 1)
    if dsn.startswith("postgres://"):
        return dsn.replace("postgres://", target, 1)
    return dsn


def build_valkey_url(*, env: dict[str, str] | None = None) -> str:
    """Return the Valkey/Redis URL, checking VALKEY_URL first then REDIS_URL.

    Args:
        env: Optional env dict override (defaults to os.environ).

    Returns:
        The connection URL string, or empty string if neither is set.
    """
    ctx = env if env is not None else os.environ
    return (
        str(ctx.get("VALKEY_URL", "")).strip()
        or str(ctx.get("REDIS_URL", "")).strip()
        or ""
    )
