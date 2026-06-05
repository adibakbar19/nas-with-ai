"""Database connection helpers for Alembic migrations.

Uses the shared nas_config package to build DSNs from PG* environment
variables. No dependency on backend/ or any application code.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from nas_config.db import build_dsn


_SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PROJECT_ROOT = Path(__file__).resolve().parent
ENV_FILE = PROJECT_ROOT / ".env"


def load_env_file(env_file: Path = ENV_FILE) -> None:
    """Load .env file into os.environ (setdefault, won't overwrite)."""
    if not env_file.exists():
        return
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            os.environ.setdefault(key, value)


def get_runtime_db_sqlalchemy_url() -> str:
    """Return a SQLAlchemy-compatible postgresql+psycopg:// URL."""
    dsn = os.getenv("INGEST_JOB_STATE_DSN", "") or build_dsn()
    if dsn.startswith("postgresql+"):
        return dsn
    if dsn.startswith("postgres://"):
        return "postgresql+psycopg://" + dsn[len("postgres://"):]
    if dsn.startswith("postgresql://"):
        return "postgresql+psycopg://" + dsn[len("postgresql://"):]
    return dsn


def get_runtime_db_schema() -> str:
    """Return the target Postgres schema name (default: 'nas')."""
    schema = (
        os.getenv("INGEST_JOB_STATE_SCHEMA")
        or os.getenv("PGSCHEMA")
        or "nas"
    ).strip() or "nas"
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        raise ValueError(f"Invalid Postgres schema name: {schema!r}")
    return schema
