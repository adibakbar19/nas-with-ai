import os
import re
from pathlib import Path


_SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PROJECT_ROOT = Path(__file__).resolve().parents[3]
ENV_FILE = PROJECT_ROOT / ".env"


def load_env_file(env_file: Path = ENV_FILE) -> None:
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


def _default_postgres_dsn() -> str:
    return (
        "postgresql://"
        f"{os.getenv('PGUSER', 'postgres')}:{os.getenv('PGPASSWORD', 'postgres')}"
        f"@{os.getenv('PGHOST', 'localhost')}:{os.getenv('PGPORT', '5432')}"
        f"/{os.getenv('PGDATABASE', 'postgres')}"
    )


def get_runtime_db_dsn() -> str:
    return os.getenv("INGEST_JOB_STATE_DSN", os.getenv("POSTGRES_DSN", _default_postgres_dsn()))


def get_runtime_db_sqlalchemy_url() -> str:
    dsn = get_runtime_db_dsn()
    if dsn.startswith("postgresql+"):
        return dsn
    if dsn.startswith("postgres://"):
        return "postgresql+psycopg://" + dsn[len("postgres://") :]
    if dsn.startswith("postgresql://"):
        return "postgresql+psycopg://" + dsn[len("postgresql://") :]
    return dsn


def get_runtime_db_schema() -> str:
    schema = (os.getenv("INGEST_JOB_STATE_SCHEMA") or os.getenv("PGSCHEMA") or "nas").strip() or "nas"
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        raise ValueError(f"Invalid Postgres schema name: {schema!r}")
    return schema
