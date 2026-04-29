from __future__ import annotations

import os
from pathlib import Path

from backend.app.db.migration_settings import get_runtime_db_dsn, get_runtime_db_schema
from backend.app.object_store import ObjectStoreSettings
from nas_core.config.env import validate_backend_env


APP_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = APP_DIR.parents[1]
LOG_DIR = PROJECT_ROOT / "logs" / "jobs"
UPLOAD_STAGING_DIR = PROJECT_ROOT / "data" / "uploads"
OUTPUT_UPLOADS_DIR = PROJECT_ROOT / "output" / "uploads"
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


def parse_cors_origins(raw: str) -> list[str]:
    origins = [item.strip() for item in raw.split(",")]
    return [item for item in origins if item]


def resolve_project_path(raw_path: str | Path) -> Path:
    path = Path(raw_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


load_env_file()

RUNNING_RECOVERY_WINDOW_SECONDS = max(
    60,
    int(os.getenv("INGEST_RUNNING_RECOVERY_WINDOW_SECONDS", "600")),
)
STRICT_ENV_CHECK = os.getenv("STRICT_ENV_CHECK", "1").lower() in {"1", "true", "yes", "y"}
if STRICT_ENV_CHECK:
    missing_backend_env = validate_backend_env()
    if missing_backend_env:
        raise RuntimeError(f"Missing required backend env vars: {', '.join(missing_backend_env)}")

ES_URL = os.getenv("ES_URL", "http://localhost:9200").rstrip("/")
ES_INDEX = os.getenv("ES_INDEX", "nas_addresses")
INGEST_PERSIST_LIVE_UPDATES = os.getenv("INGEST_PERSIST_LIVE_UPDATES", "1").lower() in {"1", "true", "yes", "y"}
INGEST_PERSIST_LIVE_INTERVAL_SECONDS = max(
    0.5,
    float(os.getenv("INGEST_PERSIST_LIVE_INTERVAL_SECONDS", "2.0")),
)
INGEST_STALE_CLAIM_SECONDS = max(
    30,
    int(os.getenv("INGEST_STALE_CLAIM_SECONDS", "300")),
)
INGEST_STALE_CLAIM_RECOVERY_INTERVAL_SECONDS = max(
    5,
    int(os.getenv("INGEST_STALE_CLAIM_RECOVERY_INTERVAL_SECONDS", "30")),
)

OBJECT_STORE = ObjectStoreSettings.from_env()
OBJECT_STORE_BUCKET = OBJECT_STORE.bucket
OBJECT_STORE_ENDPOINT = OBJECT_STORE.endpoint
OBJECT_STORE_PUBLIC_ENDPOINT = OBJECT_STORE.public_endpoint
OBJECT_STORE_SECURE = OBJECT_STORE.secure

INGEST_EXECUTION_MODE = os.getenv("INGEST_EXECUTION_MODE", "queue_worker").strip().lower()
if INGEST_EXECUTION_MODE != "queue_worker":
    raise RuntimeError("INGEST_EXECUTION_MODE must be queue_worker in production")

INGEST_JOB_STATE_DSN = get_runtime_db_dsn()
INGEST_JOB_STATE_SCHEMA = get_runtime_db_schema()
CORS_ALLOW_ORIGINS = parse_cors_origins(
    os.getenv(
        "CORS_ALLOW_ORIGINS",
        "http://localhost:3000,http://localhost:5173,https://admin.alamat.gov.my",
    )
)
