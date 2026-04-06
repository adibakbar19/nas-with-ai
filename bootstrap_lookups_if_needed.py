import json
import os
from pathlib import Path

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None


PROJECT_ROOT = Path(__file__).resolve().parent
ENV_FILE = PROJECT_ROOT / ".env"
DEFAULT_INGEST_CONFIG_PATH = Path(os.getenv("INGEST_DEFAULT_CONFIG_PATH", "config/config.json"))


def _load_env_file(env_file: Path) -> None:
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


def _resolve_project_path(raw_path: str | Path) -> Path:
    path = Path(raw_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _config_bool(config: dict[str, object], key: str, default: bool) -> bool:
    value = config.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return bool(value)


def _load_ingest_config(config_path: str | Path = DEFAULT_INGEST_CONFIG_PATH) -> tuple[Path, dict[str, object]]:
    resolved = _resolve_project_path(config_path)
    if not resolved.exists():
        raise RuntimeError(f"Ingest config not found: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Unable to read ingest config {resolved}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Ingest config must be a JSON object: {resolved}")
    return resolved, payload


def _required_lookup_keys(config: dict[str, object]) -> tuple[str, set[str]]:
    lookup_source = str(config.get("lookup_source", "files")).strip().lower()
    boundary_source = str(config.get("boundary_source", "files")).strip().lower()
    schema = (
        str(
            config.get("lookup_db_schema")
            or config.get("boundary_db_schema")
            or os.getenv("LOOKUP_SCHEMA")
            or os.getenv("PGSCHEMA")
            or "nas_lookup"
        ).strip()
        or "nas_lookup"
    )

    required: set[str] = set()
    if lookup_source == "db":
        required.update({"state", "district", "mukim", "locality", "sublocality", "postcode"})

    if boundary_source == "db":
        if _config_bool(config, "admin_boundary_enabled", True):
            required.update({"state_boundary", "district_boundary", "mukim_boundary"})
        if _config_bool(config, "postcode_boundary_enabled", True):
            required.add(str(config.get("postcode_boundary_table", "postcode_boundary")).strip() or "postcode_boundary")
        if _config_bool(config, "pbt_enabled", True):
            required.add(str(config.get("pbt_boundary_table", "pbt")).strip() or "pbt")

    return schema, required


def _lookup_bootstrap_ready() -> tuple[bool, str, set[str]]:
    if psycopg is None:
        raise RuntimeError("psycopg is required for conditional lookup bootstrap")

    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE", "postgres")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    schema = (os.getenv("LOOKUP_SCHEMA") or os.getenv("PGSCHEMA") or "nas_lookup").strip() or "nas_lookup"

    if not user or not password:
        raise RuntimeError("PGUSER and PGPASSWORD are required for conditional lookup bootstrap")

    _, config = _load_ingest_config()
    schema, required_keys = _required_lookup_keys(config)
    dsn = f"host={host} port={port} dbname={dbname} user={user} password={password}"
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = 'lookup_version'
            LIMIT 1
            """,
            (schema,),
        )
        if cur.fetchone() is None:
            return False, schema, required_keys

        cur.execute(
            f"SELECT lookup_key FROM {schema}.lookup_version"
        )
        existing_keys = {str(row[0]) for row in cur.fetchall()}

    missing_keys = required_keys - existing_keys
    return not missing_keys, schema, missing_keys


def main() -> int:
    _load_env_file(ENV_FILE)
    ready, schema, missing_keys = _lookup_bootstrap_ready()
    if ready:
        print(f"Lookup bootstrap skipped: {schema}.lookup_version already contains all required keys.")
        return 0

    missing_summary = ", ".join(sorted(missing_keys)) if missing_keys else "lookup_version"
    print(f"Lookup bootstrap required: missing keys/table for schema {schema}: {missing_summary}.")
    print("Running bootstrap_lookups.py.")
    from etl.bootstrap_lookups import main as bootstrap_main

    bootstrap_main()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
