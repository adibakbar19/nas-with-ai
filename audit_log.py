import json
import os
import socket
import uuid
from datetime import datetime, timezone
from getpass import getuser
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _sanitize_value(key: str, value: Any) -> Any:
    lowered = key.lower()
    if any(token in lowered for token in ("password", "secret", "token", "api_key")):
        return "***REDACTED***"
    return value


def _sanitize_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not payload:
        return {}
    clean: dict[str, Any] = {}
    for key, value in payload.items():
        clean[key] = _sanitize_value(str(key), value)
    return clean


def _write_jsonl(log_path: str, record: dict[str, Any]) -> None:
    try:
        directory = os.path.dirname(log_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=True, sort_keys=True, default=str))
            handle.write("\n")
    except Exception:
        # Audit logging must never fail the pipeline.
        pass


def start_audit_run(log_path: str, app: str, args: dict[str, Any] | None = None) -> str:
    run_id = uuid.uuid4().hex
    record = {
        "ts": _utc_now_iso(),
        "app": app,
        "event": "run_start",
        "run_id": run_id,
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "user": getuser(),
        "args": _sanitize_payload(args),
    }
    _write_jsonl(log_path, record)
    return run_id


def audit_event(log_path: str, app: str, run_id: str, event: str, **fields: Any) -> None:
    record = {
        "ts": _utc_now_iso(),
        "app": app,
        "event": event,
        "run_id": run_id,
    }
    record.update(fields)
    _write_jsonl(log_path, record)

