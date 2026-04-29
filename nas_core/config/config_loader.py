import json
from pathlib import Path
from typing import Any


SUPPORTED_CONFIG_MAJOR = 1
SECTION_KEYS = ("schema", "sources", "rules")


def _strip_metadata(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_metadata(item)
            for key, item in value.items()
            if not str(key).startswith("_")
        }
    if isinstance(value, list):
        return [_strip_metadata(item) for item in value]
    return value


def normalize_config(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Config must be a JSON object.")

    config_version = str(payload.get("config_version", "1.0.0")).strip() or "1.0.0"
    try:
        major = int(config_version.split(".", 1)[0])
    except ValueError as exc:
        raise ValueError(f"Invalid config_version: {config_version!r}") from exc
    if major != SUPPORTED_CONFIG_MAJOR:
        raise ValueError(
            f"Unsupported config_version={config_version!r}; "
            f"supported major version is {SUPPORTED_CONFIG_MAJOR}."
        )

    cleaned = _strip_metadata(payload)
    normalized: dict[str, Any] = {"config_version": config_version}

    for key, value in cleaned.items():
        if key in SECTION_KEYS or key == "config_version":
            continue
        normalized[key] = value

    for section in SECTION_KEYS:
        section_value = cleaned.get(section, {})
        if section_value is None:
            continue
        if not isinstance(section_value, dict):
            raise ValueError(f"Config section {section!r} must be an object.")
        for key, value in section_value.items():
            normalized[key] = value

    return normalized


def load_config(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return normalize_config({})
    resolved = Path(path)
    with resolved.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return normalize_config(payload)
