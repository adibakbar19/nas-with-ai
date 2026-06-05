"""Autocomplete service configuration."""

from __future__ import annotations

import os


DOMAIN_CONFIG: dict[str, dict] = {
    "address": {
        "index": "nas_addresses",
        "query_type": "bool_prefix",
        "query_fields": [
            "autocomplete",
            "autocomplete._2gram",
            "autocomplete._3gram",
        ],
        "result_field": "address_clean",
    },
    # Future domains:
    # "complaint": {
    #     "index": "autocomplete-complaint",
    #     "query_type": "match_phrase_prefix",
    #     "query_fields": ["value"],
    #     "result_field": "value",
    # },
}


class Settings:
    """Settings for autocomplete-service."""

    def __init__(self) -> None:
        self.HOST: str = os.environ.get("HOST", "0.0.0.0")
        self.OPENSEARCH_URL: str = os.environ.get(
            "OPENSEARCH_URL", "http://opensearch:9200"
        ).rstrip("/")
        self.REDIS_URL: str = os.environ.get("REDIS_URL", "redis://valkey:6379/0")
        self.LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "info")

        # PORT — must be an integer in range 1–65535
        port_raw = os.environ.get("PORT", "8002")
        try:
            port_value = int(port_raw)
        except ValueError:
            raise SystemExit(
                f"Invalid configuration: PORT must be an integer, got '{port_raw}'"
            )
        if port_value < 1 or port_value > 65535:
            raise SystemExit(
                f"Invalid configuration: PORT must be in range 1-65535, got '{port_raw}'"
            )
        self.PORT: int = port_value

        # CACHE_TTL_SECONDS — must be a positive integer
        ttl_raw = os.environ.get("CACHE_TTL_SECONDS", "300")
        try:
            ttl_value = int(ttl_raw)
        except ValueError:
            raise SystemExit(
                f"Invalid configuration: CACHE_TTL_SECONDS must be a positive integer, got '{ttl_raw}'"
            )
        if ttl_value < 1:
            raise SystemExit(
                f"Invalid configuration: CACHE_TTL_SECONDS must be a positive integer, got '{ttl_raw}'"
            )
        self.CACHE_TTL_SECONDS: int = ttl_value


settings = Settings()
