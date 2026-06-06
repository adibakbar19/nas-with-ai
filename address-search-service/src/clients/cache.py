"""Cache client — synchronous Redis, best-effort (never raises)."""

from __future__ import annotations

import logging

import redis as redis_lib

from config import settings

logger = logging.getLogger(__name__)

_client: redis_lib.Redis | None = None


def get_cache() -> redis_lib.Redis:
    global _client
    if _client is None:
        _client = redis_lib.from_url(settings.redis_url, decode_responses=True)
    return _client


def cache_get(key: str) -> str | None:
    try:
        return get_cache().get(key)
    except Exception as exc:
        logger.debug("cache_get_failed key=%s: %s", key, exc)
        return None


def cache_set(key: str, value: str, ttl: int | None = None) -> None:
    try:
        get_cache().setex(key, ttl or settings.cache_ttl_seconds, value)
    except Exception as exc:
        logger.debug("cache_set_failed key=%s: %s", key, exc)


def check_health() -> bool:
    try:
        return bool(get_cache().ping())
    except Exception:
        return False
