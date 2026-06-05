"""Redis cache helpers — best effort, never crash."""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


async def get_cached(redis, key: str) -> list[str] | None:
    """Attempt Redis GET + JSON deserialize. Returns None on miss or any error."""
    try:
        raw = await redis.get(key)
        if raw is None:
            return None
        return json.loads(raw)
    except Exception as exc:
        logger.debug("cache_get_failed key=%s: %s", key, exc)
        return None


async def set_cached(redis, key: str, value: list[str], ttl: int) -> None:
    """Attempt Redis SETEX. Silently ignores errors."""
    try:
        await redis.setex(key, ttl, json.dumps(value))
    except Exception as exc:
        logger.debug("cache_set_failed key=%s: %s", key, exc)
