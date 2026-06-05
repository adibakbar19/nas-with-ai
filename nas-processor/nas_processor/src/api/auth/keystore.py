"""Redis-backed client routing lookup.

Maps Keycloak client_id to agency routing info:
  key:    apikey:{client_id}
  value:  {"source": "nas", "agency": "agency-a", "topic": "ingest.nas"}

Receives Redis instance via FastAPI Request.app.state (lifespan-managed).
No module-level singletons — fully testable with fakes.
"""

from __future__ import annotations

import json

from fastapi import HTTPException, Request
from redis.asyncio import Redis


async def get_routing(client_id: str, request: Request) -> dict[str, str]:
    """Look up routing info for a client_id from Redis.

    Returns dict with keys: source, agency, topic.
    Raises HTTPException(401) if client_id not found or data is corrupt.
    """
    redis: Redis = request.app.state.redis
    raw = await redis.get(f"apikey:{client_id}")

    if raw is None:
        raise HTTPException(status_code=401, detail=f"unknown client: {client_id}")

    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        raise HTTPException(
            status_code=401, detail=f"corrupt routing data for client: {client_id}"
        )

    required_keys = {"source", "agency", "topic"}
    if not required_keys.issubset(data.keys()):
        raise HTTPException(
            status_code=401,
            detail=f"incomplete routing data for client: {client_id}",
        )

    return data
