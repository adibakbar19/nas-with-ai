"""Autocomplete service — FastAPI application."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from cache import get_cached, set_cached
from config import DOMAIN_CONFIG, settings
from search import query_opensearch

logging.basicConfig(level=settings.LOG_LEVEL.upper())
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create shared Redis connection on startup, close on shutdown."""
    app.state.redis = aioredis.from_url(
        settings.REDIS_URL, decode_responses=True
    )
    yield
    await app.state.redis.aclose()


app = FastAPI(title="Autocomplete Service", lifespan=lifespan)


@app.middleware("http")
async def add_deprecation_header(request: Request, call_next):
    response = await call_next(request)
    response.headers["Deprecation"] = "true"
    response.headers["Sunset"] = "Sat, 01 Aug 2026 00:00:00 GMT"
    response.headers["Link"] = '<http://localhost:8003>; rel="successor-version"'
    response.headers["X-Deprecation-Notice"] = (
        "This service is deprecated. "
        "Migrate to address-search-service port 8003. "
        "Equivalent endpoint: GET /autocomplete"
    )
    return response


@app.get("/health")
async def health():
    """Liveness probe."""
    return {"status": "ok", "service": "autocomplete-service"}


@app.get("/autocomplete")
async def autocomplete(
    request: Request,
    q: str | None = Query(default=None),
    domain: str | None = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100),
):
    """Return autocomplete suggestions."""
    # Validate q
    if not q or not q.strip():
        return JSONResponse(
            status_code=400,
            content={"detail": "q is required"},
        )

    # Validate domain
    if not domain or not domain.strip():
        return JSONResponse(
            status_code=400,
            content={"detail": "domain is required"},
        )
    if domain not in DOMAIN_CONFIG:
        valid = list(DOMAIN_CONFIG.keys())
        return JSONResponse(
            status_code=400,
            content={"detail": f"Unknown domain '{domain}'. Valid domains: {valid}"},
        )

    # Normalize query for cache key
    normalized_q = q.strip().lower()
    cache_key = f"ac:{domain}:{normalized_q}:{limit}"

    # 1. Check Redis cache
    redis_client = request.app.state.redis
    cached = await get_cached(redis_client, cache_key)
    if cached is not None:
        return {"suggestions": cached}

    # 2. Query OpenSearch
    domain_cfg = DOMAIN_CONFIG[domain]
    try:
        suggestions = await query_opensearch(
            opensearch_url=settings.OPENSEARCH_URL,
            domain_config=domain_cfg,
            query=normalized_q,
            limit=limit,
        )
    except httpx.ConnectError:
        return JSONResponse(
            status_code=503,
            content={"detail": "OpenSearch is unreachable"},
        )
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if status >= 500:
            return JSONResponse(
                status_code=503,
                content={"detail": f"OpenSearch upstream error: {status}"},
            )
        return JSONResponse(
            status_code=502,
            content={"detail": f"OpenSearch bad gateway: {status}"},
        )

    # 3. Cache result (even empty lists)
    await set_cached(redis_client, cache_key, suggestions, settings.CACHE_TTL_SECONDS)

    return {"suggestions": suggestions}
