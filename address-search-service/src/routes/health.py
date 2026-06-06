"""Health and readiness endpoints."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from src.clients.cache import check_health as cache_health
from src.clients.opensearch import check_health as os_health
from src.clients.postgres import check_health as pg_health
from src.schemas.responses import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health() -> dict:
    os_ok = os_health()
    pg_ok = pg_health()
    cache_ok = cache_health()

    if os_ok and pg_ok and cache_ok:
        status = "healthy"
    elif os_ok:
        status = "degraded"
    else:
        status = "unhealthy"

    return {
        "status": status,
        "opensearch": os_ok,
        "postgres": pg_ok,
        "cache": cache_ok,
    }


@router.get("/ready")
def ready():
    """Readiness probe — returns 200 if OpenSearch is reachable, 503 otherwise."""
    if os_health():
        return {"status": "ready"}
    return JSONResponse(status_code=503, content={"status": "not_ready"})
