"""Address Search Service — main FastAPI application."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from config import settings
from src.clients.cache import check_health as cache_health
from src.clients.opensearch import check_health as os_health, ensure_location_mapping
from src.clients.postgres import check_health as pg_health
from src.routes.geocode import router as geocode_router
from src.routes.health import router as health_router
from src.routes.search import router as search_router
from src.routes.spatial import router as spatial_router
from src.routes.validate import router as validate_router

logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("address_search_service_starting port=%d", settings.port)

    os_ok = os_health()
    pg_ok = pg_health()
    cache_ok = cache_health()
    logger.info("startup_health os=%s pg=%s cache=%s", os_ok, pg_ok, cache_ok)

    if os_ok:
        ensure_location_mapping()

    yield

    logger.info("address_search_service_stopping")


app = FastAPI(
    title="Address Search Service",
    description="Malaysian address search, autocomplete, validation, spatial queries, and geocoding.",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(health_router)
app.include_router(search_router)
app.include_router(validate_router)
app.include_router(spatial_router)
app.include_router(geocode_router)
