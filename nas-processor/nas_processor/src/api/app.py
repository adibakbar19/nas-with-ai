"""NAS Processor Read API — address search, lookup, and admin endpoints."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from nas_processor.src.api.config import settings
from nas_processor.src.api.errors import ServiceError
from nas_processor.src.api.repositories.address_read_repository import AddressReadRepository
from nas_processor.src.api.repositories.boundary_admin_repository import BoundaryAdminRepository
from nas_processor.src.api.repositories.lookup_admin_repository import LookupAdminRepository
from nas_processor.src.api.services.address_service import AddressReadService
from nas_processor.src.api.services.boundary_admin_service import BoundaryAdminService
from nas_processor.src.api.services.lookup_admin_service import LookupAdminService
from nas_processor.src.api.services.search_service import SearchApiService


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage shared resources: Redis, JWKS cache, services."""
    # ── Auth layer ───────────────────────────────────────────────────────
    app.state.redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    keycloak_url = settings.KEYCLOAK_URL
    if keycloak_url:
        from nas_processor.src.api.auth.jwks_cache import JWKSCache
        app.state.jwks_cache = JWKSCache(
            f"{keycloak_url}/protocol/openid-connect/certs"
        )
    else:
        app.state.jwks_cache = None

    # ── Services ─────────────────────────────────────────────────────────
    dsn = settings.POSTGRES_DSN
    schema = settings.postgres_schema()
    lookup_schema = settings.lookup_schema()

    app.state.search_service = SearchApiService(
        es_url=settings.ES_URL,
        es_index=settings.ES_INDEX,
    )
    app.state.address_service = AddressReadService(
        repository=AddressReadRepository(dsn=dsn, schema=schema)
    )
    app.state.lookup_admin_service = LookupAdminService(
        repository=LookupAdminRepository(
            dsn=dsn,
            lookup_schema=lookup_schema,
            runtime_schema=schema,
        ),
        lookup_schema=lookup_schema,
        runtime_schema=schema,
    )
    app.state.boundary_service = BoundaryAdminService(
        repository=BoundaryAdminRepository(
            dsn=dsn,
            lookup_schema=lookup_schema,
        ),
        lookup_schema=lookup_schema,
    )

    # Review queue — just store dsn/schema; repository functions are called directly
    app.state.review_dsn = dsn
    app.state.review_schema = schema

    yield

    # ── Shutdown ─────────────────────────────────────────────────────────
    await app.state.redis.aclose()


app = FastAPI(
    title="NAS Processor Read API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url=None,
    lifespan=lifespan,
)


@app.exception_handler(ServiceError)
async def service_error_handler(request: Request, exc: ServiceError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )


from nas_processor.src.api.routes.search import router as search_router  # noqa: E402
from nas_processor.src.api.routes.address import router as address_router  # noqa: E402
from nas_processor.src.api.routes.admin_lookup import router as admin_lookup_router  # noqa: E402
from nas_processor.src.api.routes.admin_boundary import router as admin_boundary_router  # noqa: E402
from nas_processor.src.api.routes.review import router as review_router  # noqa: E402

app.include_router(search_router)
app.include_router(address_router)
app.include_router(admin_lookup_router)
app.include_router(admin_boundary_router)
app.include_router(review_router)


@app.get("/health")
def health() -> dict[str, str]:
    """Liveness probe."""
    return {"status": "ok", "service": "nas-processor-api"}
