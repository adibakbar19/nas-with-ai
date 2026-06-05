"""Ingestion API — lightweight FastAPI service for file upload and job management."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from config import settings
from src.auth.jwks_cache import JWKSCache
from src.errors import ServiceError
from src.queue.producer import ValkeyStreamQueueProducer
from src.repositories.api_idempotency_repository import ApiIdempotencyRepository
from src.repositories.multipart_upload_repository import MultipartUploadRepository
from src.services.ingest_service import IngestService
from src.state.job_repository import IngestJobStateRepository
from src.state.job_state import IngestJobState
from src.storage.object_store import ObjectStoreSettings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage shared resources: Redis, JWKS cache, IngestService."""
    # ── Auth layer ───────────────────────────────────────────────────────
    app.state.redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    keycloak_url = settings.KEYCLOAK_URL
    if keycloak_url:
        app.state.jwks_cache = JWKSCache(
            f"{keycloak_url}/protocol/openid-connect/certs"
        )
    else:
        app.state.jwks_cache = None

    # ── Service layer ────────────────────────────────────────────────────
    dsn = settings.POSTGRES_DSN
    schema = settings.postgres_schema()
    object_store = ObjectStoreSettings.from_env()

    job_repo = IngestJobStateRepository(dsn=dsn, schema=schema)
    producer = ValkeyStreamQueueProducer(
        valkey_url=settings.VALKEY_URL,
        stream_key=settings.VALKEY_STREAM_KEY,
    )
    job_state = IngestJobState(
        job_repo=job_repo,
        producer=producer,
        object_store_bucket=object_store.bucket,
    )
    multipart_repo = MultipartUploadRepository(dsn=dsn, schema=schema)
    idempotency_repo = ApiIdempotencyRepository(dsn=dsn, schema=schema)

    app.state.ingest_service = IngestService(
        job_state=job_state,
        multipart_repo=multipart_repo,
        idempotency_repo=idempotency_repo,
        object_store=object_store,
        output_dir=Path("output/uploads"),
    )

    yield

    # ── Shutdown ─────────────────────────────────────────────────────────
    await app.state.redis.aclose()


app = FastAPI(
    title="NAS Ingestion API",
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


from src.routes.ingest import router as ingest_router  # noqa: E402

app.include_router(ingest_router)


@app.get("/health")
def health() -> dict[str, str]:
    """Liveness probe."""
    return {"status": "ok", "service": "ingestion-api"}
