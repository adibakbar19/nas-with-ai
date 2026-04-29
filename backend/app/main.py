import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from backend.app.api.v1.router import api_router
from backend.app.monitoring import record_http_request
from backend.app.runtime import jobs as ingest_jobs
from backend.app.runtime import settings as runtime_settings


@asynccontextmanager
async def _lifespan(_: FastAPI):
    ingest_jobs.load_jobs_state()
    yield


app = FastAPI(title="NAS API", version="0.1.0", lifespan=_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=runtime_settings.CORS_ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def _metrics_middleware(request: Request, call_next):
    start = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        route_obj = request.scope.get("route")
        route_path = getattr(route_obj, "path", request.url.path) or request.url.path
        if route_path != "/metrics":
            record_http_request(
                method=request.method,
                route=route_path,
                status_code=status_code,
                duration_seconds=time.perf_counter() - start,
            )


app.include_router(api_router)
