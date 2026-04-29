from typing import Any

from fastapi import APIRouter, Depends, Response

from backend.app.dependencies import get_ops_service
from backend.app.monitoring import CONTENT_TYPE_LATEST, metrics_payload
from backend.app.services.ops_service import OpsService

router = APIRouter(tags=["ops"])


@router.get("/api/v1/health")
def health(ops_service: OpsService = Depends(get_ops_service)) -> dict[str, Any]:
    return ops_service.health()


@router.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    return Response(content=metrics_payload(), media_type=CONTENT_TYPE_LATEST)


@router.get("/")
def root(ops_service: OpsService = Depends(get_ops_service)) -> dict[str, Any]:
    return ops_service.root()
