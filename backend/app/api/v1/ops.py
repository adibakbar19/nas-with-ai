from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.app.dependencies import get_ops_service
from backend.app.services.errors import ServiceError
from backend.app.services.ops_service import OpsService

router = APIRouter(tags=["ops"])


@router.get("/api/v1/health")
def health(ops_service: OpsService = Depends(get_ops_service)) -> dict[str, Any]:
    return ops_service.health()


@router.get("/api/v1/jobs")
def list_jobs(limit: int = Query(20, ge=1, le=100), ops_service: OpsService = Depends(get_ops_service)) -> dict[str, Any]:
    return ops_service.list_jobs(limit=limit)


@router.get("/api/v1/jobs/{run_id}")
def get_job(run_id: str, ops_service: OpsService = Depends(get_ops_service)) -> dict[str, Any]:
    try:
        return ops_service.get_job(run_id)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/")
def root(ops_service: OpsService = Depends(get_ops_service)) -> dict[str, Any]:
    return ops_service.root()
