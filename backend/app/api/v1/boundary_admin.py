from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile

from backend.app.dependencies import get_boundary_admin_service
from backend.app.schemas.boundary_admin import (
    BoundaryActivateRequest,
    BoundaryUploadCreateResponse,
    BoundaryUploadVersionListResponse,
    BoundaryUploadVersionResponse,
)
from backend.app.security import Admin, get_current_admin
from backend.app.services.boundary_admin_service import BoundaryAdminService
from backend.app.services.errors import ServiceError

router = APIRouter(tags=["admin-boundaries"])


def _raise_service_error(exc: ServiceError) -> None:
    raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post(
    "/api/v1/admin/boundaries/uploads",
    response_model=BoundaryUploadCreateResponse,
    response_model_exclude_none=True,
)
async def create_boundary_upload(
    boundary_type: str = Form(...),
    version_label: str = Form(...),
    source_note: str | None = Form(default=None),
    file: UploadFile = File(...),
    admin: Admin = Depends(get_current_admin),
    service: BoundaryAdminService = Depends(get_boundary_admin_service),
) -> dict[str, Any]:
    try:
        payload = await file.read()
        return service.create_upload(
            boundary_type=boundary_type,
            version_label=version_label,
            uploaded_by=admin.actor,
            source_note=source_note,
            upload_filename=file.filename,
            raw_bytes=payload,
        )
    except ServiceError as exc:
        _raise_service_error(exc)


@router.get(
    "/api/v1/admin/boundaries/versions",
    response_model=BoundaryUploadVersionListResponse,
    response_model_exclude_none=True,
)
def list_boundary_versions(
    boundary_type: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    _admin: Admin = Depends(get_current_admin),
    service: BoundaryAdminService = Depends(get_boundary_admin_service),
) -> dict[str, Any]:
    try:
        return service.list_versions(boundary_type=boundary_type, status=status, limit=limit)
    except ServiceError as exc:
        _raise_service_error(exc)


@router.get(
    "/api/v1/admin/boundaries/versions/{version_id}",
    response_model=BoundaryUploadVersionResponse,
    response_model_exclude_none=True,
)
def get_boundary_version(
    version_id: int,
    _admin: Admin = Depends(get_current_admin),
    service: BoundaryAdminService = Depends(get_boundary_admin_service),
) -> dict[str, Any]:
    try:
        return service.get_version(version_id=version_id)
    except ServiceError as exc:
        _raise_service_error(exc)


@router.post(
    "/api/v1/admin/boundaries/versions/{version_id}/activate",
    response_model=BoundaryUploadVersionResponse,
    response_model_exclude_none=True,
)
def activate_boundary_version(
    version_id: int,
    request: BoundaryActivateRequest,
    admin: Admin = Depends(get_current_admin),
    service: BoundaryAdminService = Depends(get_boundary_admin_service),
) -> dict[str, Any]:
    try:
        return service.activate_version(
            version_id=version_id,
            activated_by=admin.actor,
            activation_note=request.activation_note,
        )
    except ServiceError as exc:
        _raise_service_error(exc)
