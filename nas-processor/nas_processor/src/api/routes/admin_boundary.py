"""Boundary admin routes — requires platform_admin realm role."""

from typing import Any

from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile

from nas_processor.src.api.auth import Agency, get_current_agency, require_platform_admin
from nas_processor.src.api.schemas.boundary import (
    BoundaryActivateRequest,
    BoundaryUploadCreateResponse,
    BoundaryUploadVersionListResponse,
)
from nas_processor.src.api.services.boundary_admin_service import BoundaryAdminService

router = APIRouter(tags=["admin-boundary"])


def get_boundary_service(request: Request) -> BoundaryAdminService:
    return request.app.state.boundary_service


@router.post(
    "/api/v1/admin/boundaries/uploads",
    response_model=BoundaryUploadCreateResponse,
    response_model_exclude_none=True,
)
async def upload_boundary(
    boundary_type: str = Form(...),
    version_label: str = Form(...),
    source_note: str | None = Form(None),
    file: UploadFile = File(...),
    agency: Agency = Depends(get_current_agency),
    service: BoundaryAdminService = Depends(get_boundary_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    raw_bytes = await file.read()
    return service.create_upload(
        boundary_type=boundary_type,
        version_label=version_label,
        uploaded_by=agency.client_id,
        source_note=source_note,
        upload_filename=file.filename or "upload.json",
        raw_bytes=raw_bytes,
    )


@router.get(
    "/api/v1/admin/boundaries/versions",
    response_model=BoundaryUploadVersionListResponse,
    response_model_exclude_none=True,
)
async def list_boundary_versions(
    boundary_type: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    agency: Agency = Depends(get_current_agency),
    service: BoundaryAdminService = Depends(get_boundary_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.list_versions(boundary_type=boundary_type, status=status, limit=limit)


@router.get("/api/v1/admin/boundaries/versions/{version_id}")
async def get_boundary_version(
    version_id: int,
    agency: Agency = Depends(get_current_agency),
    service: BoundaryAdminService = Depends(get_boundary_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.get_version(version_id=version_id)


@router.post("/api/v1/admin/boundaries/versions/{version_id}/activate")
async def activate_boundary_version(
    version_id: int,
    body: BoundaryActivateRequest,
    agency: Agency = Depends(get_current_agency),
    service: BoundaryAdminService = Depends(get_boundary_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.activate_version(
        version_id=version_id,
        activated_by=agency.client_id,
        activation_note=body.activation_note,
    )
