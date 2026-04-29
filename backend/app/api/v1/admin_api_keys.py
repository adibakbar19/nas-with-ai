from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from backend.app.dependencies import get_agency_api_key_service
from backend.app.schemas.api_keys import (
    AgencyApiKeyCreateRequest,
    AgencyApiKeyCreateResponse,
    AgencyApiKeyListResponse,
    AgencyApiKeyRevokeRequest,
    AgencyApiKeyRevokeResponse,
    AgencyApiKeyRotateRequest,
)
from backend.app.security import Admin, get_current_admin
from backend.app.services.agency_api_key_service import AgencyApiKeyService
from backend.app.services.errors import ServiceError

router = APIRouter(tags=["admin-api-keys"])


@router.post(
    "/api/v1/admin/agencies/{agency_id}/api-keys",
    response_model=AgencyApiKeyCreateResponse,
    response_model_exclude_none=True,
)
def create_agency_api_key(
    agency_id: str,
    request: AgencyApiKeyCreateRequest,
    admin: Admin = Depends(get_current_admin),
    service: AgencyApiKeyService = Depends(get_agency_api_key_service),
) -> dict[str, Any]:
    try:
        return service.create_key(
            agency_id=agency_id,
            label=request.label,
            expires_at=request.expires_at,
            created_by=admin.actor,
        )
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get(
    "/api/v1/admin/agencies/{agency_id}/api-keys",
    response_model=AgencyApiKeyListResponse,
    response_model_exclude_none=True,
)
def list_agency_api_keys(
    agency_id: str,
    _admin: Admin = Depends(get_current_admin),
    service: AgencyApiKeyService = Depends(get_agency_api_key_service),
) -> dict[str, Any]:
    try:
        return service.list_keys(agency_id=agency_id)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post(
    "/api/v1/admin/api-keys/{key_id}/revoke",
    response_model=AgencyApiKeyRevokeResponse,
    response_model_exclude_none=True,
)
def revoke_agency_api_key(
    key_id: str,
    request: AgencyApiKeyRevokeRequest,
    _admin: Admin = Depends(get_current_admin),
    service: AgencyApiKeyService = Depends(get_agency_api_key_service),
) -> dict[str, Any]:
    try:
        return service.revoke_key(key_id=key_id, revoked_reason=request.revoked_reason)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post(
    "/api/v1/admin/api-keys/{key_id}/rotate",
    response_model=AgencyApiKeyCreateResponse,
    response_model_exclude_none=True,
)
def rotate_agency_api_key(
    key_id: str,
    request: AgencyApiKeyRotateRequest,
    admin: Admin = Depends(get_current_admin),
    service: AgencyApiKeyService = Depends(get_agency_api_key_service),
) -> dict[str, Any]:
    try:
        return service.rotate_key(
            key_id=key_id,
            label=request.label,
            expires_at=request.expires_at,
            revoked_reason=request.revoked_reason,
            created_by=admin.actor,
        )
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
