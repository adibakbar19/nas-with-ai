from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from backend.app.dependencies import get_agency_user_service
from backend.app.schemas.users import AgencyUserCreateRequest, AgencyUserListResponse, AgencyUserResponse
from backend.app.security import Admin, get_current_admin
from backend.app.services.agency_user_service import AgencyUserService
from backend.app.services.errors import ServiceError

router = APIRouter(tags=["admin-users"])


@router.post(
    "/api/v1/admin/agencies/{agency_id}/users",
    response_model=AgencyUserResponse,
    response_model_exclude_none=True,
)
def create_agency_user(
    agency_id: str,
    request: AgencyUserCreateRequest,
    admin: Admin = Depends(get_current_admin),
    service: AgencyUserService = Depends(get_agency_user_service),
) -> dict[str, Any]:
    try:
        return service.create_user(
            agency_id=agency_id,
            username=request.username,
            password=request.password,
            role=request.role,
            display_name=request.display_name,
            email=request.email,
            created_by=admin.actor,
        )
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get(
    "/api/v1/admin/agencies/{agency_id}/users",
    response_model=AgencyUserListResponse,
    response_model_exclude_none=True,
)
def list_agency_users(
    agency_id: str,
    _admin: Admin = Depends(get_current_admin),
    service: AgencyUserService = Depends(get_agency_user_service),
) -> dict[str, Any]:
    try:
        return service.list_users(agency_id=agency_id)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
