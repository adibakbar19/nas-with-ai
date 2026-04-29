from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException

from backend.app.dependencies import get_auth_service
from backend.app.schemas.auth import AgencyTokenRequest, AgencyTokenResponse, UserLoginRequest
from backend.app.services.auth_service import AuthService
from backend.app.services.errors import ServiceError

router = APIRouter(tags=["auth"])


@router.post(
    "/api/v1/auth/token",
    response_model=AgencyTokenResponse,
    response_model_exclude_none=True,
)
def issue_agency_token(
    request: AgencyTokenRequest | None = None,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    auth_service: AuthService = Depends(get_auth_service),
) -> dict[str, Any]:
    try:
        return auth_service.issue_agency_token(request=request, api_key=x_api_key)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post(
    "/api/v1/auth/login",
    response_model=AgencyTokenResponse,
    response_model_exclude_none=True,
)
def login_user(
    request: UserLoginRequest,
    auth_service: AuthService = Depends(get_auth_service),
) -> dict[str, Any]:
    try:
        return auth_service.login_user(
            username=request.username,
            password=request.password,
            expires_in=request.expires_in,
        )
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
