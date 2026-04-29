from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.app.dependencies import get_lookup_admin_service
from backend.app.schemas.lookup_admin import (
    LookupDistrictCreateRequest,
    LookupDistrictListResponse,
    LookupDistrictResponse,
    LookupDistrictUpdateRequest,
    LookupLocalityCreateRequest,
    LookupLocalityListResponse,
    LookupLocalityResponse,
    LookupLocalityUpdateRequest,
    LookupMukimCreateRequest,
    LookupMukimListResponse,
    LookupMukimResponse,
    LookupMukimUpdateRequest,
    LookupPostcodeCreateRequest,
    LookupPostcodeListResponse,
    LookupPostcodeResponse,
    LookupPostcodeUpdateRequest,
    LookupStateListResponse,
)
from backend.app.security import Admin, get_current_admin
from backend.app.services.errors import ServiceError
from backend.app.services.lookup_admin_service import LookupAdminService

router = APIRouter(tags=["admin-lookups"])


def _raise_service_error(exc: ServiceError) -> None:
    raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/api/v1/admin/lookups/states", response_model=LookupStateListResponse, response_model_exclude_none=True)
def list_lookup_states(
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.list_states()
    except ServiceError as exc:
        _raise_service_error(exc)


@router.get("/api/v1/admin/lookups/districts", response_model=LookupDistrictListResponse, response_model_exclude_none=True)
def list_lookup_districts(
    state_id: int | None = Query(default=None),
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.list_districts(state_id=state_id)
    except ServiceError as exc:
        _raise_service_error(exc)


@router.get("/api/v1/admin/lookups/districts/{district_id}", response_model=LookupDistrictResponse, response_model_exclude_none=True)
def get_lookup_district(
    district_id: int,
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.get_district(district_id=district_id)
    except ServiceError as exc:
        _raise_service_error(exc)


@router.post("/api/v1/admin/lookups/districts", response_model=LookupDistrictResponse, response_model_exclude_none=True)
def create_lookup_district(
    request: LookupDistrictCreateRequest,
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.create_district(
            district_name=request.district_name,
            district_code=request.district_code,
            state_id=request.state_id,
        )
    except ServiceError as exc:
        _raise_service_error(exc)


@router.patch("/api/v1/admin/lookups/districts/{district_id}", response_model=LookupDistrictResponse, response_model_exclude_none=True)
def update_lookup_district(
    district_id: int,
    request: LookupDistrictUpdateRequest,
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.update_district(
            district_id=district_id,
            district_name=request.district_name,
            district_code=request.district_code,
            state_id=request.state_id,
        )
    except ServiceError as exc:
        _raise_service_error(exc)


@router.get("/api/v1/admin/lookups/mukim", response_model=LookupMukimListResponse, response_model_exclude_none=True)
def list_lookup_mukim(
    district_id: int | None = Query(default=None),
    state_id: int | None = Query(default=None),
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.list_mukim(district_id=district_id, state_id=state_id)
    except ServiceError as exc:
        _raise_service_error(exc)


@router.get("/api/v1/admin/lookups/mukim/{mukim_id}", response_model=LookupMukimResponse, response_model_exclude_none=True)
def get_lookup_mukim(
    mukim_id: int,
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.get_mukim(mukim_id=mukim_id)
    except ServiceError as exc:
        _raise_service_error(exc)


@router.post("/api/v1/admin/lookups/mukim", response_model=LookupMukimResponse, response_model_exclude_none=True)
def create_lookup_mukim(
    request: LookupMukimCreateRequest,
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.create_mukim(
            mukim_name=request.mukim_name,
            mukim_code=request.mukim_code,
            district_id=request.district_id,
        )
    except ServiceError as exc:
        _raise_service_error(exc)


@router.patch("/api/v1/admin/lookups/mukim/{mukim_id}", response_model=LookupMukimResponse, response_model_exclude_none=True)
def update_lookup_mukim(
    mukim_id: int,
    request: LookupMukimUpdateRequest,
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.update_mukim(
            mukim_id=mukim_id,
            mukim_name=request.mukim_name,
            mukim_code=request.mukim_code,
            district_id=request.district_id,
        )
    except ServiceError as exc:
        _raise_service_error(exc)


@router.get("/api/v1/admin/lookups/localities", response_model=LookupLocalityListResponse, response_model_exclude_none=True)
def list_lookup_localities(
    mukim_id: int | None = Query(default=None),
    district_id: int | None = Query(default=None),
    state_id: int | None = Query(default=None),
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.list_localities(mukim_id=mukim_id, district_id=district_id, state_id=state_id)
    except ServiceError as exc:
        _raise_service_error(exc)


@router.get("/api/v1/admin/lookups/localities/{locality_id}", response_model=LookupLocalityResponse, response_model_exclude_none=True)
def get_lookup_locality(
    locality_id: int,
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.get_locality(locality_id=locality_id)
    except ServiceError as exc:
        _raise_service_error(exc)


@router.post("/api/v1/admin/lookups/localities", response_model=LookupLocalityResponse, response_model_exclude_none=True)
def create_lookup_locality(
    request: LookupLocalityCreateRequest,
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.create_locality(
            locality_name=request.locality_name,
            locality_code=request.locality_code,
            mukim_id=request.mukim_id,
        )
    except ServiceError as exc:
        _raise_service_error(exc)


@router.patch("/api/v1/admin/lookups/localities/{locality_id}", response_model=LookupLocalityResponse, response_model_exclude_none=True)
def update_lookup_locality(
    locality_id: int,
    request: LookupLocalityUpdateRequest,
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.update_locality(
            locality_id=locality_id,
            locality_name=request.locality_name,
            locality_code=request.locality_code,
            mukim_id=request.mukim_id,
        )
    except ServiceError as exc:
        _raise_service_error(exc)


@router.get("/api/v1/admin/lookups/postcodes", response_model=LookupPostcodeListResponse, response_model_exclude_none=True)
def list_lookup_postcodes(
    locality_id: int | None = Query(default=None),
    mukim_id: int | None = Query(default=None),
    district_id: int | None = Query(default=None),
    state_id: int | None = Query(default=None),
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.list_postcodes(
            locality_id=locality_id,
            mukim_id=mukim_id,
            district_id=district_id,
            state_id=state_id,
        )
    except ServiceError as exc:
        _raise_service_error(exc)


@router.get("/api/v1/admin/lookups/postcodes/{postcode_id}", response_model=LookupPostcodeResponse, response_model_exclude_none=True)
def get_lookup_postcode(
    postcode_id: int,
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.get_postcode(postcode_id=postcode_id)
    except ServiceError as exc:
        _raise_service_error(exc)


@router.post("/api/v1/admin/lookups/postcodes", response_model=LookupPostcodeResponse, response_model_exclude_none=True)
def create_lookup_postcode(
    request: LookupPostcodeCreateRequest,
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.create_postcode(
            postcode_name=request.postcode_name,
            postcode=request.postcode,
            locality_id=request.locality_id,
        )
    except ServiceError as exc:
        _raise_service_error(exc)


@router.patch("/api/v1/admin/lookups/postcodes/{postcode_id}", response_model=LookupPostcodeResponse, response_model_exclude_none=True)
def update_lookup_postcode(
    postcode_id: int,
    request: LookupPostcodeUpdateRequest,
    _admin: Admin = Depends(get_current_admin),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    try:
        return service.update_postcode(
            postcode_id=postcode_id,
            postcode_name=request.postcode_name,
            postcode=request.postcode,
            locality_id=request.locality_id,
        )
    except ServiceError as exc:
        _raise_service_error(exc)
