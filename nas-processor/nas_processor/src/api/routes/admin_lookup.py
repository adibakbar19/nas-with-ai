"""Admin lookup CRUD routes — requires platform_admin realm role."""

from typing import Any

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel

from nas_processor.src.api.auth import Agency, get_current_agency, require_platform_admin
from nas_processor.src.api.services.lookup_admin_service import LookupAdminService

router = APIRouter(tags=["admin-lookup"])


def get_lookup_admin_service(request: Request) -> LookupAdminService:
    return request.app.state.lookup_admin_service


# ── Request models ───────────────────────────────────────────────────────────

class DistrictCreateRequest(BaseModel):
    district_name: str
    district_code: str
    state_id: int

class DistrictUpdateRequest(BaseModel):
    district_name: str | None = None
    district_code: str | None = None
    state_id: int | None = None

class MukimCreateRequest(BaseModel):
    mukim_name: str
    mukim_code: str
    district_id: int

class MukimUpdateRequest(BaseModel):
    mukim_name: str | None = None
    mukim_code: str | None = None
    district_id: int | None = None

class LocalityCreateRequest(BaseModel):
    locality_name: str
    locality_code: str | None = None
    mukim_id: int | None = None

class LocalityUpdateRequest(BaseModel):
    locality_name: str | None = None
    locality_code: str | None = None
    mukim_id: int | None = None

class PostcodeCreateRequest(BaseModel):
    postcode_name: str
    postcode: str
    locality_id: int | None = None

class PostcodeUpdateRequest(BaseModel):
    postcode_name: str | None = None
    postcode: str | None = None
    locality_id: int | None = None


# ── Routes ───────────────────────────────────────────────────────────────────

@router.get("/api/v1/admin/lookups/states")
async def list_states(
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.list_states()


@router.get("/api/v1/admin/lookups/districts")
async def list_districts(
    state_id: int | None = Query(default=None),
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.list_districts(state_id=state_id)


@router.get("/api/v1/admin/lookups/districts/{district_id}")
async def get_district(
    district_id: int,
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.get_district(district_id=district_id)


@router.post("/api/v1/admin/lookups/districts")
async def create_district(
    request: DistrictCreateRequest,
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.create_district(district_name=request.district_name, district_code=request.district_code, state_id=request.state_id)


@router.patch("/api/v1/admin/lookups/districts/{district_id}")
async def update_district(
    district_id: int,
    request: DistrictUpdateRequest,
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.update_district(district_id=district_id, district_name=request.district_name, district_code=request.district_code, state_id=request.state_id)


@router.get("/api/v1/admin/lookups/mukim")
async def list_mukim(
    district_id: int | None = Query(default=None),
    state_id: int | None = Query(default=None),
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.list_mukim(district_id=district_id, state_id=state_id)


@router.get("/api/v1/admin/lookups/mukim/{mukim_id}")
async def get_mukim(
    mukim_id: int,
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.get_mukim(mukim_id=mukim_id)


@router.post("/api/v1/admin/lookups/mukim")
async def create_mukim(
    request: MukimCreateRequest,
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.create_mukim(mukim_name=request.mukim_name, mukim_code=request.mukim_code, district_id=request.district_id)


@router.patch("/api/v1/admin/lookups/mukim/{mukim_id}")
async def update_mukim(
    mukim_id: int,
    request: MukimUpdateRequest,
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.update_mukim(mukim_id=mukim_id, mukim_name=request.mukim_name, mukim_code=request.mukim_code, district_id=request.district_id)


@router.get("/api/v1/admin/lookups/localities")
async def list_localities(
    mukim_id: int | None = Query(default=None),
    district_id: int | None = Query(default=None),
    state_id: int | None = Query(default=None),
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.list_localities(mukim_id=mukim_id, district_id=district_id, state_id=state_id)


@router.get("/api/v1/admin/lookups/localities/{locality_id}")
async def get_locality(
    locality_id: int,
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.get_locality(locality_id=locality_id)


@router.post("/api/v1/admin/lookups/localities")
async def create_locality(
    request: LocalityCreateRequest,
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.create_locality(locality_name=request.locality_name, locality_code=request.locality_code, mukim_id=request.mukim_id)


@router.patch("/api/v1/admin/lookups/localities/{locality_id}")
async def update_locality(
    locality_id: int,
    request: LocalityUpdateRequest,
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.update_locality(locality_id=locality_id, locality_name=request.locality_name, locality_code=request.locality_code, mukim_id=request.mukim_id)


@router.get("/api/v1/admin/lookups/postcodes")
async def list_postcodes(
    locality_id: int | None = Query(default=None),
    mukim_id: int | None = Query(default=None),
    district_id: int | None = Query(default=None),
    state_id: int | None = Query(default=None),
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.list_postcodes(locality_id=locality_id, mukim_id=mukim_id, district_id=district_id, state_id=state_id)


@router.get("/api/v1/admin/lookups/postcodes/{postcode_id}")
async def get_postcode(
    postcode_id: int,
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.get_postcode(postcode_id=postcode_id)


@router.post("/api/v1/admin/lookups/postcodes")
async def create_postcode(
    request: PostcodeCreateRequest,
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.create_postcode(postcode_name=request.postcode_name, postcode=request.postcode, locality_id=request.locality_id)


@router.patch("/api/v1/admin/lookups/postcodes/{postcode_id}")
async def update_postcode(
    postcode_id: int,
    request: PostcodeUpdateRequest,
    agency: Agency = Depends(get_current_agency),
    service: LookupAdminService = Depends(get_lookup_admin_service),
) -> dict[str, Any]:
    require_platform_admin(agency)
    return service.update_postcode(postcode_id=postcode_id, postcode_name=request.postcode_name, postcode=request.postcode, locality_id=request.locality_id)
