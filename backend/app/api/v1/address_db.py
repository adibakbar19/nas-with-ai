from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.app.dependencies import get_address_read_service
from backend.app.services.address_read_service import AddressReadService
from backend.app.services.errors import ServiceError


router = APIRouter(tags=["address-db"])


@router.get("/api/v1/db/addresses")
def search_db_addresses(
    q: str | None = Query(default=None, description="Query by address_id, naskod, postcode or address text"),
    limit: int = Query(20, ge=1, le=100),
    service: AddressReadService = Depends(get_address_read_service),
) -> dict[str, Any]:
    try:
        return service.search(query=q, limit=limit)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/api/v1/db/addresses/{address_id}")
def get_db_address(
    address_id: int,
    service: AddressReadService = Depends(get_address_read_service),
) -> dict[str, Any]:
    try:
        return service.get_address(address_id=address_id)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/api/v1/db/naskod/{code}")
def get_db_address_by_naskod(
    code: str,
    service: AddressReadService = Depends(get_address_read_service),
) -> dict[str, Any]:
    try:
        return service.get_by_naskod(naskod=code)
    except ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
