"""Address read routes — public endpoints, no auth required."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request

from nas_processor.src.api.services.address_service import AddressReadService

router = APIRouter(tags=["address"])


def _svc(request: Request) -> AddressReadService:
    return request.app.state.address_service


# ── New clean routes (/addresses/...) ─────────────────────────────────────────
# IMPORTANT: specific paths (naskod/{naskod}) must be registered before
# the generic {record_id} catch-all to prevent FastAPI capturing "naskod" as record_id.

@router.get("/addresses")
def search_addresses(
    request: Request,
    q: str | None = Query(default=None, description="Free-text search on address_clean"),
    postcode: str | None = Query(default=None),
    state_code: str | None = Query(default=None),
    mukim_id: str | None = Query(default=None),
    pbt_id: str | None = Query(default=None),
    locality_name: str | None = Query(default=None),
    confidence_band: str | None = Query(default=None, description="HIGH / MEDIUM / LOW / FAILED"),
    lifecycle_status: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    return _svc(request).search_addresses(
        q=q, postcode=postcode, state_code=state_code,
        mukim_id=mukim_id, pbt_id=pbt_id, locality_name=locality_name,
        confidence_band=confidence_band, lifecycle_status=lifecycle_status,
        limit=limit, offset=offset,
    )


@router.get("/addresses/naskod/{naskod}")
def get_by_naskod(naskod: str, request: Request) -> dict[str, Any]:
    return _svc(request).get_naskod_detail(naskod)


@router.get("/addresses/{record_id}/raw-history")
def get_raw_history(record_id: str, request: Request) -> list[dict[str, Any]]:
    return _svc(request).get_raw_history(record_id)


@router.get("/addresses/{record_id}/aliases")
def get_aliases(record_id: str, request: Request) -> list[dict[str, Any]]:
    return _svc(request).get_aliases(record_id)


@router.get("/addresses/{record_id}")
def get_address(record_id: str, request: Request) -> dict[str, Any]:
    return _svc(request).get_address_detail(record_id)


# ── Legacy routes (/api/v1/db/...) — kept for backward compatibility ──────────

@router.get("/api/v1/db/addresses")
def list_addresses_legacy(
    request: Request,
    q: str | None = Query(default=None, description="Query by record_id, naskod, postcode or address text"),
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    return _svc(request).search(query=q, limit=limit)


@router.get("/api/v1/db/addresses/{record_id}")
def get_address_legacy(record_id: str, request: Request) -> dict[str, Any]:
    return _svc(request).get_address(record_id=record_id)


@router.get("/api/v1/db/naskod/{code}")
def get_by_naskod_legacy(code: str, request: Request) -> dict[str, Any]:
    return _svc(request).get_by_naskod(naskod=code)
