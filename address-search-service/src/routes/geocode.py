"""Geocode routes — forward (address → coordinates) and reverse (coordinates → address)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Header, Query

from src.schemas.requests import GeocodeForwardRequest
from src.schemas.responses import GeocodeForwardResult, GeocodeReverseResult
from src.services.geocode_service import geocode_forward, geocode_reverse
from config import settings

router = APIRouter(tags=["geocode"])


def verify_api_key(
    api_key: str | None = Header(alias="X-API-Key", default=None),
) -> str:
    if not settings.auth_enabled:
        return "anonymous"
    if api_key is None or api_key not in settings.api_keys:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key


@router.post("/geocode/forward", response_model=GeocodeForwardResult)
def forward(
    body: GeocodeForwardRequest,
    _: str = Depends(verify_api_key),
) -> GeocodeForwardResult:
    return geocode_forward(
        address=body.address,
        postcode=body.postcode,
        state_code=body.state_code,
    )


@router.get("/geocode/reverse", response_model=GeocodeReverseResult)
def reverse(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    radius_m: int = Query(default=100, ge=1, le=5000),
    _: str = Depends(verify_api_key),
) -> GeocodeReverseResult:
    return geocode_reverse(lat=lat, lon=lon, radius_m=radius_m)
