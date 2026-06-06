"""Spatial query routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Header, Query

from src.schemas.responses import SpatialResult
from src.services.spatial_service import find_in_boundary, find_nearby
from config import settings

router = APIRouter(tags=["spatial"])


def verify_api_key(
    api_key: str | None = Header(alias="X-API-Key", default=None),
) -> str:
    if not settings.auth_enabled:
        return "anonymous"
    if api_key is None or api_key not in settings.api_keys:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key


@router.get("/spatial/nearby", response_model=SpatialResult)
def nearby(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    radius_m: int = Query(default=500, ge=1, le=5000),
    limit: int = Query(default=10, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    _: str = Depends(verify_api_key),
) -> dict:
    radius_m = min(radius_m, settings.max_radius_m)
    results, total, elapsed_ms = find_nearby(lat, lon, radius_m=radius_m, limit=limit, offset=offset)
    return {
        "results": results,
        "total": total,
        "limit": limit,
        "offset": offset,
        "query_time_ms": elapsed_ms,
    }


@router.get("/spatial/boundary", response_model=SpatialResult)
def boundary(
    boundary_type: str = Query(..., description="mukim / pbt / district / state"),
    boundary_id: str = Query(...),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    _: str = Depends(verify_api_key),
) -> dict:
    valid_types = {"mukim", "pbt", "district", "state"}
    if boundary_type.lower() not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"boundary_type must be one of {sorted(valid_types)}",
        )
    results, total, elapsed_ms = find_in_boundary(
        boundary_type=boundary_type,
        boundary_id=boundary_id,
        limit=limit,
        offset=offset,
    )
    return {
        "results": results,
        "total": total,
        "limit": limit,
        "offset": offset,
        "query_time_ms": elapsed_ms,
    }
