"""Search, autocomplete, and suggest routes."""

from __future__ import annotations

from fastapi import APIRouter, Query

from src.schemas.responses import AutocompleteResponse, SearchResponse, SuggestResponse
from src.services.search_service import (
    autocomplete_addresses,
    search_addresses,
    suggest_addresses,
)
from config import settings

router = APIRouter(tags=["search"])


@router.get("/search", response_model=SearchResponse)
def search(
    q: str | None = Query(default=None, description="Free-text address search"),
    postcode: str | None = Query(default=None),
    state_code: str | None = Query(default=None),
    address_type: str | None = Query(default=None),
    confidence_band: str | None = Query(default=None),
    lifecycle_status: str | None = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> dict:
    results, total, elapsed_ms = search_addresses(
        q=q,
        postcode=postcode,
        state_code=state_code,
        address_type=address_type,
        confidence_band=confidence_band,
        lifecycle_status=lifecycle_status,
        limit=min(limit, settings.max_limit),
        offset=offset,
    )
    return {
        "results": results,
        "total": total,
        "limit": limit,
        "offset": offset,
        "query_time_ms": elapsed_ms,
    }


@router.get("/autocomplete", response_model=AutocompleteResponse)
def autocomplete(
    q: str = Query(..., min_length=1, description="Search prefix"),
    state_code: str | None = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100),
) -> dict:
    suggestions, elapsed_ms = autocomplete_addresses(
        q=q,
        state_code=state_code,
        limit=min(limit, settings.max_limit),
    )
    return {"suggestions": suggestions, "query_time_ms": elapsed_ms}


@router.get("/suggest", response_model=SuggestResponse)
def suggest(
    address: str = Query(..., min_length=1, description="Address to find suggestions for"),
    limit: int = Query(default=5, ge=1, le=20),
) -> dict:
    suggestions, elapsed_ms = suggest_addresses(address=address, limit=limit)
    return {"suggestions": suggestions, "query_time_ms": elapsed_ms}
