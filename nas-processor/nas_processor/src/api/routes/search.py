"""Address search route — public endpoint, no auth required."""

from typing import Any

from fastapi import APIRouter, Depends, Query, Request

from nas_processor.src.api.services.search_service import SearchApiService

router = APIRouter(tags=["search"])


def get_search_service(request: Request) -> SearchApiService:
    return request.app.state.search_service


@router.get("/api/v1/search/address")
def search_address_route(
    q: str = Query(..., min_length=2),
    size: int = Query(10, ge=1, le=50),
    search_service: SearchApiService = Depends(get_search_service),
) -> dict[str, Any]:
    return search_service.search(query=q, size=size)
