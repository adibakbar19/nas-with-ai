from typing import Any

from fastapi import APIRouter, Depends, Query

from backend.app.dependencies import get_search_api_service
from backend.app.services.search_api_service import SearchApiService

router = APIRouter(tags=["search"])


@router.get("/api/v1/search/autocomplete")
def autocomplete(
    q: str = Query(..., min_length=2),
    size: int = Query(10, ge=1, le=50),
    search_service: SearchApiService = Depends(get_search_api_service),
) -> dict[str, Any]:
    return search_service.autocomplete(query=q, size=size)
