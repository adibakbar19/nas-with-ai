from typing import Any

from nas_processor.src.api.search.query import search_address
from nas_processor.src.api.errors import ServiceError


class SearchApiService:
    def __init__(self, *, es_url: str, es_index: str) -> None:
        self._es_url = es_url
        self._es_index = es_index

    def search(self, *, query: str, size: int) -> dict[str, Any]:
        try:
            hits = search_address(es_url=self._es_url, index=self._es_index, query=query, size=size)
        except Exception as exc:
            raise ServiceError(status_code=502, detail=f"search query failed: {exc}") from exc
        return {"query": query, "size": size, "count": len(hits), "items": hits}
