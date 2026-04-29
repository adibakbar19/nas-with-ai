from typing import Any

from backend.app.search.query import autocomplete
from backend.app.services.errors import ServiceError


class SearchApiService:
    def __init__(self, *, es_url: str, es_index: str) -> None:
        self._es_url = es_url
        self._es_index = es_index

    def autocomplete(self, *, query: str, size: int) -> dict[str, Any]:
        try:
            hits = autocomplete(es_url=self._es_url, index=self._es_index, query=query, size=size)
        except Exception as exc:
            raise ServiceError(status_code=502, detail=f"search query failed: {exc}") from exc
        return {"query": query, "size": size, "count": len(hits), "items": hits}
