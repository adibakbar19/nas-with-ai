from typing import Any


class SearchApiService:
    @staticmethod
    def _runtime():
        from backend.app import main as runtime

        return runtime

    def autocomplete(self, *, query: str, size: int) -> dict[str, Any]:
        runtime = self._runtime()
        hits = runtime._autocomplete(query, size)
        return {"query": query, "size": size, "count": len(hits), "items": hits}
