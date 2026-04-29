from typing import Any

from backend.app.search.indexer import index_documents
from backend.app.search.postgres_source import iter_search_documents


class SearchSyncService:
    def __init__(self, *, postgres_dsn: str, postgres_schema: str, es_url: str, es_index: str) -> None:
        self._postgres_dsn = postgres_dsn
        self._postgres_schema = postgres_schema
        self._es_url = es_url
        self._es_index = es_index

    def sync_from_postgres(
        self,
        *,
        schema_name: str | None = None,
        es_index: str | None = None,
        recreate_index: bool = False,
        batch_size: int = 1000,
        timeout_seconds: int = 60,
    ) -> dict[str, Any]:
        schema = (schema_name or self._postgres_schema).strip() or self._postgres_schema
        index = (es_index or self._es_index).strip() or self._es_index
        docs = iter_search_documents(
            db_url=self._postgres_dsn,
            schema=schema,
            batch_size=batch_size,
        )
        return index_documents(
            docs,
            es_url=self._es_url,
            index=index,
            batch_size=batch_size,
            recreate_index=recreate_index,
            timeout_seconds=timeout_seconds,
        )
