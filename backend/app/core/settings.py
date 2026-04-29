from functools import lru_cache
import os
from pydantic import BaseModel, Field


class AppSettings(BaseModel):
    postgres_dsn: str = Field(default="postgresql://postgres:postgres@localhost:5432/postgres")
    postgres_schema: str = Field(default="nas")
    lookup_schema: str = Field(default="nas_lookup")
    postcode_boundary_table: str = Field(default="postcode_boundary")

    es_url: str = Field(default="http://localhost:9200")
    es_index: str = Field(default="nas_addresses")

    valkey_url: str = Field(default="redis://localhost:6379/0")
    valkey_stream_key: str = Field(default="bulk_ingest_events")
    valkey_stream_group: str = Field(default="bulk_ingest_workers")
    valkey_stream_block_ms: int = Field(default=5000)


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    return AppSettings(
        postgres_dsn=os.getenv(
            "POSTGRES_DSN",
            "postgresql://"
            f"{os.getenv('PGUSER', 'postgres')}:{os.getenv('PGPASSWORD', 'postgres')}"
            f"@{os.getenv('PGHOST', 'localhost')}:{os.getenv('PGPORT', '5432')}"
            f"/{os.getenv('PGDATABASE', 'postgres')}",
        ),
        postgres_schema=os.getenv("PGSCHEMA", "nas"),
        lookup_schema=os.getenv("LOOKUP_SCHEMA", "nas_lookup"),
        postcode_boundary_table=os.getenv("POSTCODE_BOUNDARY_TABLE", "postcode_boundary"),
        es_url=os.getenv("ES_URL", "http://localhost:9200").rstrip("/"),
        es_index=os.getenv("ES_INDEX", "nas_addresses"),
        valkey_url=os.getenv("VALKEY_URL", "redis://localhost:6379/0"),
        valkey_stream_key=os.getenv("VALKEY_STREAM_KEY", "bulk_ingest_events"),
        valkey_stream_group=os.getenv("VALKEY_STREAM_GROUP", "bulk_ingest_workers"),
        valkey_stream_block_ms=max(1000, int(os.getenv("VALKEY_STREAM_BLOCK_MS", "5000"))),
    )
