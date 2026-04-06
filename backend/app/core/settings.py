from functools import lru_cache
import os
from pydantic import BaseModel, Field


class AppSettings(BaseModel):
    postgres_dsn: str = Field(default="postgresql://postgres:postgres@localhost:5432/postgres")
    postgres_schema: str = Field(default="nas")
    postcode_boundary_table: str = Field(default="postcode_boundary")

    es_url: str = Field(default="http://localhost:9200")
    es_index: str = Field(default="nas_addresses")

    queue_backend: str = Field(default="redis_stream")
    queue_event_log: str = Field(default="logs/queue/bulk_ingest_events.jsonl")
    sqs_queue_url: str = Field(default="")
    aws_region: str = Field(default="ap-southeast-5")
    redis_url: str = Field(default="redis://localhost:6379/0")
    redis_stream_key: str = Field(default="bulk_ingest_events")
    redis_stream_group: str = Field(default="bulk_ingest_workers")
    redis_stream_block_ms: int = Field(default=5000)


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
        postcode_boundary_table=os.getenv("POSTCODE_BOUNDARY_TABLE", "postcode_boundary"),
        es_url=os.getenv("ES_URL", "http://localhost:9200").rstrip("/"),
        es_index=os.getenv("ES_INDEX", "nas_addresses"),
        queue_backend=os.getenv("QUEUE_BACKEND", "redis_stream").lower(),
        queue_event_log=os.getenv("QUEUE_EVENT_LOG", "logs/queue/bulk_ingest_events.jsonl"),
        sqs_queue_url=os.getenv("SQS_QUEUE_URL", ""),
        aws_region=os.getenv("AWS_REGION", "ap-southeast-5"),
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        redis_stream_key=os.getenv("REDIS_STREAM_KEY", "bulk_ingest_events"),
        redis_stream_group=os.getenv("REDIS_STREAM_GROUP", "bulk_ingest_workers"),
        redis_stream_block_ms=max(1000, int(os.getenv("REDIS_STREAM_BLOCK_MS", "5000"))),
    )
