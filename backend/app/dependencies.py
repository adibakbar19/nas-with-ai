from functools import lru_cache

from backend.app.core.settings import AppSettings, get_settings
from backend.app.queue.producer import LoggingQueueProducer, QueueProducer, SQSQueueProducer
from backend.app.repositories.address_read_repository import AddressReadRepository
from backend.app.services.address_read_service import AddressReadService
from backend.app.services.ingest_service import IngestService
from backend.app.services.ops_service import OpsService
from backend.app.services.search_api_service import SearchApiService


@lru_cache(maxsize=1)
def get_address_read_repository() -> AddressReadRepository:
    settings = get_settings()
    return AddressReadRepository(dsn=settings.postgres_dsn, schema=settings.postgres_schema)


@lru_cache(maxsize=1)
def get_address_read_service() -> AddressReadService:
    return AddressReadService(repository=get_address_read_repository())


@lru_cache(maxsize=1)
def get_queue_producer() -> QueueProducer:
    settings: AppSettings = get_settings()
    if settings.queue_backend == "sqs":
        return SQSQueueProducer(queue_url=settings.sqs_queue_url, region=settings.aws_region)
    return LoggingQueueProducer(event_log_path=settings.queue_event_log)


@lru_cache(maxsize=1)
def get_ingest_service() -> IngestService:
    return IngestService()


@lru_cache(maxsize=1)
def get_search_api_service() -> SearchApiService:
    return SearchApiService()


@lru_cache(maxsize=1)
def get_ops_service() -> OpsService:
    return OpsService()
