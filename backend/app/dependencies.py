from functools import lru_cache

from backend.app.core.settings import AppSettings, get_settings
from backend.app.queue.producer import LoggingQueueProducer, QueueProducer, SQSQueueProducer
from backend.app.repositories.address_repository import PostgresAddressRepository
from backend.app.search.elasticsearch_gateway import ElasticsearchSearchGateway
from backend.app.services.address_service import AddressService
from backend.app.services.ingest_service import IngestService
from backend.app.services.ops_service import OpsService
from backend.app.services.search_api_service import SearchApiService


@lru_cache(maxsize=1)
def get_search_gateway() -> ElasticsearchSearchGateway:
    settings = get_settings()
    return ElasticsearchSearchGateway(es_url=settings.es_url, index=settings.es_index)


@lru_cache(maxsize=1)
def get_repository() -> PostgresAddressRepository:
    settings = get_settings()
    return PostgresAddressRepository(
        dsn=settings.postgres_dsn,
        schema=settings.postgres_schema,
        postcode_boundary_table=settings.postcode_boundary_table,
    )


@lru_cache(maxsize=1)
def get_address_service() -> AddressService:
    return AddressService(repository=get_repository(), search_gateway=get_search_gateway())


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
