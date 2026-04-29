from functools import lru_cache

from backend.app.core.settings import AppSettings, get_settings
from backend.app.queue.producer import QueueProducer, ValkeyStreamQueueProducer
from backend.app.repositories.address_read_repository import AddressReadRepository
from backend.app.repositories.address_match_review_repository import AddressMatchReviewRepository
from backend.app.repositories.boundary_admin_repository import BoundaryAdminRepository
from backend.app.repositories.lookup_admin_repository import LookupAdminRepository
from backend.app.services.agency_api_key_service import AgencyApiKeyService
from backend.app.services.address_match_review_service import AddressMatchReviewService
from backend.app.services.address_parse_service import AddressParseService
from backend.app.services.address_read_service import AddressReadService
from backend.app.services.auth_service import AuthService
from backend.app.services.agency_user_service import AgencyUserService
from backend.app.services.boundary_admin_service import BoundaryAdminService
from backend.app.services.ingest_service import IngestService
from backend.app.services.lookup_admin_service import LookupAdminService
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
def get_address_match_review_repository() -> AddressMatchReviewRepository:
    settings = get_settings()
    return AddressMatchReviewRepository(dsn=settings.postgres_dsn, schema=settings.postgres_schema)


@lru_cache(maxsize=1)
def get_address_match_review_service() -> AddressMatchReviewService:
    return AddressMatchReviewService(repository=get_address_match_review_repository())


@lru_cache(maxsize=1)
def get_address_parse_service() -> AddressParseService:
    return AddressParseService()


@lru_cache(maxsize=1)
def get_boundary_admin_repository() -> BoundaryAdminRepository:
    settings = get_settings()
    return BoundaryAdminRepository(
        dsn=settings.postgres_dsn,
        lookup_schema=settings.lookup_schema,
    )


@lru_cache(maxsize=1)
def get_boundary_admin_service() -> BoundaryAdminService:
    settings = get_settings()
    return BoundaryAdminService(
        repository=get_boundary_admin_repository(),
        lookup_schema=settings.lookup_schema,
    )


@lru_cache(maxsize=1)
def get_lookup_admin_repository() -> LookupAdminRepository:
    settings = get_settings()
    return LookupAdminRepository(
        dsn=settings.postgres_dsn,
        lookup_schema=settings.lookup_schema,
        runtime_schema=settings.postgres_schema,
    )


@lru_cache(maxsize=1)
def get_lookup_admin_service() -> LookupAdminService:
    settings = get_settings()
    return LookupAdminService(
        repository=get_lookup_admin_repository(),
        lookup_schema=settings.lookup_schema,
        runtime_schema=settings.postgres_schema,
    )


@lru_cache(maxsize=1)
def get_queue_producer() -> QueueProducer:
    settings: AppSettings = get_settings()
    return ValkeyStreamQueueProducer(
        valkey_url=settings.valkey_url,
        stream_key=settings.valkey_stream_key,
    )


@lru_cache(maxsize=1)
def get_ingest_service() -> IngestService:
    return IngestService()


@lru_cache(maxsize=1)
def get_search_api_service() -> SearchApiService:
    settings = get_settings()
    return SearchApiService(es_url=settings.es_url, es_index=settings.es_index)


@lru_cache(maxsize=1)
def get_ops_service() -> OpsService:
    return OpsService()


@lru_cache(maxsize=1)
def get_agency_api_key_service() -> AgencyApiKeyService:
    return AgencyApiKeyService()


@lru_cache(maxsize=1)
def get_auth_service() -> AuthService:
    return AuthService()


@lru_cache(maxsize=1)
def get_agency_user_service() -> AgencyUserService:
    return AgencyUserService()
