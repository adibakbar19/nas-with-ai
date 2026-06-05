"""Response models for ingest API endpoints."""

from pydantic import BaseModel, Field

from .requests import MultipartUploadedPart


class UploadJobResponse(BaseModel):
    job_id: str
    agency_id: str
    status: str
    object_name: str
    content_sha256: str | None = None
    content_bytes: int
    object_reused: bool | None = None
    load_to_db: bool
    idempotent_replay: bool = False


class RetryFailedRowsResponse(BaseModel):
    job_id: str
    agency_id: str
    parent_job_id: str
    status: str
    job_type: str
    object_name: str
    content_sha256: str
    content_bytes: int
    load_to_db: bool
    require_mukim: bool
    idempotent_replay: bool = False


class JobActionResponse(BaseModel):
    job_id: str
    agency_id: str
    status: str
    phase: str | None = None
    idempotent_replay: bool = False


class IngestJobResponse(BaseModel):
    job_id: str
    agency_id: str
    status: str
    job_type: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    file_name: str | None = None
    source_type: str | None = None
    config_path: str | None = None
    object_name: str | None = None
    bucket: str | None = None
    content_sha256: str | None = None
    content_bytes: int | None = None
    object_reused: bool | None = None
    success_path: str | None = None
    warning_path: str | None = None
    failed_path: str | None = None
    checkpoint_root: str | None = None
    load_to_db: bool | None = None
    load_status: str | None = None
    progress_stage: str | None = None
    error: str | None = None
    parent_job_id: str | None = None
    require_mukim: bool | None = None
    success_count: int | None = None
    warning_count: int | None = None
    failed_count: int | None = None

    model_config = {"extra": "allow"}


class IngestJobListResponse(BaseModel):
    count: int
    items: list[IngestJobResponse]


class MultipartInitiateResponse(BaseModel):
    session_id: str
    agency_id: str
    job_id: str
    bucket: str
    object_name: str
    upload_id: str
    part_size: int
    expires_in: int
    idempotent_replay: bool = False


class MultipartPartUrlResponse(BaseModel):
    session_id: str
    part_number: int
    url: str
    expires_in: int


class MultipartSessionStatusResponse(BaseModel):
    session_id: str
    agency_id: str
    status: str
    bucket: str
    object_name: str
    upload_id: str
    file_name: str
    content_type: str | None = None
    content_bytes: int
    part_size: int
    job_id: str | None = None
    uploaded_parts: list[MultipartUploadedPart] = Field(default_factory=list)


class MultipartCompleteResponse(BaseModel):
    job_id: str
    agency_id: str | None = None
    status: str
    session_id: str
    object_name: str | None = None
    content_sha256: str | None = None
    content_bytes: int | None = None
    object_reused: bool | None = None
    load_to_db: bool | None = None
    idempotent_replay: bool = False


class MultipartAbortResponse(BaseModel):
    session_id: str
    status: str
