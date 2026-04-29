from datetime import datetime, timezone
import uuid

from pydantic import BaseModel, Field


class BulkIngestEvent(BaseModel):
    event_type: str = Field(default="bulk_ingest_requested")
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    job_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    requested_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    object_name: str
    bucket: str
    file_name: str | None = None
    source_type: str
    config_path: str
    load_to_db: bool
    success_path: str | None = None
    warning_path: str | None = None
    failed_path: str | None = None
    checkpoint_root: str | None = None
    resume_from_checkpoint: bool = Field(default=True)
    resume_failed_only: bool = Field(default=True)


class SearchSyncEvent(BaseModel):
    event_type: str = Field(default="search_sync_requested")
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    job_id: str = Field(min_length=1)
    requested_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    schema_name: str = Field(default="nas")
    es_index: str = Field(default="nas_addresses")
    recreate_index: bool = Field(default=False)


class AddressParseRequest(BaseModel):
    address: str | None = Field(default=None, examples=["No 12, Jln Ampang, 50450 Kuala Lumpur"])
    addresses: list[str] | None = Field(default=None)
    text: str | None = Field(default=None, description="Line-separated addresses.")
    csv_text: str | None = Field(default=None, description="CSV payload containing an address column.")
    csv_address_column: str | None = Field(default=None)
    use_ai: bool = Field(default=True)
    require_mukim: bool = Field(default=True)
    ai_min_confidence: float = Field(default=0.85, ge=0.0, le=1.0)
    max_records: int = Field(default=100, ge=1, le=1000)


class AddressParseItem(BaseModel):
    record_id: str | None = None
    source_address: str | None = None
    standardized_address: dict
    confidence_score: float
    confidence_band: str | None = None
    matched: bool
    match_status: str
    visual_indicator: str
    reason_codes: list[str] = Field(default_factory=list)
    correction_notes: list[str] = Field(default_factory=list)
    parser_mode: str | None = None
    ai_model: str | None = None
    ai_reason: str | None = None


class AddressParseResponse(BaseModel):
    count: int
    ai_attempted: bool
    ai_model: str | None = None
    counts: dict[str, int]
    items: list[AddressParseItem]


class MultipartUploadInitiateRequest(BaseModel):
    file_name: str = Field(min_length=1, examples=["agency-batch-2026-04.csv"])
    content_bytes: int = Field(gt=0, examples=[73400320])
    content_type: str | None = Field(default=None, examples=["text/csv"])
    auto_start: bool = Field(default=True)
    resume_from_checkpoint: bool = Field(default=True)
    resume_failed_only: bool = Field(default=True)


class MultipartUploadPartUrlRequest(BaseModel):
    part_number: int = Field(ge=1, examples=[1])


class MultipartUploadedPart(BaseModel):
    part_number: int = Field(ge=1, examples=[1])
    etag: str = Field(min_length=1, examples=["9b2cf535f27731c974343645a3985328"])


class MultipartUploadCompleteRequest(BaseModel):
    parts: list[MultipartUploadedPart] = Field(min_length=1)


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

    model_config = {
        "json_schema_extra": {
            "example": {
                "job_id": "f72f7f13ef7940c4b4e5178d4a8ea733",
                "agency_id": "jpn",
                "status": "queued",
                "object_name": "agency/jpn/jobs/f72f7f13ef7940c4b4e5178d4a8ea733/source/original/agency-batch-2026-04.csv",
                "content_sha256": "1c4f0f3ad2f4...",
                "content_bytes": 14203,
                "object_reused": False,
                "load_to_db": True,
                "idempotent_replay": False,
            }
        }
    }


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

    model_config = {
        "json_schema_extra": {
            "example": {
                "job_id": "9a07f4db5668441f9d0d2a1d4d1d8baf",
                "agency_id": "jpn",
                "parent_job_id": "f72f7f13ef7940c4b4e5178d4a8ea733",
                "status": "queued",
                "job_type": "retry_failed_rows",
                "object_name": "agency/jpn/jobs/f72f7f13ef7940c4b4e5178d4a8ea733/retries/9a07f4db5668441f9d0d2a1d4d1d8baf/corrections/corrections.csv",
                "content_sha256": "8db741ca5f0e...",
                "content_bytes": 920,
                "load_to_db": True,
                "require_mukim": False,
                "idempotent_replay": False,
            }
        }
    }


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

    model_config = {
        "json_schema_extra": {
            "example": {
                "session_id": "630da1c02d104718a3ecb10c9b8f7e2f",
                "agency_id": "jpn",
                "job_id": "f72f7f13ef7940c4b4e5178d4a8ea733",
                "bucket": "nas-uploads",
                "object_name": "agency/jpn/jobs/f72f7f13ef7940c4b4e5178d4a8ea733/source/original/agency-batch-2026-04.csv",
                "upload_id": "VXBsb2FkIElE",
                "part_size": 16777216,
                "expires_in": 3600,
                "idempotent_replay": False,
            }
        }
    }


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

    model_config = {
        "extra": "allow",
        "json_schema_extra": {
            "example": {
                "job_id": "f72f7f13ef7940c4b4e5178d4a8ea733",
                "agency_id": "jpn",
                "status": "running",
                "job_type": "bulk_ingest",
                "created_at": "2026-04-10T08:00:00+00:00",
                "updated_at": "2026-04-10T08:01:25+00:00",
                "started_at": "2026-04-10T08:00:05+00:00",
                "file_name": "agency-batch-2026-04.csv",
                "source_type": "csv",
                "object_name": "agency/jpn/jobs/f72f7f13ef7940c4b4e5178d4a8ea733/source/original/agency-batch-2026-04.csv",
                "bucket": "nas-uploads",
                "content_bytes": 14203,
                "warning_path": "output/uploads/f72f7f13ef7940c4b4e5178d4a8ea733/warnings",
                "load_to_db": True,
                "load_status": "pending",
                "progress_stage": "pipeline_running",
            }
        },
    }


class IngestJobListResponse(BaseModel):
    count: int
    items: list[IngestJobResponse]
