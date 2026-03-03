from datetime import datetime, timezone
import uuid
from pydantic import BaseModel, Field


class BulkIngestRequest(BaseModel):
    object_name: str = Field(min_length=1)
    bucket: str = Field(min_length=1)
    file_name: str | None = None
    source_type: str = Field(default="csv")
    config_path: str = Field(default="config/config.json")
    load_to_db: bool = Field(default=True)
    success_path: str | None = None
    failed_path: str | None = None
    checkpoint_root: str | None = None
    resume_from_checkpoint: bool = Field(default=True)
    resume_failed_only: bool = Field(default=True)


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
    failed_path: str | None = None
    checkpoint_root: str | None = None
    resume_from_checkpoint: bool = Field(default=True)
    resume_failed_only: bool = Field(default=True)


class AcceptedJobResponse(BaseModel):
    job_id: str
    status: str
    message: str
    status_url: str
