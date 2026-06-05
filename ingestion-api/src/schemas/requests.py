"""Request models for ingest API endpoints."""

from pydantic import BaseModel, Field


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
