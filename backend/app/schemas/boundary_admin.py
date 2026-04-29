from pydantic import BaseModel, Field


class BoundaryUploadVersionResponse(BaseModel):
    version_id: int
    boundary_type: str
    version_label: str
    status: str
    row_count: int
    uploaded_by: str | None = None
    activated_by: str | None = None
    source_note: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    activated_at: str | None = None
    lookup_schema: str | None = None
    active_in_lookup: bool | None = None


class BoundaryUploadVersionListResponse(BaseModel):
    lookup_schema: str
    boundary_type: str | None = None
    status: str | None = None
    count: int
    items: list[BoundaryUploadVersionResponse]


class BoundaryUploadCreateResponse(BoundaryUploadVersionResponse):
    upload_filename: str | None = None


class BoundaryActivateRequest(BaseModel):
    activation_note: str | None = Field(default=None, examples=["Activate revised Kuala Lumpur district boundary set"])
