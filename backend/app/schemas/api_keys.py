from pydantic import BaseModel, Field


class AgencyApiKeyCreateRequest(BaseModel):
    label: str | None = Field(default=None, examples=["JPN production integration"])
    expires_at: str | None = Field(default=None, examples=["2026-12-31T23:59:59+00:00"])


class AgencyApiKeyRotateRequest(BaseModel):
    label: str | None = Field(default=None, examples=["JPN production integration"])
    expires_at: str | None = Field(default=None, examples=["2026-12-31T23:59:59+00:00"])
    revoked_reason: str | None = Field(default=None, examples=["routine quarterly rotation"])


class AgencyApiKeyRevokeRequest(BaseModel):
    revoked_reason: str | None = Field(default=None, examples=["integration retired"])


class AgencyApiKeyMetadataResponse(BaseModel):
    key_id: str
    agency_id: str
    label: str | None = None
    secret_preview: str
    status: str
    created_by: str | None = None
    revoked_reason: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    last_used_at: str | None = None
    expires_at: str | None = None
    revoked_at: str | None = None


class AgencyApiKeyCreateResponse(AgencyApiKeyMetadataResponse):
    api_key: str
    rotated_from_key_id: str | None = None


class AgencyApiKeyListResponse(BaseModel):
    count: int
    items: list[AgencyApiKeyMetadataResponse]


class AgencyApiKeyRevokeResponse(BaseModel):
    key_id: str
    agency_id: str | None = None
    status: str
    revoked_at: str | None = None
