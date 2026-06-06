"""Request body schemas."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ValidateRequest(BaseModel):
    address: str
    postcode: str | None = None
    state_code: str | None = None


class BatchValidateRequest(BaseModel):
    addresses: list[str] = Field(..., max_length=100)


class GeocodeForwardRequest(BaseModel):
    address: str
    postcode: str | None = None
    state_code: str | None = None
