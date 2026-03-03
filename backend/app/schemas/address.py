from typing import Any
from pydantic import BaseModel, Field


class AddressSearchItem(BaseModel):
    record_id: str | None = None
    naskod: str | None = None
    address_clean: str | None = None
    postcode: str | None = None
    locality_name: str | None = None
    district_name: str | None = None
    state_name: str | None = None
    confidence_score: float | None = None
    score: float | None = Field(default=None, description="Elasticsearch score")


class AddressSearchResponse(BaseModel):
    query: str
    size: int
    count: int
    items: list[AddressSearchItem]


class ValidateAddressRequest(BaseModel):
    address_id: str | None = None
    raw_address: str = Field(min_length=3)
    postcode: str | None = None
    latitude: float | None = None
    longitude: float | None = None


class ValidateAddressResponse(BaseModel):
    normalized_address: str
    confidence_score: int
    is_valid: bool
    validation_reasons: list[str]
    spatial_match: bool | None = None
    canonical_record: dict[str, Any] | None = None
