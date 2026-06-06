"""Response schemas."""

from __future__ import annotations

from pydantic import BaseModel


class AddressResult(BaseModel):
    record_id: str
    naskod: str | None = None
    address_clean: str | None = None
    street_name: str | None = None
    building_name: str | None = None
    sub_locality_1: str | None = None
    sub_locality_2: str | None = None
    sub_locality_3: str | None = None
    locality_name: str | None = None
    postcode: str | None = None
    postcode_name: str | None = None
    state_code: str | None = None
    state_name: str | None = None
    mukim_name: str | None = None
    district_name: str | None = None
    pbt_name: str | None = None
    address_type: str | None = None
    address_subtype: str | None = None
    confidence_band: str | None = None
    lifecycle_status: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    score: float | None = None
    distance_m: float | None = None


class SearchResponse(BaseModel):
    results: list[AddressResult]
    total: int
    limit: int
    offset: int
    query_time_ms: int


class AutocompleteResponse(BaseModel):
    suggestions: list[str]
    query_time_ms: int


class SuggestResponse(BaseModel):
    suggestions: list[AddressResult]
    query_time_ms: int


class ValidateResult(BaseModel):
    input_address: str
    matched: bool
    match_type: str
    confidence: float
    record_id: str | None = None
    naskod: str | None = None
    address_clean: str | None = None
    postcode: str | None = None
    state_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None


class BatchValidateResponse(BaseModel):
    results: list[ValidateResult]
    total: int
    matched: int
    unmatched: int
    query_time_ms: int


class BoundaryInfo(BaseModel):
    mukim: str | None = None
    mukim_id: str | None = None
    district: str | None = None
    district_id: str | None = None
    state: str | None = None
    state_code: str | None = None
    pbt: str | None = None
    pbt_id: str | None = None


class Coordinates(BaseModel):
    latitude: float
    longitude: float
    crs: str = "WGS84"
    accuracy_metres: int | None = None


class GeocodeForwardResult(BaseModel):
    query: str
    match_type: str
    confidence: float
    coordinates: Coordinates | None = None
    address: AddressResult | None = None
    boundaries: BoundaryInfo | None = None
    query_time_ms: int


class GeocodeReverseResult(BaseModel):
    latitude: float
    longitude: float
    nearest_address: AddressResult | None = None
    distance_m: float | None = None
    boundaries: BoundaryInfo
    query_time_ms: int


class SpatialResult(BaseModel):
    results: list[AddressResult]
    total: int
    limit: int
    offset: int
    query_time_ms: int


class HealthResponse(BaseModel):
    status: str
    opensearch: bool
    postgres: bool
    cache: bool
    version: str = "1.0.0"
