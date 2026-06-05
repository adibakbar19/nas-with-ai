"""Pydantic response schemas for address routes."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class StandardizedAddressResponse(BaseModel):
    model_config = {"from_attributes": True}

    record_id: str
    naskod: str | None = None
    canonical_address_key: str | None = None
    premise_no: str | None = None
    lot_no: str | None = None
    unit_no: str | None = None
    floor_no: str | None = None
    floor_level: str | None = None
    building_name: str | None = None
    street_name_prefix: str | None = None
    street_name: str | None = None
    sub_locality_1: str | None = None
    sub_locality_2: str | None = None
    sub_locality_3: str | None = None
    rural_relative_direction: str | None = None
    rural_landmark_name: str | None = None
    rural_estate_name: str | None = None
    rural_descriptor: str | None = None
    locality_name: str | None = None
    postcode: str | None = None
    postcode_name: str | None = None
    state_code: str | None = None
    state_name: str | None = None
    district_code: str | None = None
    district_name: str | None = None
    mukim_code: str | None = None
    mukim_name: str | None = None
    mukim_id: str | None = None
    pbt_id: str | None = None
    pbt_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    address_type_id: int | None = None
    address_clean: str | None = None
    ownership_structure: str | None = None
    confidence_score: float | None = None
    confidence_band: str | None = None
    validation_status: str | None = None
    validation_date: datetime | None = None
    validation_by: str | None = None
    lifecycle_status: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class RawAddressRecord(BaseModel):
    model_config = {"from_attributes": True}

    raw_id: str
    raw_text: str | None = None
    match_status: str | None = None
    agency_id: str | None = None
    error_reason: str | None = None
    replaces_raw_id: str | None = None
    ingest_timestamp: datetime | None = None
    job_id: str | None = None
    match_confidence: float | None = None


class AddressAliasRecord(BaseModel):
    model_config = {"from_attributes": True}

    alias_id: str
    alias_text: str | None = None
    alias_type: str | None = None
    source_system: str | None = None
    valid_from: datetime | None = None
    valid_until: datetime | None = None


class AddressDetailResponse(BaseModel):
    address: StandardizedAddressResponse
    raw_history: list[RawAddressRecord] = []
    aliases: list[AddressAliasRecord] = []


class AddressSearchResponse(BaseModel):
    results: list[StandardizedAddressResponse]
    total: int
    limit: int
    offset: int
