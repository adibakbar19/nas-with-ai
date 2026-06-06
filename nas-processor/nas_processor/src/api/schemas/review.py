"""Pydantic schemas for the match review queue routes."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class CandidateAddress(BaseModel):
    model_config = {"from_attributes": True}

    record_id: str
    address_clean: str | None = None
    postcode: str | None = None
    state_name: str | None = None
    locality_name: str | None = None
    street_name: str | None = None
    mukim_name: str | None = None
    naskod: str | None = None
    confidence_band: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    score: float
    matching_fields: list[str] = []
    differing_fields: list[str] = []
    other_agency_count: int = 0
    maps_url: str | None = None


class ReviewQueueItem(BaseModel):
    model_config = {"from_attributes": True}

    review_id: str
    raw_id: str
    raw_text: str | None = None
    agency_id: str | None = None
    error_reason: str | None = None
    original_confidence: float | None = None
    reason: str
    status: str
    assigned_to: str | None = None
    assigned_at: datetime | None = None
    resolved_at: datetime | None = None
    decision: str | None = None
    resolved_by: str | None = None
    resolution_notes: str | None = None
    candidates: list[CandidateAddress] = []
    created_at: datetime
    updated_at: datetime


class ReviewListResponse(BaseModel):
    results: list[ReviewQueueItem]
    total: int
    limit: int
    offset: int


class AssignReviewRequest(BaseModel):
    assigned_to: str


class ResolveReviewRequest(BaseModel):
    decision: str
    chosen_candidate_id: str | None = None
    resolution_notes: str | None = None


class ResolveReviewResponse(BaseModel):
    review_id: str
    decision: str
    raw_id: str
    standardized_id: str | None = None
    alias_created: bool
    message: str
