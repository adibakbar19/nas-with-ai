from pydantic import BaseModel, Field


class AddressMatchReviewDecisionRequest(BaseModel):
    review_note: str | None = Field(default=None, examples=["Confirmed same premises after agency clarification."])


class AddressMatchReviewResponse(BaseModel):
    review_id: int
    candidate_canonical_address_key: str
    candidate_checksum: str | None = None
    candidate_raw_address_variant: str | None = None
    candidate_normalized_address_variant: str
    candidate_state_id: int | None = None
    candidate_district_id: int | None = None
    candidate_postcode_id: int | None = None
    candidate_street_id: int | None = None
    candidate_locality_id: int | None = None
    matched_address_id: int
    matched_canonical_address_key: str | None = None
    matched_naskod: str | None = None
    matched_address_summary: str | None = None
    match_score: int
    match_reasons: str
    review_status: str
    reviewed_by: str | None = None
    reviewed_at: str | None = None
    review_note: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class AddressMatchReviewListResponse(BaseModel):
    status: str | None = None
    count: int
    items: list[AddressMatchReviewResponse]
