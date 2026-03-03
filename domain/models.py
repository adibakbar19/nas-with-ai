from dataclasses import dataclass


@dataclass(frozen=True)
class AddressInput:
    raw_address: str
    postcode: str | None = None
    latitude: float | None = None
    longitude: float | None = None


@dataclass(frozen=True)
class ConfidenceSignals:
    has_postcode: bool = False
    has_state: bool = False
    has_district: bool = False
    has_mukim: bool = False
    has_locality: bool = False
    has_pbt: bool = False
    has_postcode_boundary: bool = False
    state_from_postcode: bool = False
    state_from_locality: bool = False
    has_district_and_mukim: bool = False
    has_pbt_and_state: bool = False
    has_state_boundary: bool = False
    has_district_boundary: bool = False
    has_mukim_boundary: bool = False
    has_state_boundary_conflict: bool = False
    has_district_boundary_conflict: bool = False
    has_mukim_boundary_conflict: bool = False
    has_postcode_boundary_conflict: bool = False
    state_conflict_postcode: bool = False
    suspicious_locality_has_mukim: bool = False
    suspicious_sub_has_street: bool = False
    suspicious_sub_has_bandar: bool = False
    suspicious_missing_street: bool = False
