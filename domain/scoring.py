from domain.models import ConfidenceSignals


def _flag(value: bool) -> int:
    return 1 if bool(value) else 0


def calculate_confidence(signals: ConfidenceSignals) -> int:
    score = 0
    score += 10 * _flag(signals.has_postcode)
    score += 20 * _flag(signals.has_state)
    score += 20 * _flag(signals.has_district)
    score += 15 * _flag(signals.has_mukim)
    score += 10 * _flag(signals.has_locality)
    score += 10 * _flag(signals.has_pbt)
    score += 10 * _flag(signals.has_postcode_boundary)
    score += 5 * _flag(signals.state_from_postcode)
    score += 5 * _flag(signals.state_from_locality)
    score += 5 * _flag(signals.has_district_and_mukim)
    score += 5 * _flag(signals.has_pbt_and_state)
    score += 10 * _flag(signals.has_state_boundary)
    score += 10 * _flag(signals.has_district_boundary)
    score += 10 * _flag(signals.has_mukim_boundary)

    score -= 35 * _flag(signals.has_state_boundary_conflict)
    score -= 35 * _flag(signals.has_district_boundary_conflict)
    score -= 35 * _flag(signals.has_mukim_boundary_conflict)
    score -= 35 * _flag(signals.has_postcode_boundary_conflict)
    score -= 25 * _flag(signals.state_conflict_postcode)
    score -= 20 * _flag(signals.suspicious_locality_has_mukim)
    score -= 15 * _flag(signals.suspicious_sub_has_street)
    score -= 10 * _flag(signals.suspicious_sub_has_bandar)
    score -= 10 * _flag(signals.suspicious_missing_street)

    if score < 0:
        return 0
    if score > 100:
        return 100
    return int(score)


def confidence_band(score: int) -> str:
    if score >= 95:
        return "VERIFIED"
    if score >= 85:
        return "HIGH"
    if score >= 70:
        return "REVIEW"
    return "REJECT"
