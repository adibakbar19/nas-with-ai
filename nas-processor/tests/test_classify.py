"""Unit tests for address type classification (DMS 2039 rules).

Run: cd nas-processor && python -m pytest tests/test_classify.py -v
"""

from __future__ import annotations

import pytest

from nas_processor.etl.pipeline.stages.classify import detect_address_type


_DEFAULTS = {
    "address_clean": None,
    "building_name": None,
    "street_name": None,
    "sub_locality_1": None,
    "sub_locality_2": None,
    "premise_no": None,
    "lot_no": None,
    "unit_no": None,
    "floor_no": None,
    "postcode": None,
}


def detect(text_or_row) -> tuple[str | None, str | None]:
    """Helper: accepts a string (address_clean only) or a full field dict."""
    if isinstance(text_or_row, dict):
        row = {**_DEFAULTS, **text_or_row}
    else:
        row = {**_DEFAULTS, "address_clean": text_or_row}
    return detect_address_type(row)


# ── PO BOX ────────────────────────────────────────────────────────────────────

def test_po_box_po_box_english():
    assert detect("P.O. Box 1234 50670 Kuala Lumpur") == ("po_box", "po_box")

def test_po_box_po_box_malay():
    assert detect("Peti Surat 626 46770 Petaling Jaya") == ("po_box", "po_box")

def test_po_box_wdt():
    assert detect("WDT 426 87897 Kuching") == ("po_box", "wdt")


# ── RURAL ─────────────────────────────────────────────────────────────────────

def test_rural_kampung():
    assert detect("Kampung Baru Sungai Buloh 47000") == ("rural", "kampung")

def test_rural_felda():
    assert detect("FELDA Jengka 25 26400 Bandar Tun Razak") == ("rural", "felda")

def test_rural_rumah_panjang():
    assert detect("Uma Lesong Rumah Panjang 96950 Belaga") == ("rural", "rumah_panjang")

def test_rural_kampung_atas_air():
    assert detect("Kampung Atas Air Miri 98000") == ("rural", "kampung_atas_air")


# ── GOVERNMENT ────────────────────────────────────────────────────────────────

def test_government_school_full():
    assert detect("Sekolah Kebangsaan Bukit Bintang") == ("government", "school")

def test_government_school_smk():
    assert detect("SMK Damansara Utama") == ("government", "school")

def test_government_hospital():
    assert detect("Hospital Kuala Lumpur") == ("government", "hospital")

def test_government_police():
    assert detect("Balai Polis Cheras") == ("government", "police")

def test_government_federal_jabatan():
    assert detect("Jabatan Pendaftaran Negara") == ("government", "federal")

def test_government_court():
    assert detect("Mahkamah Sesyen Kuala Lumpur") == ("government", "court")

def test_government_pbt():
    assert detect("Majlis Bandaraya Johor Bahru") == ("government", "pbt")

def test_government_university():
    assert detect("Universiti Malaya") == ("government", "university")


# ── POI ───────────────────────────────────────────────────────────────────────

def test_poi_mosque_masjid():
    assert detect("Masjid Negara Kuala Lumpur") == ("poi", "mosque")

def test_poi_mosque_surau():
    assert detect("Surau Al-Hidayah Taman Maju") == ("poi", "mosque")

def test_poi_train_station():
    # "Stesen" must win over "Masjid" in the name
    assert detect("Stesen LRT Masjid Jamek") == ("poi", "train_station")

def test_poi_stadium():
    assert detect("Stadium Nasional Bukit Jalil") == ("poi", "stadium")

def test_poi_airport():
    assert detect("KLIA Lapangan Terbang") == ("poi", "airport")

def test_poi_market():
    assert detect("Pasar Borong Selayang") == ("poi", "market")


# ── COMMERCIAL ───────────────────────────────────────────────────────────────

def test_commercial_office_menara():
    assert detect("Menara Kuala Lumpur") == ("commercial", "office")

def test_commercial_hotel():
    assert detect("Hotel Istana Kuala Lumpur") == ("commercial", "hotel")

def test_commercial_mall():
    assert detect("Suria KLCC Shopping Mall") == ("commercial", "mall")


# ── RESIDENTIAL ───────────────────────────────────────────────────────────────

def test_residential_landed_digit_prefix():
    assert detect("12 Jalan Bukit Bintang 55100") == ("residential", "landed")

def test_residential_apartment_pangsapuri():
    assert detect("A-12-5 Pangsapuri Kelana") == ("residential", "apartment")

def test_residential_flat():
    assert detect("Flat Sri Kelantan Jalan Ipoh") == ("residential", "flat")


# ── NULL CASES ────────────────────────────────────────────────────────────────

def test_null_empty_string():
    assert detect("") == (None, None)

def test_null_none():
    assert detect(None) == (None, None)


# ── EDGE CASES ────────────────────────────────────────────────────────────────

def test_premise_no_field_triggers_landed():
    """When premise_no is set in the dict, address is residential/landed."""
    result = detect_address_type({
        "address_clean": "NO 12 JALAN BUKIT BINTANG KUALA LUMPUR",
        "building_name": None,
        "street_name": None,
        "sub_locality_1": None,
        "sub_locality_2": None,
        "premise_no": "12",
        "unit_no": None,
        "floor_no": None,
        "postcode": None,
    })
    assert result == ("residential", "landed")

def test_unit_and_floor_triggers_apartment():
    result = detect_address_type({
        "address_clean": "A-5-12 Jalan Ampang",
        "building_name": None,
        "street_name": None,
        "sub_locality_1": None,
        "sub_locality_2": None,
        "premise_no": None,
        "unit_no": "12",
        "floor_no": "5",
        "postcode": None,
    })
    assert result == ("residential", "apartment")

def test_stesen_wins_over_masjid_in_name():
    """Transport poi check must run before mosque check to handle named stations."""
    result = detect_address_type({
        "address_clean": "Stesen LRT Masjid Jamek Kuala Lumpur",
        "building_name": None,
        "street_name": None,
        "sub_locality_1": None,
        "sub_locality_2": None,
        "premise_no": None,
        "unit_no": None,
        "floor_no": None,
        "postcode": None,
    })
    assert result == ("poi", "train_station")

def test_government_wins_before_commercial_kompleks():
    """Mahkamah (court) in 'Kompleks Mahkamah' must beat commercial/office."""
    result = detect("Kompleks Mahkamah Kuala Lumpur")
    assert result == ("government", "court")

def test_never_raises_on_garbage():
    result = detect_address_type({"address_clean": 12345, "unknown_key": object()})
    assert result == (None, None) or isinstance(result, tuple)


# ── NEW: street prefix signals ────────────────────────────────────────────────

def test_commercial_persiaran():
    assert detect({"address_clean": "LOT 3A PERSIARAN KEWAJIPAN USJ 1 47600"}) == ("commercial", "office")

def test_commercial_lebuh():
    assert detect({"address_clean": "12 Lebuh Ampang 50100 Kuala Lumpur"}) == ("commercial", "office")

def test_commercial_leboh():
    assert detect({"address_clean": "Leboh Chulia Penang"}) == ("commercial", "office")

def test_commercial_dataran():
    assert detect({"address_clean": "Dataran KLCC 50450 Kuala Lumpur"}) == ("commercial", "office")


# ── NEW: residential street signals ──────────────────────────────────────────

def test_residential_lorong():
    assert detect({"address_clean": "NO 8 LORONG DELIMA 11700 GELUGOR"}) == ("residential", "landed")

def test_residential_lorong_in_street_name():
    result = detect_address_type({
        **_DEFAULTS,
        "address_clean": "Residensi Delima",
        "street_name": "Lorong Delima 3",
    })
    assert result == ("residential", "landed")

def test_residential_taman_in_street_name():
    result = detect_address_type({
        **_DEFAULTS,
        "address_clean": "No 5 Jalan Harmoni",
        "street_name": "Taman Bukit Indah",
    })
    assert result == ("residential", "landed")


# ── NEW: digit-start fallback ─────────────────────────────────────────────────

def test_residential_digit_start_fallback():
    assert detect({"address_clean": "12 JALAN BUKIT BINTANG 55100"}) == ("residential", "landed")


# ── NEW: lot_no fallback ──────────────────────────────────────────────────────

def test_residential_lot_no_fallback():
    # No keyword match in address_clean → lot_no triggers residential/landed
    result = detect_address_type({
        **_DEFAULTS,
        "address_clean": "PT 1234 Sungai Buloh",
        "lot_no": "PT1234",
    })
    assert result == ("residential", "landed")


# ── NEW: postcode zone tiebreaker ─────────────────────────────────────────────

def test_government_postcode_zone_500_699():
    # postcode 62514: last 3 digits = 514 → government zone, AND "jabatan" keyword
    assert detect({
        "address_clean": "Lot 5 Jabatan Imigresen",
        "postcode": "62514",
    }) == ("government", "federal")

def test_postcode_zone_tiebreaker_no_keyword():
    # No keyword match — postcode last 3 digits 550 → government zone tiebreaker
    assert detect({"postcode": "62550"}) == ("government", "federal")

def test_postcode_zone_700_999_po_box():
    # last 3 digits 810 → po_box zone tiebreaker
    assert detect({"postcode": "50810"}) == ("po_box", "po_box")

def test_postcode_zone_does_not_override_keyword():
    # "kampung" keyword wins — postcode zone is tiebreaker only
    assert detect({
        "address_clean": "Kampung Baru 50300",
        "postcode": "50810",  # po_box zone, but keyword wins
    }) == ("rural", "kampung")
