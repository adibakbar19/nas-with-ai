"""Unit tests for address type classification (DMS 2039 rules).

Run: cd nas-processor && python -m pytest tests/test_classify.py -v
"""

from __future__ import annotations

import pytest

from nas_processor.etl.pipeline.stages.classify import detect_address_type


def detect(text: str | None) -> tuple[str | None, str | None]:
    """Helper: call detect_address_type with only address_clean set."""
    return detect_address_type({
        "address_clean": text,
        "building_name": None,
        "street_name": None,
        "sub_locality_1": None,
        "sub_locality_2": None,
        "premise_no": None,
        "unit_no": None,
        "floor_no": None,
        "postcode": None,
    })


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
