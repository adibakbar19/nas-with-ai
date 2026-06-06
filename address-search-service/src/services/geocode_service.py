"""Geocode service — forward (address → coordinates) and reverse (coordinates → address)."""

from __future__ import annotations

import logging
import time

from sqlalchemy import text

from src.clients.postgres import get_engine
from src.schemas.responses import (
    BoundaryInfo,
    Coordinates,
    GeocodeForwardResult,
    GeocodeReverseResult,
)
from src.services.spatial_service import find_nearby
from src.services.validate_service import validate_address
from config import settings

logger = logging.getLogger(__name__)


def _get_boundary_for_point(lat: float, lon: float) -> BoundaryInfo:
    """Look up administrative boundary information for a lat/lon point via PostGIS."""
    lookup = settings.postgres_lookup_schema
    try:
        stmt = text(f"""
            SELECT
                m.mukim_name AS mukim,
                m.mukim_id::text AS mukim_id,
                d.district_name AS district,
                d.district_id::text AS district_id,
                s.state_name AS state,
                s.state_code AS state_code,
                p.pbt_name AS pbt,
                p.pbt_id::text AS pbt_id
            FROM "{lookup}".mukim_boundary m
            JOIN "{lookup}".district d ON d.district_id = m.district_id
            JOIN "{lookup}".state s ON s.state_id = d.state_id
            LEFT JOIN "{lookup}".pbt p
                ON ST_Within(ST_SetSRID(ST_MakePoint(:lon, :lat), 4326), p.boundary_geom)
            WHERE ST_Within(ST_SetSRID(ST_MakePoint(:lon, :lat), 4326), m.boundary_geom)
            LIMIT 1
        """)
        with get_engine().connect() as conn:
            row = conn.execute(stmt, {"lat": lat, "lon": lon}).mappings().fetchone()
        if row:
            return BoundaryInfo(
                mukim=row["mukim"],
                mukim_id=row["mukim_id"],
                district=row["district"],
                district_id=row["district_id"],
                state=row["state"],
                state_code=row["state_code"],
                pbt=row["pbt"],
                pbt_id=row["pbt_id"],
            )
    except Exception:
        logger.debug("boundary_lookup_failed lat=%s lon=%s", lat, lon, exc_info=True)
    return BoundaryInfo()


def geocode_forward(
    address: str,
    postcode: str | None = None,
    state_code: str | None = None,
) -> GeocodeForwardResult:
    """Forward geocode: address → coordinates + boundary. Never raises."""
    t0 = time.time()
    try:
        validate_result = validate_address(address, postcode=postcode, state_code=state_code)

        if not validate_result.matched:
            return GeocodeForwardResult(
                query=address,
                match_type="no_match",
                confidence=0.0,
                coordinates=None,
                address=None,
                boundaries=None,
                query_time_ms=int((time.time() - t0) * 1000),
            )

        lat = validate_result.latitude
        lon = validate_result.longitude
        coordinates = None
        match_type = validate_result.match_type

        if lat is not None and lon is not None:
            coordinates = Coordinates(
                latitude=lat,
                longitude=lon,
                accuracy_metres=10,
            )
            boundaries = _get_boundary_for_point(lat, lon)
        else:
            boundaries = BoundaryInfo()

        from src.schemas.responses import AddressResult
        address_result = AddressResult(
            record_id=validate_result.record_id or "",
            naskod=validate_result.naskod,
            address_clean=validate_result.address_clean,
            postcode=validate_result.postcode,
            state_name=validate_result.state_name,
            latitude=lat,
            longitude=lon,
        )

        return GeocodeForwardResult(
            query=address,
            match_type=match_type,
            confidence=validate_result.confidence,
            coordinates=coordinates,
            address=address_result,
            boundaries=boundaries,
            query_time_ms=int((time.time() - t0) * 1000),
        )

    except Exception:
        logger.exception("geocode_forward_failed address=%r", address)
        return GeocodeForwardResult(
            query=address,
            match_type="no_match",
            confidence=0.0,
            coordinates=None,
            address=None,
            boundaries=None,
            query_time_ms=int((time.time() - t0) * 1000),
        )


def geocode_reverse(
    lat: float,
    lon: float,
    radius_m: int = 100,
) -> GeocodeReverseResult:
    """Reverse geocode: coordinates → nearest address + boundary. Never raises."""
    t0 = time.time()
    nearest_address = None
    distance_m = None

    try:
        results, _, _ = find_nearby(lat, lon, radius_m=radius_m, limit=1, offset=0)
        if results:
            from src.schemas.responses import AddressResult
            r = results[0]
            distance_m = r.get("distance_m")
            nearest_address = AddressResult(
                record_id=r["record_id"],
                naskod=r.get("naskod"),
                address_clean=r.get("address_clean"),
                street_name=r.get("street_name"),
                building_name=r.get("building_name"),
                locality_name=r.get("locality_name"),
                postcode=r.get("postcode"),
                state_code=r.get("state_code"),
                state_name=r.get("state_name"),
                mukim_name=r.get("mukim_name"),
                district_name=r.get("district_name"),
                pbt_name=r.get("pbt_name"),
                confidence_band=r.get("confidence_band"),
                lifecycle_status=r.get("lifecycle_status"),
                latitude=r.get("latitude"),
                longitude=r.get("longitude"),
                distance_m=distance_m,
            )
    except Exception:
        logger.exception("geocode_reverse_nearest_failed lat=%s lon=%s", lat, lon)

    boundaries = _get_boundary_for_point(lat, lon)

    return GeocodeReverseResult(
        latitude=lat,
        longitude=lon,
        nearest_address=nearest_address,
        distance_m=distance_m,
        boundaries=boundaries,
        query_time_ms=int((time.time() - t0) * 1000),
    )
