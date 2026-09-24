"""Zone mapping and distance maths, shared by both layers (SPEC §5.1).

The speed layer enriches each event with `zone_id` and the batch layer recomputes
`gps_km` from raw pings. If those two used different grids or different distance
formulas the Lambda merge in `/vehicles/{id}` would compare apples to oranges, so
both import from here.
"""
from __future__ import annotations

import math
from typing import Optional, Tuple

from common import config

EARTH_RADIUS_KM = 6371.0088


def zone_of(lat: float, lon: float) -> Optional[str]:
    """Map a coordinate to a zone id like 'Z07', or None if outside the city.

    The city bounding box is divided into a ZONE_ROWS x ZONE_COLS uniform grid.
    Row 0 is the NORTH-most band so that zone ids read top-left to bottom-right,
    which is how they are laid out on the Grafana geomap.
    """
    if lat is None or lon is None:
        return None
    if not (config.CITY_LAT_MIN <= lat <= config.CITY_LAT_MAX):
        return None
    if not (config.CITY_LON_MIN <= lon <= config.CITY_LON_MAX):
        return None

    lat_span = config.CITY_LAT_MAX - config.CITY_LAT_MIN
    lon_span = config.CITY_LON_MAX - config.CITY_LON_MIN

    # Fraction of the way down from the northern edge.
    lat_frac = (config.CITY_LAT_MAX - lat) / lat_span
    lon_frac = (lon - config.CITY_LON_MIN) / lon_span

    # min(..., rows-1) puts a point exactly on the southern/eastern edge in the
    # last band instead of in a non-existent row `rows`.
    row = min(int(lat_frac * config.ZONE_ROWS), config.ZONE_ROWS - 1)
    col = min(int(lon_frac * config.ZONE_COLS), config.ZONE_COLS - 1)

    index = row * config.ZONE_COLS + col + 1  # 1-based: Z01 .. Z16
    return f"Z{index:02d}"


def zone_centre(zone_id: str) -> Tuple[float, float]:
    """Centre coordinate of a zone. Used by the simulator to pick trip targets."""
    index = int(zone_id[1:]) - 1
    row, col = divmod(index, config.ZONE_COLS)
    lat_step = (config.CITY_LAT_MAX - config.CITY_LAT_MIN) / config.ZONE_ROWS
    lon_step = (config.CITY_LON_MAX - config.CITY_LON_MIN) / config.ZONE_COLS
    lat = config.CITY_LAT_MAX - (row + 0.5) * lat_step
    lon = config.CITY_LON_MIN + (col + 0.5) * lon_step
    return lat, lon


def all_zones() -> list[str]:
    """Every zone id in the grid, in display order."""
    return [f"Z{i:02d}" for i in range(1, config.ZONE_ROWS * config.ZONE_COLS + 1)]


def in_city(lat: float, lon: float) -> bool:
    """Bounding-box check. This is the validation rule for lat/lon (SPEC §4.1)."""
    return zone_of(lat, lon) is not None


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km.

    Used for (a) simulator movement and (b) the batch layer's `gps_km`, which is
    compared against the partner's reported `distance_covered`. A flat-earth
    approximation would be fine at city scale, but haversine costs nothing here
    and removes one thing to defend in the viva.
    """
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(d_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def move_towards(
    lat: float, lon: float, target_lat: float, target_lon: float, km: float
) -> Tuple[float, float]:
    """Step `km` from (lat, lon) towards the target, stopping at the target.

    Straight-line movement: real road routing is explicitly out of scope
    (SPEC §13), and utilization/earnings figures do not depend on the road graph.
    """
    remaining = haversine_km(lat, lon, target_lat, target_lon)
    if remaining <= 1e-9 or km >= remaining:
        return target_lat, target_lon
    fraction = km / remaining
    return lat + (target_lat - lat) * fraction, lon + (target_lon - lon) * fraction
