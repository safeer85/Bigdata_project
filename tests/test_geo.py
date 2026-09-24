"""Tests for zone mapping and haversine distance (SPEC 11)."""
from __future__ import annotations

import pytest

from common import config, geo


def test_all_zones_covers_the_grid():
    zones = geo.all_zones()
    assert len(zones) == config.ZONE_ROWS * config.ZONE_COLS
    assert zones[0] == "Z01"
    assert zones[-1] == f"Z{config.ZONE_ROWS * config.ZONE_COLS:02d}"


def test_northwest_corner_is_zone_one():
    """Row 0 is the NORTH-most band, so ids read top-left to bottom-right."""
    assert geo.zone_of(config.CITY_LAT_MAX - 1e-6, config.CITY_LON_MIN + 1e-6) == "Z01"


def test_southeast_corner_is_the_last_zone():
    last = f"Z{config.ZONE_ROWS * config.ZONE_COLS:02d}"
    assert geo.zone_of(config.CITY_LAT_MIN + 1e-6, config.CITY_LON_MAX - 1e-6) == last


def test_exact_boundary_points_stay_inside_the_grid():
    """A point exactly on the southern/eastern edge must not fall off the grid.

    Without the `min(..., rows-1)` clamp in zone_of this would compute row ==
    ZONE_ROWS and produce a zone id that does not exist.
    """
    assert geo.zone_of(config.CITY_LAT_MIN, config.CITY_LON_MAX) is not None
    assert geo.zone_of(config.CITY_LAT_MAX, config.CITY_LON_MIN) is not None


@pytest.mark.parametrize(
    "lat,lon",
    [
        (config.CITY_LAT_MAX + 0.01, 77.60),   # north of the box
        (config.CITY_LAT_MIN - 0.01, 77.60),   # south of it
        (12.98, config.CITY_LON_MIN - 0.01),   # west of it
        (12.98, config.CITY_LON_MAX + 0.01),   # east of it
        (0.0, 0.0),                            # the classic null-island fault
    ],
)
def test_points_outside_the_city_have_no_zone(lat, lon):
    """This IS the lat/lon validation rule -- the simulator injects such points."""
    assert geo.zone_of(lat, lon) is None
    assert not geo.in_city(lat, lon)


def test_none_coordinates_are_handled():
    assert geo.zone_of(None, 77.6) is None
    assert geo.zone_of(12.98, None) is None


def test_every_zone_centre_maps_back_to_its_own_zone():
    """Round-trip: zone_centre -> zone_of must be the identity.

    The simulator picks trip targets from zone centres, so a mismatch here would
    make vehicles aim at a zone and be recorded in a different one.
    """
    for zone in geo.all_zones():
        lat, lon = geo.zone_centre(zone)
        assert geo.zone_of(lat, lon) == zone


def test_haversine_zero_for_identical_points():
    assert geo.haversine_km(12.98, 77.60, 12.98, 77.60) == pytest.approx(0.0)


def test_haversine_is_symmetric():
    a = geo.haversine_km(12.90, 77.52, 13.06, 77.68)
    b = geo.haversine_km(13.06, 77.68, 12.90, 77.52)
    assert a == pytest.approx(b)


def test_haversine_one_degree_of_latitude_is_about_111_km():
    """A known reference value, so a sign or radians error would be obvious."""
    assert geo.haversine_km(12.0, 77.6, 13.0, 77.6) == pytest.approx(111.19, abs=0.5)


def test_haversine_matches_a_known_short_distance():
    """~1.11 km for 0.01 degrees of latitude, the scale this project works at."""
    assert geo.haversine_km(12.98, 77.60, 12.99, 77.60) == pytest.approx(1.112, abs=0.01)


def test_gps_jitter_accumulates_to_almost_nothing():
    """An idle vehicle's 1e-5-degree jitter must not fake kilometres.

    The batch layer's gps_km sums consecutive haversine hops, so if jitter
    registered as real distance a vehicle parked all day would report tens of km
    and be flagged for a distance mismatch it did not cause.
    """
    total = sum(
        geo.haversine_km(12.98, 77.60, 12.98 + 1e-5, 77.60 + 1e-5)
        for _ in range(500)
    )
    assert total < 1.0   # 500 jitters over a simulated day: under a kilometre


def test_move_towards_stops_at_the_target():
    """Overshoot must clamp, or vehicles would oscillate around their target."""
    lat, lon = geo.move_towards(12.90, 77.52, 12.91, 77.52, km=1000.0)
    assert (lat, lon) == pytest.approx((12.91, 77.52))


def test_move_towards_covers_the_requested_distance():
    start = (12.95, 77.60)
    target = (13.00, 77.60)
    lat, lon = geo.move_towards(start[0], start[1], target[0], target[1], km=1.0)
    assert geo.haversine_km(start[0], start[1], lat, lon) == pytest.approx(1.0, abs=0.01)


def test_move_towards_from_the_target_is_a_no_op():
    assert geo.move_towards(12.98, 77.6, 12.98, 77.6, km=5.0) == (12.98, 77.6)
