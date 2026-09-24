"""Fare and earnings maths, shared by the simulator and both layers (SPEC 5.1).

The simulator uses `fare_for` to accumulate a trip's fare; the speed layer sums
`fare` on `trip_end` for running daily revenue; the batch layer sums the same
column from the archive. Keeping the formula here means the "expected revenue"
figure in the tests is derived from the same code the pipeline runs.
"""
from __future__ import annotations

from common import config


def fare_for(distance_km: float, duration_min: float) -> float:
    """Fare = base + per-km + per-minute, rounded to 2 decimals.

    Rounded at the point of creation so that the number the simulator emits, the
    number the speed layer sums and the number the batch layer sums are identical.
    Rounding later would leave the speed-vs-batch drift metric showing float noise
    instead of the real watermark effect we want to demonstrate.
    """
    distance_km = max(0.0, distance_km)
    duration_min = max(0.0, duration_min)
    return round(
        config.FARE_BASE
        + config.FARE_PER_KM * distance_km
        + config.FARE_PER_MIN * duration_min,
        2,
    )


def utilization(on_trip_min: float, online_min: float) -> float:
    """Share of online time spent carrying a passenger, in [0, 1].

    The denominator is ONLINE minutes, not wall-clock minutes: a vehicle that is
    off shift is not under-utilised, it is off shift. Vehicles with no online time
    get 0.0 rather than a division error.
    """
    if online_min <= 0:
        return 0.0
    return round(min(1.0, on_trip_min / online_min), 4)


def idle_ratio(idle_count: int, active_count: int) -> float:
    """Idle vehicles as a share of active vehicles, in [0, 1]."""
    if active_count <= 0:
        return 0.0
    return round(idle_count / active_count, 4)
