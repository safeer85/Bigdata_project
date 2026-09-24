"""Tests for the simulated clock (SPEC 3, 11)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from common import simclock
from tests.conftest import TEST_EPOCH, TEST_REAL_START


def test_one_real_second_is_one_simulated_minute(fixed_clock):
    """The headline property: COMPRESSION=60 means 1 real s == 1 sim min."""
    one_second_later = TEST_REAL_START + timedelta(seconds=1)
    assert fixed_clock.now_sim(one_second_later) == TEST_EPOCH + timedelta(minutes=1)


def test_one_real_minute_is_one_simulated_hour(fixed_clock):
    later = TEST_REAL_START + timedelta(minutes=1)
    assert fixed_clock.now_sim(later) == TEST_EPOCH + timedelta(hours=1)


def test_simulated_day_takes_24_real_minutes(fixed_clock):
    later = TEST_REAL_START + timedelta(minutes=24)
    assert fixed_clock.now_sim(later) == TEST_EPOCH + timedelta(days=1)


def test_real_for_sim_is_the_inverse_of_now_sim(fixed_clock):
    """Round-tripping must be exact, because the demo scripts rely on it."""
    target_sim = TEST_EPOCH + timedelta(hours=7, minutes=13)
    real = fixed_clock.real_for_sim(target_sim)
    assert fixed_clock.now_sim(real) == target_sim


def test_sim_date_uses_the_event_timestamp_not_wall_clock():
    """sim_date is the lake partition key, so it must come from the event."""
    ts = datetime(2024, 3, 17, 23, 59, 59, tzinfo=timezone.utc)
    assert simclock.sim_date(ts) == "2024-03-17"


def test_sim_date_accepts_naive_timestamps_as_utc():
    naive = datetime(2024, 3, 17, 10, 0, 0)
    assert simclock.sim_date(naive) == "2024-03-17"


def test_day_bounds_are_half_open():
    """Midnight belongs to the NEW day, so adjacent reruns cannot double-count."""
    start, end = simclock.day_bounds("2024-01-05")
    assert start == datetime(2024, 1, 5, tzinfo=timezone.utc)
    assert end == datetime(2024, 1, 6, tzinfo=timezone.utc)
    # The end bound is exclusive: it is the start of the next day's window.
    next_start, _ = simclock.day_bounds("2024-01-06")
    assert end == next_start


def test_sim_minutes_to_real_seconds(fixed_clock):
    """The 45-minute idle threshold must be reachable in 45 real seconds."""
    assert simclock.sim_minutes_to_real_seconds(45) == pytest.approx(45.0)
    assert simclock.sim_minutes_to_real_seconds(60) == pytest.approx(60.0)


def test_real_seconds_to_sim_minutes_round_trips(fixed_clock):
    assert simclock.real_seconds_to_sim_minutes(
        simclock.sim_minutes_to_real_seconds(17)
    ) == pytest.approx(17.0)


def test_thresholds_are_compression_independent():
    """Changing COMPRESSION must not change what a threshold MEANS.

    This is the property SPEC 3 demands. The same 45 simulated minutes is 45 real
    seconds at compression 60 and 4.5 real seconds at compression 600, but it is
    45 simulated minutes in both cases -- and the Spark interval is unchanged,
    because event timestamps are already simulated time.
    """
    for compression in (30, 60, 600):
        simclock.set_clock(
            simclock.SimClock(TEST_REAL_START, TEST_EPOCH, compression)
        )
        assert simclock.spark_interval(45) == "45 minutes"
        expected_real = 45 * 60 / compression
        assert simclock.sim_minutes_to_real_seconds(45) == pytest.approx(expected_real)


def test_spark_interval_formats_whole_and_fractional_minutes():
    assert simclock.spark_interval(10) == "10 minutes"
    assert simclock.spark_interval(1440) == "1440 minutes"
    assert simclock.spark_interval(2.5) == "2.5 minutes"


def test_day_is_closed_respects_the_grace_period():
    """The batch layer only accepts a day once lateness can no longer arrive."""
    day = "2024-01-01"
    _, day_end = simclock.day_bounds(day)

    # Exactly at midnight: closed with no grace, not closed with 30 minutes.
    assert simclock.day_is_closed(day, grace_sim_min=0, now=day_end)
    assert not simclock.day_is_closed(day, grace_sim_min=30, now=day_end)

    # 30 simulated minutes later, the grace period has elapsed.
    assert simclock.day_is_closed(
        day, grace_sim_min=30, now=day_end + timedelta(minutes=30)
    )


def test_grace_period_exceeds_max_injected_lateness():
    """LATE_GRACE_SIM_MIN must be > the worst lateness the simulator injects.

    If it were not, the batch layer could reconcile a day while events for it were
    still arriving, and the report's "exact" claim would be false.
    """
    from common import config

    assert config.LATE_GRACE_SIM_MIN > config.FAULT_LATE_MAX_SIM_MIN


def test_watermark_is_smaller_than_max_lateness_on_purpose():
    """The speed layer MUST drop some late events (SPEC 5.1).

    This is not a bug to be fixed: it is the evidence that the speed layer is
    approximate and the batch layer is authoritative. If someone "helpfully"
    raises WATERMARK_SIM_MIN above the injected lateness, this test fails and
    tells them why.
    """
    from common import config

    assert config.WATERMARK_SIM_MIN < config.FAULT_LATE_MAX_SIM_MIN
