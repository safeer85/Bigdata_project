"""Tests for the earnings and profitability rules (SPEC 11).

These encode the actual business question the project answers, so they are worth
reading as a specification in their own right.
"""
from __future__ import annotations

import pytest

from common import config, earnings, profitability


# --- fares -----------------------------------------------------------------

def test_fare_is_base_plus_distance_plus_time():
    expected = config.FARE_BASE + config.FARE_PER_KM * 10 + config.FARE_PER_MIN * 20
    assert earnings.fare_for(10, 20) == pytest.approx(round(expected, 2))


def test_a_zero_length_trip_still_charges_the_base_fare():
    assert earnings.fare_for(0, 0) == pytest.approx(config.FARE_BASE)


def test_negative_inputs_are_clamped_rather_than_producing_a_negative_fare():
    """Defensive: a negative fare would be rejected by validation downstream."""
    assert earnings.fare_for(-5, -5) == pytest.approx(config.FARE_BASE)


def test_fare_is_rounded_to_two_decimals():
    """Rounded at creation so speed and batch sums agree bit for bit."""
    fare = earnings.fare_for(3.333333, 7.777777)
    assert fare == round(fare, 2)


# --- utilization -----------------------------------------------------------

def test_utilization_is_on_trip_over_online():
    assert earnings.utilization(on_trip_min=300, online_min=600) == 0.5


def test_utilization_of_an_offline_vehicle_is_zero_not_an_error():
    assert earnings.utilization(on_trip_min=0, online_min=0) == 0.0


def test_utilization_is_capped_at_one():
    """Clock skew must never produce a 140%-utilised vehicle in the report."""
    assert earnings.utilization(on_trip_min=700, online_min=600) == 1.0


def test_idle_ratio():
    assert earnings.idle_ratio(idle_count=5, active_count=20) == 0.25
    assert earnings.idle_ratio(idle_count=0, active_count=0) == 0.0


# --- reconciliation --------------------------------------------------------

def _reconcile(**overrides):
    kwargs = dict(
        vehicle_id="V001",
        sim_date="2024-01-01",
        revenue=1000.0,
        gps_km=100.0,
        fuel_cost=300.0,
        maintenance_cost=100.0,
        distance_covered=100.0,
        service_flag=False,
        has_telemetry=True,
        has_expenses=True,
    )
    kwargs.update(overrides)
    return profitability.reconcile(**kwargs)


def test_basic_reconciliation_arithmetic():
    result = _reconcile()
    assert result.cost == 400.0
    assert result.net_profit == 600.0
    assert result.margin == 0.6
    assert result.revenue_per_km == 10.0
    assert result.cost_per_km == 4.0
    assert not result.unprofitable
    assert not result.low_margin


def test_a_loss_making_vehicle_is_flagged_unprofitable():
    result = _reconcile(revenue=200.0, fuel_cost=300.0, maintenance_cost=100.0)
    assert result.net_profit == -200.0
    assert result.unprofitable


def test_low_margin_is_flagged_below_the_threshold():
    # 1000 revenue, 950 cost -> 5% margin, under the 10% threshold.
    result = _reconcile(fuel_cost=900.0, maintenance_cost=50.0)
    assert result.margin == pytest.approx(0.05)
    assert result.low_margin
    assert not result.unprofitable   # still profitable, just barely


def test_a_zero_revenue_day_is_not_flagged_low_margin():
    """A vehicle in the garage has margin 0 by construction, not by failing.

    Without this guard every off-shift vehicle would appear in the low-margin
    list every day and the report would be unreadable.
    """
    result = _reconcile(revenue=0.0, fuel_cost=0.0, maintenance_cost=0.0, gps_km=0.0)
    assert not result.low_margin
    assert result.margin == 0.0


def test_zero_distance_does_not_raise_and_yields_zero_ratios():
    result = _reconcile(gps_km=0.0, distance_covered=0.0)
    assert result.revenue_per_km == 0.0
    assert result.cost_per_km == 0.0
    assert result.distance_mismatch_pct == 0.0
    assert not result.distance_mismatch


def test_distance_mismatch_is_measured_against_the_reported_distance():
    """25% over-report: |80 - 100| / 100 = 20%, above the 15% threshold."""
    result = _reconcile(gps_km=80.0, distance_covered=100.0)
    assert result.distance_mismatch_pct == pytest.approx(0.20)
    assert result.distance_mismatch


def test_a_small_distance_difference_is_not_flagged():
    """The ~3% odometer/GPS noise must not trip the check."""
    result = _reconcile(gps_km=97.0, distance_covered=100.0)
    assert result.distance_mismatch_pct == pytest.approx(0.03)
    assert not result.distance_mismatch


def test_missing_costs_flag():
    result = _reconcile(has_expenses=False, fuel_cost=0, maintenance_cost=0,
                        distance_covered=0)
    assert result.missing_costs
    assert not result.missing_telemetry


def test_missing_telemetry_flag():
    result = _reconcile(has_telemetry=False, revenue=0, gps_km=0)
    assert result.missing_telemetry
    assert not result.missing_costs


def test_service_flag_propagates():
    assert _reconcile(service_flag=True).in_service


def test_nulls_are_treated_as_zero_rather_than_dropping_the_row():
    """A vehicle with telemetry but no expense row must still appear."""
    result = _reconcile(fuel_cost=None, maintenance_cost=None, distance_covered=None,
                        has_expenses=False)
    assert result.cost == 0.0
    assert result.net_profit == 1000.0
    assert result.missing_costs


# --- the trend flag --------------------------------------------------------

def test_becoming_unprofitable_needs_at_least_three_days():
    """We refuse to call a trend from two points."""
    assert not profitability.becoming_unprofitable([])
    assert not profitability.becoming_unprofitable([100.0])
    assert not profitability.becoming_unprofitable([100.0, 50.0])


def test_three_consecutive_declines_are_flagged_even_while_profitable():
    """The early-warning case: still earning, but sliding."""
    assert profitability.becoming_unprofitable([500.0, 300.0, 100.0])


def test_a_flat_or_rising_trend_is_not_flagged():
    assert not profitability.becoming_unprofitable([100.0, 200.0, 300.0])
    assert not profitability.becoming_unprofitable([100.0, 100.0, 100.0])


def test_loss_on_two_of_the_last_three_days_is_flagged():
    """The erratic case: not a clean slide, but losing money repeatedly."""
    assert profitability.becoming_unprofitable([-50.0, 200.0, -30.0])


def test_one_bad_day_among_three_is_not_enough():
    assert not profitability.becoming_unprofitable([100.0, -20.0, 300.0])


def test_only_the_last_three_days_are_considered():
    """A long-ago slide must not keep a recovered vehicle flagged forever."""
    assert not profitability.becoming_unprofitable(
        [900.0, 600.0, 300.0, 100.0, 400.0, 500.0]
    )


def test_reasons_are_generated_for_each_flag():
    row = {
        "unprofitable": True,
        "net_profit": -120.5,
        "low_margin": True,
        "margin": 0.02,
        "distance_mismatch": True,
        "distance_mismatch_pct": 0.31,
        "in_service": True,
        "becoming_unprofitable": True,
        "missing_costs": False,
        "missing_telemetry": False,
    }
    reasons = profitability.reasons_for(row)
    assert len(reasons) == 5
    assert any("negative" in r for r in reasons)
    assert any("margin" in r for r in reasons)
    assert any("GPS distance" in r for r in reasons)


def test_a_clean_row_has_no_reasons():
    assert profitability.reasons_for({"unprofitable": False, "low_margin": False}) == []
