"""Tests for the shared validation rules (SPEC 11).

These matter more than they look: `common.validation` is the single definition of
"is this event usable", applied by the speed layer to decide what reaches the DLQ
and by the batch layer to decide what enters the profitability figures. If the
rules were wrong, both layers would be wrong in the same way and the drift metric
would not reveal it.
"""
from __future__ import annotations

import copy

import pytest

from common import config, validation


def test_a_clean_event_is_valid(base_event):
    assert validation.validate_event(base_event) == (True, None)


@pytest.mark.parametrize(
    "field",
    ["event_id", "event_type", "driver_id", "vehicle_id", "lat", "lon",
     "speed", "status", "fare", "timestamp", "schema_version"],
)
def test_missing_required_fields_are_rejected(base_event, field):
    broken = copy.deepcopy(base_event)
    broken[field] = None
    valid, reason = validation.validate_event(broken)
    assert not valid
    assert reason == validation.MISSING_FIELD


def test_trip_id_may_be_null(base_event):
    """trip_id is the ONLY nullable field: a vehicle is idle most of the time."""
    base_event["trip_id"] = None
    assert validation.validate_event(base_event)[0]


def test_unknown_event_type_is_rejected(base_event):
    base_event["event_type"] = "teleport"
    assert validation.validate_event(base_event) == (False, validation.BAD_EVENT_TYPE)


def test_unknown_status_is_rejected(base_event):
    base_event["status"] = "hovering"
    assert validation.validate_event(base_event) == (False, validation.BAD_STATUS)


def test_coordinates_outside_the_city_are_rejected(base_event):
    """The injected out-of-box fault (~0.5% of events)."""
    base_event["lat"] = config.CITY_LAT_MAX + 3.0
    assert validation.validate_event(base_event) == (False, validation.OUT_OF_BOUNDS)


def test_negative_speed_is_rejected(base_event):
    base_event["speed"] = -12.0
    assert validation.validate_event(base_event) == (False, validation.NEGATIVE_SPEED)


def test_negative_fare_is_rejected(base_event):
    base_event["fare"] = -100.0
    assert validation.validate_event(base_event) == (False, validation.NEGATIVE_FARE)


def test_zero_speed_and_zero_fare_are_valid(base_event):
    """Idle vehicles have speed 0 and non-trip_end events have fare 0."""
    base_event["speed"] = 0.0
    base_event["fare"] = 0.0
    assert validation.validate_event(base_event)[0]


def test_unparseable_timestamp_is_rejected(base_event):
    base_event["timestamp"] = "yesterday afternoon"
    assert validation.validate_event(base_event) == (False, validation.BAD_TIMESTAMP)


def test_non_numeric_coordinates_are_rejected(base_event):
    base_event["lat"] = "twelve point nine"
    assert validation.validate_event(base_event) == (False, validation.BAD_NUMBER)


def test_a_non_dict_payload_is_unparseable():
    assert validation.validate_event("not json at all") == (False, validation.UNPARSEABLE)
    assert validation.validate_event(None) == (False, validation.UNPARSEABLE)


def test_timestamp_parsing_accepts_the_z_suffix():
    """Python < 3.11 rejects a trailing Z, and every event we emit has one."""
    parsed = validation.parse_event_timestamp("2024-01-01T08:30:00Z")
    assert parsed is not None
    assert parsed.tzinfo is not None
    assert parsed.hour == 8


def test_timestamp_parsing_accepts_an_explicit_offset():
    parsed = validation.parse_event_timestamp("2024-01-01T08:30:00+00:00")
    assert parsed is not None and parsed.hour == 8


def test_naive_timestamps_are_treated_as_utc():
    parsed = validation.parse_event_timestamp("2024-01-01T08:30:00")
    assert parsed is not None and parsed.tzinfo is not None


# --- expense rows ----------------------------------------------------------

def _expense_row(**overrides):
    row = {
        "date": "2024-01-01",
        "vehicle_id": "V001",
        "fuel_cost": "412.50",
        "maintenance_cost": "18.20",
        "distance_covered": "145.2",
        "service_flag": "false",
    }
    row.update(overrides)
    return row


def test_a_clean_expense_row_is_valid():
    assert validation.validate_expense_row(_expense_row()) == (True, None)


def test_empty_expense_value_is_a_missing_value():
    row = _expense_row(fuel_cost="")
    assert validation.validate_expense_row(row) == (False, validation.EXP_MISSING_VALUE)


def test_whitespace_only_value_counts_as_missing():
    row = _expense_row(distance_covered="   ")
    assert validation.validate_expense_row(row) == (False, validation.EXP_MISSING_VALUE)


def test_negative_expense_value_is_rejected():
    row = _expense_row(maintenance_cost="-50.0")
    assert validation.validate_expense_row(row) == (False, validation.EXP_NEGATIVE_VALUE)


def test_unknown_vehicle_is_rejected_when_a_roster_is_supplied():
    row = _expense_row(vehicle_id="V999")
    valid, reason = validation.validate_expense_row(row, known_vehicles={"V001", "V002"})
    assert not valid and reason == validation.EXP_UNKNOWN_VEHICLE


def test_known_vehicle_passes_the_roster_check():
    assert validation.validate_expense_row(
        _expense_row(), known_vehicles={"V001"}
    ) == (True, None)


def test_non_numeric_expense_value_is_a_missing_value():
    """A garbled number is as unusable as an absent one, and reported the same."""
    row = _expense_row(fuel_cost="N/A")
    assert validation.validate_expense_row(row) == (False, validation.EXP_MISSING_VALUE)


def test_zero_costs_are_valid():
    """A vehicle that did not move has zero fuel cost; that is data, not an error."""
    row = _expense_row(fuel_cost="0", maintenance_cost="0", distance_covered="0")
    assert validation.validate_expense_row(row) == (True, None)


def test_every_reason_code_is_a_non_empty_string():
    codes = validation.reason_codes()
    assert len(codes) == len(set(codes))
    assert all(isinstance(c, str) and c for c in codes)
