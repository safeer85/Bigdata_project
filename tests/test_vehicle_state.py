"""Direct tests for the per-vehicle state machine (streaming/state.py).

`applyInPandasWithState` is hard to exercise through a real stream, so the update
function is tested directly against a fake GroupState. The function is pure apart
from the state object, which makes this both fast and exhaustive.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from common import config
from streaming import state as vstate

BASE = datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc)


class FakeGroupState:
    """Stand-in for Spark's GroupState, with the same surface the code uses."""

    def __init__(self, value=None, timed_out: bool = False):
        self._value = value
        self.exists = value is not None
        self.hasTimedOut = timed_out
        self.removed = False
        self.timeout_ms = None

    @property
    def get(self):
        return self._value

    def update(self, value):
        self._value = value
        self.exists = True

    def remove(self):
        self.removed = True
        self._value = None
        self.exists = False

    def setTimeoutTimestamp(self, ms):
        self.timeout_ms = ms


def _events(*specs):
    """Build the pandas frame Spark would hand the function."""
    rows = []
    for minutes, status in specs:
        rows.append({
            "vehicle_id": "V001",
            "driver_id": "D001",
            "status": status,
            "zone_id": "Z06",
            "lat": 12.98,
            "lon": 77.60,
            "speed": 0.0 if status == "idle" else 30.0,
            "trip_id": None if status == "idle" else "T1",
            "event_time": BASE + timedelta(minutes=minutes),
        })
    return pd.DataFrame(rows)


def _run(state, *specs):
    frames = list(
        vstate.update_vehicle_state(("V001",), iter([_events(*specs)]), state)
    )
    return frames[0].iloc[0] if frames else None


# --- basic folding ---------------------------------------------------------

def test_first_event_creates_state():
    state = FakeGroupState()
    out = _run(state, (0, "idle"))
    assert out["vehicle_id"] == "V001"
    assert out["status"] == "idle"
    assert state.exists
    assert not out["alert_open"]


def test_a_timeout_is_scheduled_in_event_time():
    """The timeout must be OFFLINE_SIM_MIN of SIMULATED time after the last event.

    Scheduling it in processing time would make the rule change meaning whenever
    COMPRESSION changed, which SPEC 3 forbids.
    """
    state = FakeGroupState()
    _run(state, (0, "idle"))
    expected_ms = int((BASE.timestamp() + config.OFFLINE_SIM_MIN * 60) * 1000)
    assert state.timeout_ms == expected_ms


def test_an_empty_batch_emits_nothing():
    state = FakeGroupState()
    assert list(vstate.update_vehicle_state(("V001",), iter([]), state)) == []


# --- the idle stopwatch ----------------------------------------------------

def test_idle_time_accumulates_across_pings():
    """The stopwatch starts on the TRANSITION into idle, not on each idle ping.

    Resetting it per ping was the obvious-looking bug: the timer would never
    advance past one tick and no alert could ever fire.
    """
    state = FakeGroupState()
    out = _run(state, (0, "idle"), (10, "idle"), (20, "idle"))
    assert out["idle_sim_minutes"] == pytest.approx(20.0)
    assert not out["alert_open"]     # 20 < 45


def test_alert_opens_at_the_threshold():
    state = FakeGroupState()
    out = _run(state, (0, "idle"), (config.IDLE_ALERT_SIM_MIN, "idle"))
    assert out["alert_open"]
    assert out["alert_event"] == vstate.ALERT_OPENED
    assert out["idle_sim_minutes"] == pytest.approx(config.IDLE_ALERT_SIM_MIN)
    assert out["alert_opened_at"] is not None


def test_alert_does_not_open_one_minute_early():
    state = FakeGroupState()
    out = _run(state, (0, "idle"), (config.IDLE_ALERT_SIM_MIN - 1, "idle"))
    assert not out["alert_open"]
    assert out["alert_event"] == vstate.ALERT_NONE


def test_alert_is_not_reopened_while_already_open():
    """An open alert must produce ONE row, not one per micro-batch."""
    state = FakeGroupState()
    _run(state, (0, "idle"), (config.IDLE_ALERT_SIM_MIN, "idle"))
    out = _run(state, (config.IDLE_ALERT_SIM_MIN + 10, "idle"))
    assert out["alert_open"]
    assert out["alert_event"] == vstate.ALERT_NONE


def test_alert_closes_when_the_vehicle_moves():
    state = FakeGroupState()
    _run(state, (0, "idle"), (config.IDLE_ALERT_SIM_MIN, "idle"))
    out = _run(state, (config.IDLE_ALERT_SIM_MIN + 5, "enroute"))
    assert not out["alert_open"]
    assert out["alert_event"] == vstate.ALERT_CLOSED


def test_a_closing_alert_reports_how_long_the_vehicle_was_idle():
    """The duration must survive the close.

    `idle_since` is cleared by the very event that closes the alert, so a naive
    implementation writes 0.0 into the history and the alert row becomes useless
    for any "how bad was it" question.
    """
    state = FakeGroupState()
    _run(state, (0, "idle"), (config.IDLE_ALERT_SIM_MIN, "idle"))
    out = _run(state, (config.IDLE_ALERT_SIM_MIN + 5, "enroute"))
    assert out["idle_sim_minutes"] == pytest.approx(config.IDLE_ALERT_SIM_MIN + 5)


def test_the_stopwatch_restarts_after_a_trip():
    state = FakeGroupState()
    _run(state, (0, "idle"), (10, "on_trip"))
    out = _run(state, (20, "idle"), (30, "idle"))
    assert out["idle_sim_minutes"] == pytest.approx(10.0)


# --- out-of-order handling -------------------------------------------------

def test_events_are_folded_in_event_time_order():
    """A micro-batch may mix Kafka partitions, so rows arrive unordered."""
    state = FakeGroupState()
    out = _run(state, (30, "idle"), (0, "idle"), (15, "idle"))
    assert out["idle_sim_minutes"] == pytest.approx(30.0)


def test_a_stale_event_does_not_rewind_the_state():
    """An out-of-order event older than what we have folded is ignored for STATE.

    It is not lost: the archiver holds it and the batch layer counts it. But
    rewinding the state machine would corrupt the idle measurement.
    """
    state = FakeGroupState()
    _run(state, (0, "idle"), (40, "idle"))
    out = _run(state, (5, "on_trip"))    # arrives late, 35 minutes stale
    assert out is not None
    assert out["status"] == "idle"       # state was NOT rewound to on_trip
    assert out["idle_sim_minutes"] == pytest.approx(40.0)


# --- timeout path ----------------------------------------------------------

def test_timeout_marks_the_vehicle_offline_and_clears_its_state():
    stored = ("D001", "idle", "Z06", 12.98, 77.60, 0.0, None,
              BASE.timestamp(), BASE.timestamp(), False, None)
    state = FakeGroupState(stored, timed_out=True)
    out = _run(state)
    assert out["status"] == vstate.OFFLINE
    assert out["speed"] == 0.0
    assert state.removed, "state must be released or it leaks for every vehicle"


def test_timeout_closes_an_open_alert():
    """An off-shift vehicle must stop counting as an operational problem."""
    idle_since = BASE.timestamp()
    last_event = (BASE + timedelta(minutes=50)).timestamp()
    stored = ("D001", "idle", "Z06", 12.98, 77.60, 0.0, None,
              last_event, idle_since, True, last_event)
    state = FakeGroupState(stored, timed_out=True)
    out = _run(state)
    assert out["alert_event"] == vstate.ALERT_CLOSED
    assert not out["alert_open"]
    # And it still reports the duration, so the history is complete.
    assert out["idle_sim_minutes"] == pytest.approx(50.0)


def test_timeout_without_an_open_alert_reports_no_alert_event():
    stored = ("D001", "on_trip", "Z06", 12.98, 77.60, 30.0, "T1",
              BASE.timestamp(), None, False, None)
    state = FakeGroupState(stored, timed_out=True)
    out = _run(state)
    assert out["alert_event"] == vstate.ALERT_NONE


# --- the state contract ----------------------------------------------------

def test_state_tuple_matches_the_declared_schema():
    """An arity mismatch here fails inside a Spark executor with an opaque error,
    so it is worth catching in a unit test."""
    state = FakeGroupState()
    _run(state, (0, "idle"))
    assert len(state.get) == len(vstate.STATE_SCHEMA.split(","))


def test_output_columns_match_the_declared_output_schema():
    state = FakeGroupState()
    frames = list(
        vstate.update_vehicle_state(("V001",), iter([_events((0, "idle"))]), state)
    )
    declared = [part.strip().split()[0] for part in vstate.OUTPUT_SCHEMA.split(",")]
    assert list(frames[0].columns) == declared
