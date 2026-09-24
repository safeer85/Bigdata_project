"""Tests for the simulator state machine and fault injection (SPEC 11).

The simulator is the source of every number in this project, so if it is wrong,
everything downstream is confidently wrong. These tests drive a whole simulated
day through `FleetSimulator` without Kafka, a clock file or a container.
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone

import pytest

from common import config, geo, schemas, validation
from simulators.expenses import generator
from simulators.telemetry import fleet
from simulators.telemetry.faults import FaultInjector

SIM_START = datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
TICK_MIN = 2.0


@pytest.fixture
def sim():
    return fleet.FleetSimulator(seed=42)


# --- fleet construction ----------------------------------------------------

def test_fleet_has_the_configured_size_and_lemon_count(sim):
    assert len(sim.fleet) == config.N_VEHICLES
    lemons = [p for p in sim.profiles() if p.is_lemon]
    assert len(lemons) == config.N_LEMONS


def test_the_fleet_is_reproducible_for_a_given_seed():
    """A demo that names a specific lemon must name the same one every run."""
    a = [p.vehicle_id for p in fleet.FleetSimulator(seed=7).profiles() if p.is_lemon]
    b = [p.vehicle_id for p in fleet.FleetSimulator(seed=7).profiles() if p.is_lemon]
    assert a == b


def test_lemons_are_bad_in_three_distinct_ways():
    """Each lemon fails for its own reason, so the report's flags are not all
    triggered by one root cause."""
    lemons = [p for p in fleet.FleetSimulator(seed=42).profiles() if p.is_lemon]
    assert any(p.fuel_rate > 0.12 for p in lemons)            # thirsty
    assert any(p.maintenance_propensity > 0.3 for p in lemons)  # breaks often
    assert any(p.demand_multiplier < 0.5 for p in lemons)     # sits idle


def test_every_vehicle_starts_inside_the_city(sim):
    for state in sim.fleet.values():
        assert geo.in_city(state.lat, state.lon)


# --- shifts ----------------------------------------------------------------

def test_a_day_shift_vehicle_is_off_at_night():
    profile = fleet.VehicleProfile("V001", "D001", "day", 6, 16, 0.07, 0.05, 1.0, False)
    assert fleet.on_shift(profile, SIM_START.replace(hour=9))
    assert not fleet.on_shift(profile, SIM_START.replace(hour=3))
    assert not fleet.on_shift(profile, SIM_START.replace(hour=20))


def test_a_night_shift_wraps_past_midnight():
    """Shift end > 24 means it wraps: 22 -> 30 is 22:00 to 06:00."""
    profile = fleet.VehicleProfile("V002", "D002", "night", 22, 30, 0.07, 0.05, 1.0, False)
    assert fleet.on_shift(profile, SIM_START.replace(hour=23))
    assert fleet.on_shift(profile, SIM_START.replace(hour=2))
    assert not fleet.on_shift(profile, SIM_START.replace(hour=12))


def test_most_vehicles_are_offline_in_the_middle_of_the_night(sim):
    """SPEC 5.1: shift patterns should put most vehicles offline at night."""
    profiles = sim.profiles()
    night = SIM_START.replace(hour=3)
    on = sum(1 for p in profiles if fleet.on_shift(p, night))
    assert on < len(profiles) / 2


# --- the state machine -----------------------------------------------------

def test_offline_vehicles_emit_nothing(sim):
    """An off-shift vehicle is silent, which is what makes the timeout matter."""
    deep_night = SIM_START.replace(hour=4)
    # Prime the machine so vehicles have settled into their shift status.
    for _ in range(3):
        sim.tick(deep_night, TICK_MIN)
    events = sim.tick(deep_night, TICK_MIN)
    emitting = {e["vehicle_id"] for e in events}
    for vehicle_id in emitting:
        assert fleet.on_shift(sim.fleet[vehicle_id].profile, deep_night)


def test_every_emitted_event_passes_validation(sim):
    """The clean path must be clean: faults are injected LATER, not here."""
    ts = SIM_START
    for _ in range(40):
        for event in sim.tick(ts, TICK_MIN):
            valid, reason = validation.validate_event(event)
            assert valid, f"{reason} in {event}"
        ts += timedelta(minutes=TICK_MIN)


def test_events_carry_every_contract_field(sim):
    events = sim.tick(SIM_START, TICK_MIN)
    assert events
    expected = {name for name, _, _, _ in schemas.TELEMETRY_FIELDS}
    assert set(events[0]) == expected


def test_event_ids_are_unique_before_fault_injection(sim):
    """Duplicates are a deliberate fault, never an accident of the simulator."""
    ids = []
    ts = SIM_START
    for _ in range(30):
        ids += [e["event_id"] for e in sim.tick(ts, TICK_MIN)]
        ts += timedelta(minutes=TICK_MIN)
    assert len(ids) == len(set(ids))


def test_idle_vehicles_have_zero_speed(sim):
    ts = SIM_START
    for _ in range(20):
        for event in sim.tick(ts, TICK_MIN):
            if event["status"] == "idle":
                assert event["speed"] == 0.0
        ts += timedelta(minutes=TICK_MIN)


def test_fare_is_zero_except_on_trip_end(sim):
    """The whole revenue calculation depends on this invariant.

    `fare` is cumulative-per-trip and only final on trip_end, so both layers sum
    it over trip_end rows alone. If a ping ever carried a non-zero fare, revenue
    would be inflated by roughly the number of pings per trip.
    """
    ts = SIM_START
    for _ in range(200):
        for event in sim.tick(ts, TICK_MIN):
            if event["event_type"] != "trip_end":
                assert event["fare"] == 0.0
            else:
                assert event["fare"] > 0.0
        ts += timedelta(minutes=TICK_MIN)


def test_trips_start_and_end_in_matched_pairs(sim):
    """Every trip_start eventually gets a trip_end, so trip counts are honest."""
    starts, ends = 0, 0
    ts = SIM_START
    for _ in range(400):
        for event in sim.tick(ts, TICK_MIN):
            starts += event["event_type"] == "trip_start"
            ends += event["event_type"] == "trip_end"
        ts += timedelta(minutes=TICK_MIN)
    assert starts > 0 and ends > 0
    # Some trips are still in progress when the window closes, so ends <= starts,
    # but they must not diverge wildly.
    assert ends <= starts
    assert starts - ends < config.N_VEHICLES


def test_vehicles_stay_inside_the_city_while_driving(sim):
    ts = SIM_START
    for _ in range(300):
        for event in sim.tick(ts, TICK_MIN):
            assert geo.in_city(event["lat"], event["lon"]), event
        ts += timedelta(minutes=TICK_MIN)


def test_demand_peaks_produce_more_trips_than_the_quiet_night():
    """Demand must actually vary, or 'earnings by time of day' is a flat line."""
    def trips_over(hour: int) -> int:
        simulator = fleet.FleetSimulator(seed=3)
        ts = SIM_START.replace(hour=hour)
        count = 0
        for _ in range(120):
            count += sum(
                1 for e in simulator.tick(ts, TICK_MIN) if e["event_type"] == "trip_start"
            )
            ts += timedelta(minutes=TICK_MIN)
        return count

    assert trips_over(8) > trips_over(3)


def test_hot_zones_are_more_attractive_than_cold_ones():
    assert fleet.zone_demand("Z07") > fleet.zone_demand("Z01")
    assert fleet.zone_demand("Z02") == 1.0     # an ordinary zone
    assert fleet.zone_demand(None) == 1.0


# --- demo controls ---------------------------------------------------------

def test_force_idle_pins_a_vehicle_to_idle(sim):
    sim.tick(SIM_START, TICK_MIN)
    assert sim.force_idle("V001", 60, SIM_START)

    ts = SIM_START
    for _ in range(20):
        ts += timedelta(minutes=TICK_MIN)
        for event in sim.tick(ts, TICK_MIN):
            if event["vehicle_id"] == "V001":
                assert event["status"] == "idle"
                assert event["speed"] == 0.0


def test_force_idle_expires_and_the_vehicle_resumes(sim):
    sim.tick(SIM_START, TICK_MIN)
    sim.force_idle("V001", 10, SIM_START)
    later = SIM_START + timedelta(minutes=200)
    statuses = set()
    ts = later
    for _ in range(150):
        for event in sim.tick(ts, TICK_MIN):
            if event["vehicle_id"] == "V001":
                statuses.add(event["status"])
        ts += timedelta(minutes=TICK_MIN)
    assert statuses - {"idle"}, "vehicle never resumed after the forced idle expired"


def test_force_idle_rejects_an_unknown_vehicle(sim):
    assert not sim.force_idle("V999", 60, SIM_START)


def test_odometer_accumulates_and_resets(sim):
    ts = SIM_START
    for _ in range(100):
        sim.tick(ts, TICK_MIN)
        ts += timedelta(minutes=TICK_MIN)

    snapshot = sim.odometer_snapshot()
    assert len(snapshot) == config.N_VEHICLES
    assert sum(snapshot.values()) > 0

    sim.reset_odometers()
    assert sum(sim.odometer_snapshot().values()) == 0


def test_a_parked_vehicle_accrues_almost_no_odometer_distance(sim):
    """GPS jitter must not register as driving in the ground-truth ledger."""
    sim.tick(SIM_START, TICK_MIN)
    sim.force_idle("V001", 600, SIM_START)
    ts = SIM_START
    for _ in range(200):
        sim.tick(ts, TICK_MIN)
        ts += timedelta(minutes=TICK_MIN)
    assert sim.odometer_snapshot()["V001"] < 0.5


# --- fault injection -------------------------------------------------------

def test_fault_rates_are_close_to_the_configured_values():
    """Over many events the injected rates must match .env, within sampling noise."""
    injector = FaultInjector(rng=random.Random(99))
    simulator = fleet.FleetSimulator(seed=5)

    counts = {"total": 0, "malformed": 0, "invalid": 0, "duplicate": 0, "late": 0}
    ts = SIM_START
    for _ in range(600):
        for event in simulator.tick(ts, TICK_MIN):
            counts["total"] += 1
            produced = injector.apply(event, ts)
            if not produced:
                counts["late"] += 1
                continue
            payload = produced[0][0]
            try:
                decoded = json.loads(payload.decode("utf-8"))
            except ValueError:
                counts["malformed"] += 1
                continue
            if not validation.validate_event(decoded)[0]:
                counts["invalid"] += 1
            if len(produced) > 1:
                counts["duplicate"] += 1
        ts += timedelta(minutes=TICK_MIN)

    total = counts["total"]
    assert total > 5000, "not enough events to judge a rate"
    # Generous tolerances: these are random draws, not exact quotas. The point is
    # that the rates are in the right ballpark, not that the RNG is rigged.
    assert counts["malformed"] / total == pytest.approx(config.FAULT_MALFORMED_RATE, abs=0.004)
    assert counts["invalid"] / total == pytest.approx(config.FAULT_INVALID_RATE, abs=0.004)
    assert counts["duplicate"] / total == pytest.approx(config.FAULT_DUPLICATE_RATE, abs=0.006)
    assert counts["late"] / total == pytest.approx(config.FAULT_LATE_RATE, abs=0.008)


def test_a_duplicate_carries_the_same_event_id(base_event):
    """Both copies must be identical, or dedup would not be exercised."""
    injector = FaultInjector(rng=random.Random(1))
    for _ in range(500):
        produced = injector.apply(dict(base_event), SIM_START)
        if len(produced) == 2:
            first = json.loads(produced[0][0].decode())
            second = json.loads(produced[1][0].decode())
            assert first["event_id"] == second["event_id"]
            assert first == second
            return
    pytest.fail("no duplicate was injected in 500 attempts")


def test_a_late_event_keeps_its_original_timestamp(base_event):
    """Late means 'arrives later carrying an OLD event time'.

    If the timestamp were rewritten on release, the event would simply be a
    normal event and the watermark would never drop anything.
    """
    injector = FaultInjector(rng=random.Random(2))
    for _ in range(500):
        if not injector.apply(dict(base_event), SIM_START):
            released = injector.due_delayed(SIM_START + timedelta(minutes=30))
            assert released
            decoded = json.loads(released[0][0].decode())
            assert decoded["timestamp"] == base_event["timestamp"]
            return
    pytest.fail("no late event was injected in 500 attempts")


def test_a_late_event_is_not_released_before_its_delay_elapses(base_event):
    injector = FaultInjector(rng=random.Random(2))
    for _ in range(500):
        if not injector.apply(dict(base_event), SIM_START):
            assert injector.due_delayed(SIM_START) == []
            assert injector.pending_late() == 1
            return
    pytest.fail("no late event was injected in 500 attempts")


def test_malformed_payloads_really_are_unparseable(base_event):
    injector = FaultInjector(rng=random.Random(3))
    found = False
    for _ in range(2000):
        for payload, _key, is_fault in injector.apply(dict(base_event), SIM_START):
            if not is_fault:
                continue
            try:
                json.loads(payload.decode("utf-8"))
            except ValueError:
                found = True
    assert found, "no malformed payload was produced"


def test_all_events_are_keyed_by_vehicle_id(base_event):
    """The key is what keeps a vehicle's events in one partition, and in order."""
    injector = FaultInjector(rng=random.Random(4))
    for _ in range(200):
        for _payload, key, _ in injector.apply(dict(base_event), SIM_START):
            assert key == base_event["vehicle_id"]


# --- expense generation ----------------------------------------------------

def test_expense_rows_cover_every_vehicle():
    odometer = {f"V{i:03d}": 100.0 for i in range(1, config.N_VEHICLES + 1)}
    rows = generator.build_rows("2024-01-01", odometer, random.Random(1))
    ids = {r["vehicle_id"] for r in rows}
    assert {f"V{i:03d}" for i in range(1, config.N_VEHICLES + 1)} <= ids


def test_a_corrected_file_has_no_injected_faults():
    """`make demo-resubmit` promises a CLEAN file; the recompute depends on it."""
    odometer = {f"V{i:03d}": 100.0 for i in range(1, config.N_VEHICLES + 1)}
    rows = generator.build_rows("2024-01-01", odometer, random.Random(1), corrected=True)

    known = {f"V{i:03d}" for i in range(1, config.N_VEHICLES + 1)}
    for row in rows:
        valid, reason = validation.validate_expense_row(row, known_vehicles=known)
        assert valid, f"{reason} in corrected row {row}"


def test_a_corrected_file_has_the_same_row_count_as_the_vehicle_roster():
    """The demo's headline: same rows, different values."""
    odometer = {f"V{i:03d}": 100.0 for i in range(1, config.N_VEHICLES + 1)}
    corrected = generator.build_rows("2024-01-01", odometer, random.Random(1),
                                     corrected=True)
    assert len(corrected) == config.N_VEHICLES


def test_thirsty_vehicles_cost_more_fuel_for_the_same_distance():
    """Fuel cost must track the vehicle's own efficiency, or lemons are invisible."""
    odometer = {f"V{i:03d}": 200.0 for i in range(1, config.N_VEHICLES + 1)}
    rows = {
        r["vehicle_id"]: r
        for r in generator.build_rows("2024-01-01", odometer, random.Random(1),
                                      corrected=True)
    }
    profiles = {p.vehicle_id: p for p in fleet.FleetSimulator().profiles()}
    thirstiest = max(profiles.values(), key=lambda p: p.fuel_rate)
    leanest = min(profiles.values(), key=lambda p: p.fuel_rate)
    assert float(rows[thirstiest.vehicle_id]["fuel_cost"]) > float(
        rows[leanest.vehicle_id]["fuel_cost"]
    )


def test_file_naming_matches_the_contract():
    assert generator.file_name("2024-01-01", 1) == "expenses_2024-01-01_v1.csv"
    assert generator.file_name("2024-03-09", 2) == "expenses_2024-03-09_v2.csv"


def test_missing_odometer_yields_zero_distance_rows_not_an_exception():
    """A partial first day has no ledger; the file must still be written."""
    rows = generator.build_rows("2024-01-01", {}, random.Random(1), corrected=True)
    assert len(rows) == config.N_VEHICLES
    assert all(float(r["distance_covered"]) == 0.0 for r in rows)


def test_csv_is_written_atomically(tmp_path):
    """The sensor must never see a partial file (SPEC 4.2)."""
    path = tmp_path / "expenses_2024-01-01_v1.csv"
    rows = generator.build_rows("2024-01-01", {"V001": 10.0}, random.Random(1),
                                corrected=True)
    generator.write_csv(str(path), rows)
    assert path.exists()
    assert not (tmp_path / "expenses_2024-01-01_v1.csv.tmp").exists()
    header = path.read_text(encoding="utf-8").splitlines()[0]
    assert header.split(",") == schemas.EXPENSE_COLUMNS


def test_latest_version_finds_the_highest_version_on_disk(tmp_path):
    for version in (1, 2, 3):
        (tmp_path / f"expenses_2024-01-01_v{version}.csv").write_text("x")
    (tmp_path / "expenses_2024-01-02_v1.csv").write_text("x")
    assert generator.latest_version("2024-01-01", str(tmp_path)) == 3
    assert generator.latest_version("2024-01-09", str(tmp_path)) == 0
