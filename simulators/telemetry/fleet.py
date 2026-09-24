"""The fleet, the demand model and the per-vehicle state machine (SPEC 5.1).

This module is deliberately free of Kafka, threads and clocks: it is a pure
simulation that is handed a simulated timestamp and returns the events that
happened. That is what makes `tests/test_simulator.py` able to drive a whole
simulated day deterministically in milliseconds.

The state machine is:

    offline -> idle -> enroute -> on_trip -> idle -> ... -> offline

`enroute` is the leg from where the vehicle is now to the pickup point; `on_trip`
is the leg with a passenger aboard. Only `on_trip` time counts towards utilization
and only `on_trip` distance earns a fare, which is the distinction the whole
profitability question rests on.
"""
from __future__ import annotations

import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from common import config, earnings, geo

# --- Demand model ----------------------------------------------------------

# Relative demand by simulated hour of day. Two peaks (08-10 and 17-20), a quiet
# night, and enough trough between them that idle alerts mean something: if demand
# were flat, either every vehicle would alert or none would.
HOURLY_DEMAND = {
    0: 0.15, 1: 0.10, 2: 0.08, 3: 0.08, 4: 0.12, 5: 0.25,
    6: 0.50, 7: 0.85, 8: 1.00, 9: 0.95, 10: 0.65, 11: 0.55,
    12: 0.60, 13: 0.55, 14: 0.50, 15: 0.55, 16: 0.70, 17: 0.95,
    18: 1.00, 19: 0.90, 20: 0.70, 21: 0.55, 22: 0.40, 23: 0.25,
}

# Two or three hot zones (a central business district and a transport hub) plus a
# cold fringe. Vehicles that park in a cold zone are the ones that go idle long
# enough to alert, which is exactly the behaviour the business wants flagged.
HOT_ZONES = {"Z06": 2.4, "Z07": 2.2, "Z10": 1.8}
COLD_ZONES = {"Z01": 0.35, "Z04": 0.35, "Z13": 0.4, "Z16": 0.35}

# Shift patterns, as (start_hour, end_hour) in simulated local time. Most of the
# fleet is off at night, so the night-time active count falls realistically
# instead of 50 vehicles idling until dawn and drowning the alert channel.
SHIFTS = [
    ("day", 6, 16),
    ("day", 7, 17),
    ("evening", 13, 23),
    ("evening", 14, 24),
    ("night", 22, 30),   # 30 == 06:00 the next day; wraps past midnight
]

MIN_SPEED_KMH = 15.0
MAX_SPEED_KMH = 45.0


@dataclass
class VehicleProfile:
    """The fixed characteristics of one vehicle, decided once at startup."""

    vehicle_id: str
    driver_id: str
    shift_name: str
    shift_start: int
    shift_end: int
    # litres/km. A lemon burns materially more fuel for the same distance.
    fuel_rate: float
    # Probability of a costly service event on any given simulated day.
    maintenance_propensity: float
    # Scales how often this vehicle is offered a trip; a low value means it sits
    # idle and eventually trips the idle alert.
    demand_multiplier: float
    is_lemon: bool


@dataclass
class VehicleState:
    """The mutable simulation state of one vehicle."""

    profile: VehicleProfile
    status: str = "offline"
    lat: float = 0.0
    lon: float = 0.0
    speed: float = 0.0
    trip_id: Optional[str] = None
    # Accumulated during the current trip; emitted in full on trip_end.
    trip_distance_km: float = 0.0
    trip_minutes: float = 0.0
    target: Optional[Tuple[float, float]] = None
    # Ground truth for the odometer ledger: every km driven, trip or not.
    odometer_km: float = 0.0
    # Simulated time until which the vehicle is forced idle by `demo-idle`.
    forced_idle_until: Optional[datetime] = None
    last_tick: Optional[datetime] = None


def build_fleet(seed: int = None) -> Dict[str, VehicleState]:
    """Create N_VEHICLES vehicles, of which N_LEMONS are deliberately bad.

    Seeded so that V001 is the same vehicle on every run: a demo that says "watch
    V047, it is one of our lemons" has to be reproducible.
    """
    rng = random.Random(seed if seed is not None else config.SIM_SEED)
    fleet: Dict[str, VehicleState] = {}

    # The lemons are spread across the id range rather than clustered at the end,
    # so a reader skimming the report does not assume the flags are an artefact of
    # ordering.
    lemon_ids = sorted(rng.sample(range(1, config.N_VEHICLES + 1), config.N_LEMONS))

    # Failure modes are assigned ROUND-ROBIN over the chosen lemons, not derived
    # from the vehicle number. Deriving it from `i % 3` looked simpler but meant a
    # random sample of five ids could easily miss a whole failure mode -- and then
    # the profitability report would have nothing to say about, say, maintenance.
    # Round-robin guarantees all three modes appear whenever N_LEMONS >= 3.
    lemon_kind = {vid: index % 3 for index, vid in enumerate(lemon_ids)}

    for i in range(1, config.N_VEHICLES + 1):
        vehicle_id = f"V{i:03d}"
        shift_name, start, end = SHIFTS[i % len(SHIFTS)]
        is_lemon = i in lemon_kind

        if is_lemon:
            # Three ways to be unprofitable, one per lemon, so the report's flags
            # are not all triggered by the same root cause.
            kind = lemon_kind[i]
            fuel_rate = 0.16 if kind == 0 else 0.085
            maintenance = 0.45 if kind == 1 else 0.06
            demand = 0.35 if kind == 2 else 0.9
        else:
            fuel_rate = rng.uniform(0.060, 0.080)     # litres/km
            maintenance = rng.uniform(0.02, 0.08)
            demand = rng.uniform(0.85, 1.25)

        profile = VehicleProfile(
            vehicle_id=vehicle_id,
            driver_id=f"D{i:03d}",
            shift_name=shift_name,
            shift_start=start,
            shift_end=end,
            fuel_rate=round(fuel_rate, 4),
            maintenance_propensity=round(maintenance, 4),
            demand_multiplier=round(demand, 3),
            is_lemon=is_lemon,
        )

        lat = rng.uniform(config.CITY_LAT_MIN, config.CITY_LAT_MAX)
        lon = rng.uniform(config.CITY_LON_MIN, config.CITY_LON_MAX)
        fleet[vehicle_id] = VehicleState(profile=profile, lat=lat, lon=lon)

    return fleet


def on_shift(profile: VehicleProfile, sim_ts: datetime) -> bool:
    """Is this vehicle rostered on at this simulated time?

    Shift ends above 24 wrap past midnight, which is how the night shift is
    expressed (22 -> 30 means 22:00 to 06:00).
    """
    hour = sim_ts.hour + sim_ts.minute / 60.0
    start, end = profile.shift_start, profile.shift_end
    if end <= 24:
        return start <= hour < end
    return hour >= start or hour < (end - 24)


def zone_demand(zone_id: Optional[str]) -> float:
    """Demand multiplier for a zone: hot, cold, or ordinary."""
    if zone_id is None:
        return 1.0
    if zone_id in HOT_ZONES:
        return HOT_ZONES[zone_id]
    if zone_id in COLD_ZONES:
        return COLD_ZONES[zone_id]
    return 1.0


def trip_probability(state: VehicleState, sim_ts: datetime, tick_sim_min: float) -> float:
    """Chance that an idle vehicle is offered a trip during this tick.

    Composed of three independent factors so each can be explained on its own:
      hour of day  x  how hot the current zone is  x  this vehicle's own multiplier

    The 0.055 base is tuned so a typical vehicle gets roughly 8-14 trips over a
    simulated day and a cold-zone lemon gets few enough to cross the 45-minute
    idle threshold a handful of times. Those are the numbers SPEC 5.1 asks for:
    "a handful of alerts per simulated day, not constant".
    """
    hour_factor = HOURLY_DEMAND[sim_ts.hour]
    zone_factor = zone_demand(geo.zone_of(state.lat, state.lon))
    base_per_min = 0.055 * hour_factor * zone_factor * state.profile.demand_multiplier
    # Scale to the tick length so changing EMIT_INTERVAL_REAL_S does not silently
    # change how busy the fleet is.
    return min(0.9, base_per_min * tick_sim_min)


class FleetSimulator:
    """Drives the whole fleet forward one tick at a time.

    `tick(sim_ts)` returns the list of event dicts that occurred, and mutates the
    vehicles' state. It knows nothing about Kafka, faults or wall-clock time.
    """

    def __init__(self, seed: Optional[int] = None) -> None:
        self.rng = random.Random(seed if seed is not None else config.SIM_SEED)
        self.fleet = build_fleet(seed)
        self.tick_sim_min = 0.0

    # --- helpers ----------------------------------------------------------

    def _random_point(self) -> Tuple[float, float]:
        return (
            self.rng.uniform(config.CITY_LAT_MIN, config.CITY_LAT_MAX),
            self.rng.uniform(config.CITY_LON_MIN, config.CITY_LON_MAX),
        )

    def _demand_weighted_point(self) -> Tuple[float, float]:
        """Pick a destination, biased towards the hot zones.

        Without this bias vehicles would diffuse uniformly across the grid and the
        zone dashboard would be flat, which would make "earnings by zone" a
        pointless panel.
        """
        if self.rng.random() < 0.55:
            zone = self.rng.choice(list(HOT_ZONES))
            lat, lon = geo.zone_centre(zone)
            # Jitter inside the zone so vehicles do not all stack on one pixel.
            lat += self.rng.uniform(-0.008, 0.008)
            lon += self.rng.uniform(-0.008, 0.008)
            return (
                min(max(lat, config.CITY_LAT_MIN), config.CITY_LAT_MAX),
                min(max(lon, config.CITY_LON_MIN), config.CITY_LON_MAX),
            )
        return self._random_point()

    def _event(
        self,
        state: VehicleState,
        sim_ts: datetime,
        event_type: str,
        fare: float = 0.0,
    ) -> Dict:
        """Build one telemetry event dict matching common.schemas."""
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "trip_id": state.trip_id,
            "driver_id": state.profile.driver_id,
            "vehicle_id": state.profile.vehicle_id,
            "lat": round(state.lat, 6),
            "lon": round(state.lon, 6),
            "speed": round(state.speed, 2),
            "status": state.status,
            # Cumulative fare, and therefore 0 on every event except trip_end.
            # Summing this column over trip_end rows is exact revenue, with no
            # windowing or state needed in either layer.
            "fare": round(fare, 2),
            "timestamp": sim_ts.isoformat().replace("+00:00", "Z"),
            "schema_version": 1,
        }

    def _advance_position(self, state: VehicleState, sim_minutes: float) -> float:
        """Move a driving vehicle towards its target. Returns km travelled."""
        if state.target is None:
            return 0.0
        # Speed drifts a little each tick rather than being redrawn, so a vehicle's
        # track looks like driving rather than teleporting.
        state.speed = max(
            MIN_SPEED_KMH,
            min(MAX_SPEED_KMH, state.speed + self.rng.uniform(-6.0, 6.0)),
        )
        km = state.speed * (sim_minutes / 60.0)
        before = (state.lat, state.lon)
        state.lat, state.lon = geo.move_towards(
            state.lat, state.lon, state.target[0], state.target[1], km
        )
        actual = geo.haversine_km(before[0], before[1], state.lat, state.lon)
        state.odometer_km += actual
        return actual

    def _go_idle(self, state: VehicleState) -> None:
        state.status = "idle"
        state.speed = 0.0
        state.trip_id = None
        state.target = None
        state.trip_distance_km = 0.0
        state.trip_minutes = 0.0

    # --- the tick ---------------------------------------------------------

    def tick(self, sim_ts: datetime, tick_sim_min: float) -> List[Dict]:
        """Advance every vehicle by `tick_sim_min` simulated minutes.

        Returns one event per ONLINE vehicle, plus an extra trip_start/trip_end
        event on the ticks where a trip begins or finishes. Offline vehicles emit
        nothing at all (SPEC 5.1), which is what makes the "no data" case real:
        the pipeline cannot tell an off-shift vehicle from a broken one without
        the state timeout.
        """
        self.tick_sim_min = tick_sim_min
        events: List[Dict] = []

        for state in self.fleet.values():
            events.extend(self._tick_vehicle(state, sim_ts, tick_sim_min))

        return events

    def _tick_vehicle(
        self, state: VehicleState, sim_ts: datetime, tick_sim_min: float
    ) -> List[Dict]:
        out: List[Dict] = []
        rostered = on_shift(state.profile, sim_ts)

        # --- shift boundaries ---------------------------------------------
        if not rostered:
            if state.status != "offline":
                # A vehicle mid-trip finishes it before going off shift; drivers do
                # not abandon passengers at 17:00, and a truncated trip would emit
                # a trip_start with no trip_end and skew the batch trip count.
                if state.status != "on_trip":
                    state.status = "offline"
                    state.speed = 0.0
                    state.target = None
                    return []
            else:
                return []

        if state.status == "offline" and rostered:
            self._go_idle(state)

        # --- forced idle, for `make demo-idle` -----------------------------
        if state.forced_idle_until is not None:
            if sim_ts < state.forced_idle_until:
                if state.status != "idle":
                    self._go_idle(state)
                out.append(self._jittered_idle_event(state, sim_ts))
                return out
            state.forced_idle_until = None

        # --- state machine -------------------------------------------------
        if state.status == "idle":
            if self.rng.random() < trip_probability(state, sim_ts, tick_sim_min):
                # A trip is offered: head to the pickup point first.
                state.status = "enroute"
                state.trip_id = str(uuid.uuid4())
                state.target = self._demand_weighted_point()
                state.speed = self.rng.uniform(MIN_SPEED_KMH, MAX_SPEED_KMH)
                state.trip_distance_km = 0.0
                state.trip_minutes = 0.0
                # trip_start marks the PICKUP, which happens on arrival, not here.
                out.append(self._event(state, sim_ts, "ping"))
            else:
                out.append(self._jittered_idle_event(state, sim_ts))

        elif state.status == "enroute":
            self._advance_position(state, tick_sim_min)
            arrived = state.target is not None and geo.haversine_km(
                state.lat, state.lon, state.target[0], state.target[1]
            ) < 0.15
            if arrived:
                # Pickup. Emit trip_start, then pick the drop-off point.
                state.status = "on_trip"
                state.target = self._demand_weighted_point()
                out.append(self._event(state, sim_ts, "trip_start"))
            else:
                out.append(self._event(state, sim_ts, "ping"))

        elif state.status == "on_trip":
            km = self._advance_position(state, tick_sim_min)
            state.trip_distance_km += km
            state.trip_minutes += tick_sim_min
            arrived = state.target is not None and geo.haversine_km(
                state.lat, state.lon, state.target[0], state.target[1]
            ) < 0.15
            if arrived:
                # Drop-off. The FINAL fare rides on this one event.
                fare = earnings.fare_for(state.trip_distance_km, state.trip_minutes)
                state.status = "idle"
                state.speed = 0.0
                event = self._event(state, sim_ts, "trip_end", fare=fare)
                out.append(event)
                self._go_idle(state)
            else:
                out.append(self._event(state, sim_ts, "ping"))

        return out

    def _jittered_idle_event(self, state: VehicleState, sim_ts: datetime) -> Dict:
        """An idle ping: speed 0 with tiny GPS jitter (SPEC 5.1).

        The jitter is ~1e-5 degrees, about a metre, which is realistic GPS noise.
        It also proves the batch layer's `gps_km` ignores noise: a vehicle parked
        all day must not accumulate kilometres.
        """
        state.lat += self.rng.uniform(-1e-5, 1e-5)
        state.lon += self.rng.uniform(-1e-5, 1e-5)
        state.speed = 0.0
        return self._event(state, sim_ts, "ping")

    # --- demo controls ----------------------------------------------------

    def force_idle(self, vehicle_id: str, sim_minutes: int, sim_ts: datetime) -> bool:
        """Pin a vehicle to idle for N simulated minutes (`make demo-idle`).

        Returns False for an unknown vehicle id so the control endpoint can answer
        404 instead of silently doing nothing.
        """
        state = self.fleet.get(vehicle_id)
        if state is None:
            return False
        state.forced_idle_until = sim_ts + timedelta(minutes=sim_minutes)
        self._go_idle(state)
        return True

    def odometer_snapshot(self) -> Dict[str, float]:
        """Ground-truth km per vehicle, for the odometer ledger (SPEC 5.1)."""
        return {
            vid: round(state.odometer_km, 4) for vid, state in self.fleet.items()
        }

    def reset_odometers(self) -> None:
        """Zero the ledger at a simulated day boundary."""
        for state in self.fleet.values():
            state.odometer_km = 0.0

    def online_count(self) -> int:
        return sum(1 for s in self.fleet.values() if s.status != "offline")

    def profiles(self) -> List[VehicleProfile]:
        return [s.profile for s in self.fleet.values()]
