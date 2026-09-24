"""Per-vehicle state machine and idle alerts (SPEC 7.3, query `vehicle_state`).

This is the one genuinely stateful piece of the speed layer, and it is the part
most worth being able to explain line by line.

WHY arbitrary stateful processing at all. "Has this vehicle been idle for 45
simulated minutes?" cannot be answered by a window or an aggregation. It needs a
memory of WHEN the vehicle last stopped being idle, carried forward across
micro-batches, and it needs to fire even when NO new event arrives -- a vehicle
that goes silent is exactly the case we care about. `applyInPandasWithState` is
the only Structured Streaming construct that provides both.

WHY EventTimeTimeout rather than ProcessingTimeTimeout. Everything in this project
is measured on the simulated clock. A processing-time timeout would fire after N
real seconds, which is N/60 simulated minutes -- so changing COMPRESSION would
silently change the business rule. With EventTimeTimeout the timeout is set at a
simulated timestamp and fires when the WATERMARK passes it, so "offline after 20
simulated minutes of silence" means exactly that at any compression.

CONSEQUENCE worth knowing for the viva: an event-time timeout only fires when the
watermark advances, and the watermark only advances when other events arrive. If
the entire fleet went silent at once, no timeout would fire. That is acceptable
here (the whole-fleet outage is caught by `TelemetryNotProduced` instead) and it
is the standard trade-off of event-time processing.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterator, Tuple

import pandas as pd

from common import config

# The state tuple carried per vehicle between micro-batches. Kept as a flat tuple
# of primitives because that is what Spark can serialise into its state store.
#   (driver_id, status, zone_id, lat, lon, speed, trip_id,
#    last_event_epoch_s, idle_since_epoch_s, alert_open, alert_opened_epoch_s)
STATE_SCHEMA = (
    "driver_id STRING, status STRING, zone_id STRING, lat DOUBLE, lon DOUBLE, "
    "speed DOUBLE, trip_id STRING, last_event_ts DOUBLE, idle_since_ts DOUBLE, "
    "alert_open BOOLEAN, alert_opened_ts DOUBLE"
)

# What the function emits: one row per vehicle per batch, carrying both the
# current state and (when it changed) the alert transition.
OUTPUT_SCHEMA = (
    "vehicle_id STRING, driver_id STRING, status STRING, zone_id STRING, "
    "lat DOUBLE, lon DOUBLE, speed DOUBLE, trip_id STRING, "
    "last_event_time TIMESTAMP, idle_since TIMESTAMP, idle_sim_minutes DOUBLE, "
    "alert_open BOOLEAN, alert_event STRING, alert_opened_at TIMESTAMP"
)

ALERT_NONE = "none"
ALERT_OPENED = "opened"
ALERT_CLOSED = "closed"
OFFLINE = "offline"


def _to_ts(epoch_s):
    """Epoch seconds -> tz-aware datetime, or None."""
    if epoch_s is None or pd.isna(epoch_s):
        return None
    return datetime.fromtimestamp(float(epoch_s), tz=timezone.utc)


def update_vehicle_state(
    key: Tuple[str],
    batches: Iterator[pd.DataFrame],
    state,
) -> Iterator[pd.DataFrame]:
    """Fold a vehicle's new events into its state and emit the result.

    Called by Spark once per vehicle per micro-batch, and additionally when the
    vehicle's event-time timeout expires (then with an EMPTY batch iterator and
    `state.hasTimedOut` set).
    """
    vehicle_id = key[0]

    # --- restore ---------------------------------------------------------
    if state.exists:
        (driver_id, status, zone_id, lat, lon, speed, trip_id,
         last_event_ts, idle_since_ts, alert_open, alert_opened_ts) = state.get
    else:
        driver_id = zone_id = trip_id = None
        status = OFFLINE
        lat = lon = speed = None
        last_event_ts = idle_since_ts = alert_opened_ts = None
        alert_open = False

    alert_event = ALERT_NONE
    # How long the vehicle had been idle at the instant an alert CLOSES. Captured
    # at the transition because `idle_since` is cleared by the same event that
    # closes the alert, so computing it afterwards always yields 0 and the alert
    # history would lose the one number that makes it useful.
    closing_idle_minutes = None

    # --- timeout path: the vehicle has gone quiet ------------------------
    if state.hasTimedOut:
        # No events for OFFLINE_SIM_MIN of simulated time. Mark it offline so the
        # fleet view stops counting it as active. Note this is genuinely
        # ambiguous in the domain -- an off-shift vehicle and a vehicle with a
        # dead modem look identical from here -- which is why the dashboard shows
        # it as "offline" rather than guessing.
        status = OFFLINE
        speed = 0.0
        # An open idle alert is CLOSED on going offline. Leaving it open would
        # keep counting an off-shift vehicle as an operational problem all night.
        if alert_open:
            alert_open = False
            alert_event = ALERT_CLOSED
            if idle_since_ts is not None and last_event_ts is not None:
                closing_idle_minutes = (last_event_ts - idle_since_ts) / 60.0
        idle_since_ts = None
        state.remove()

        yield pd.DataFrame(
            [{
                "vehicle_id": vehicle_id,
                "driver_id": driver_id,
                "status": status,
                "zone_id": zone_id,
                "lat": lat,
                "lon": lon,
                "speed": speed,
                "trip_id": None,
                "last_event_time": _to_ts(last_event_ts),
                "idle_since": None,
                "idle_sim_minutes": round(closing_idle_minutes or 0.0, 2),
                "alert_open": False,
                "alert_event": alert_event,
                "alert_opened_at": _to_ts(alert_opened_ts),
            }]
        )
        return

    # --- normal path: fold in this batch's events -------------------------
    frames = [frame for frame in batches if not frame.empty]
    if not frames:
        return
    events = pd.concat(frames, ignore_index=True)

    # Kafka guarantees order only within a partition, and a micro-batch may mix
    # partitions, so sort explicitly. The idle logic reads "the last status change",
    # which is meaningless over unordered rows.
    events = events.sort_values("event_time")

    for row in events.itertuples(index=False):
        event_ts = row.event_time.timestamp()

        # An out-of-order event that predates what we have already folded in is
        # ignored for STATE purposes. It is not lost -- the archiver has it and
        # the batch layer will count it -- but rewinding the state machine to an
        # older position would corrupt the idle measurement.
        if last_event_ts is not None and event_ts < last_event_ts:
            continue

        new_status = row.status
        if new_status == "idle":
            # Start the idle stopwatch only on the TRANSITION into idle. Resetting
            # it on every idle ping would mean the timer never advanced and no
            # alert could ever fire.
            if status != "idle" or idle_since_ts is None:
                idle_since_ts = event_ts
        else:
            # Any non-idle event ends the idle period and closes an open alert.
            if alert_open:
                alert_open = False
                alert_event = ALERT_CLOSED
                if idle_since_ts is not None:
                    closing_idle_minutes = (event_ts - idle_since_ts) / 60.0
            idle_since_ts = None

        driver_id = row.driver_id
        status = new_status
        zone_id = row.zone_id
        lat = float(row.lat)
        lon = float(row.lon)
        speed = float(row.speed)
        trip_id = row.trip_id
        last_event_ts = event_ts

    if last_event_ts is None:
        return

    # --- idle alert decision ----------------------------------------------
    idle_sim_minutes = 0.0
    if status == "idle" and idle_since_ts is not None:
        idle_sim_minutes = (last_event_ts - idle_since_ts) / 60.0
        # Both values are SIMULATED times, so the difference is simulated minutes
        # directly -- no compression factor. That is the whole reason event
        # timestamps carry simulated time rather than wall-clock time.
        if idle_sim_minutes >= config.IDLE_ALERT_SIM_MIN and not alert_open:
            alert_open = True
            alert_opened_ts = last_event_ts
            alert_event = ALERT_OPENED

    # --- persist and schedule the next timeout ----------------------------
    state.update(
        (driver_id, status, zone_id, lat, lon, speed, trip_id,
         last_event_ts, idle_since_ts, alert_open, alert_opened_ts)
    )
    # Timeout measured in EVENT time: fires when the watermark passes
    # last_event + OFFLINE_SIM_MIN. setTimeoutTimestamp takes milliseconds.
    state.setTimeoutTimestamp(
        int((last_event_ts + config.OFFLINE_SIM_MIN * 60) * 1000)
    )

    yield pd.DataFrame(
        [{
            "vehicle_id": vehicle_id,
            "driver_id": driver_id,
            "status": status,
            "zone_id": zone_id,
            "lat": lat,
            "lon": lon,
            "speed": speed,
            "trip_id": trip_id,
            "last_event_time": _to_ts(last_event_ts),
            "idle_since": _to_ts(idle_since_ts),
            # A closing alert reports how long the vehicle WAS idle; an open or
            # ongoing one reports how long it has been idle so far.
            "idle_sim_minutes": round(
                closing_idle_minutes if closing_idle_minutes is not None
                else idle_sim_minutes,
                2,
            ),
            "alert_open": bool(alert_open),
            "alert_event": alert_event,
            "alert_opened_at": _to_ts(alert_opened_ts),
        }]
    )
