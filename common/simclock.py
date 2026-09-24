"""The simulated clock (SPEC §3).

Everything in this project happens on a compressed timeline: one real second is
`COMPRESSION` simulated seconds. With the default COMPRESSION=60 a simulated hour
takes one real minute and a simulated day takes 24 real minutes, which is what
makes a full daily reconciliation demonstrable inside a lab session.

Two rules keep this honest:

1. There is exactly one clock authority. The one-shot `simclock-init` service
   writes `/shared/simclock.json` once; every container then derives the same
   simulated time from (real_start_utc, sim_epoch, compression). If each service
   computed its own epoch on startup they would disagree by their start times and
   windows would never line up.
2. Every threshold in the system (watermarks, idle timeouts, SLAs) is expressed in
   SIMULATED minutes and converted here. Changing COMPRESSION must not require
   touching any business logic.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Tuple

from common import config

CLOCK_FILE = os.path.join(config.SHARED_DIR, "simclock.json")


def _parse_iso(value: str) -> datetime:
    """Parse ISO-8601, accepting the trailing 'Z' that Python < 3.11 rejects."""
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class SimClock:
    """An immutable (real_start, sim_epoch, compression) triple."""

    real_start_utc: datetime
    sim_epoch: datetime
    compression: int

    def now_sim(self, real_now: Optional[datetime] = None) -> datetime:
        """Simulated time corresponding to `real_now` (default: right now)."""
        real_now = real_now or datetime.now(timezone.utc)
        elapsed_real_s = (real_now - self.real_start_utc).total_seconds()
        return self.sim_epoch + timedelta(seconds=elapsed_real_s * self.compression)

    def real_for_sim(self, sim_ts: datetime) -> datetime:
        """Inverse of `now_sim`: the wall-clock instant a simulated time arrives."""
        delta_sim_s = (sim_ts - self.sim_epoch).total_seconds()
        return self.real_start_utc + timedelta(seconds=delta_sim_s / self.compression)


# --- Clock file handling ---------------------------------------------------

def write_clock_file(path: str = CLOCK_FILE) -> SimClock:
    """Create the clock file if it does not exist, then return the clock.

    Written only once: `make reset` deletes the file, and only then does the
    simulated timeline restart. Re-running `make up` must resume the same
    timeline, otherwise already-archived Parquet partitions would be from the
    future relative to a freshly restarted clock.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        payload = {
            "real_start_utc": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "sim_epoch": config.SIM_EPOCH,
            "compression": config.COMPRESSION,
        }
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        os.replace(tmp, path)  # atomic: no reader ever sees a half-written clock
    return load_clock(path)


def load_clock(path: str = CLOCK_FILE, wait_seconds: float = 60.0) -> SimClock:
    """Read the clock file, waiting for `simclock-init` to produce it."""
    deadline = time.time() + wait_seconds
    while not os.path.exists(path):
        if time.time() > deadline:
            raise FileNotFoundError(
                f"Sim clock file {path} not found. Is the simclock-init service up?"
            )
        time.sleep(0.5)
    with open(path, encoding="utf-8") as handle:
        payload = json.load(handle)
    return SimClock(
        real_start_utc=_parse_iso(payload["real_start_utc"]),
        sim_epoch=_parse_iso(payload["sim_epoch"]),
        compression=int(payload["compression"]),
    )


_CLOCK: Optional[SimClock] = None


def get_clock() -> SimClock:
    """Process-wide cached clock. Cheap to call in hot loops."""
    global _CLOCK
    if _CLOCK is None:
        _CLOCK = load_clock()
    return _CLOCK


def set_clock(clock: SimClock) -> None:
    """Injection point for unit tests, which must not touch /shared."""
    global _CLOCK
    _CLOCK = clock


# --- The four helpers the spec requires ------------------------------------

def now_sim() -> datetime:
    """Current simulated time, UTC."""
    return get_clock().now_sim()


def sim_date(ts: datetime) -> str:
    """The simulated calendar date of an event, as 'YYYY-MM-DD'.

    This is the partition key of the lake and the grain of the whole batch layer,
    so it has exactly one definition, here.
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).date().isoformat()


def day_bounds(day: "str | date") -> Tuple[datetime, datetime]:
    """Half-open [start, end) simulated bounds of a simulated date.

    Half-open on purpose: an event at exactly 00:00:00 belongs to the new day, so
    batch reruns of adjacent days can never double-count it.
    """
    if isinstance(day, str):
        day = date.fromisoformat(day)
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def sim_minutes_to_real_seconds(n: float) -> float:
    """Convert a simulated-minute threshold into real seconds to sleep/wait."""
    return (n * 60.0) / get_clock().compression


def real_seconds_to_sim_minutes(seconds: float) -> float:
    """Inverse of `sim_minutes_to_real_seconds`."""
    return (seconds * get_clock().compression) / 60.0


def sim_minutes_to_timedelta(n: float) -> timedelta:
    """A simulated-minute threshold as an EVENT-TIME timedelta.

    Event timestamps are already simulated time, so Spark watermarks and window
    sizes take this value directly -- no compression factor applies.
    """
    return timedelta(minutes=n)


def spark_interval(sim_minutes: float) -> str:
    """Render a simulated-minute threshold as a Spark interval string.

    Spark windows/watermarks operate on the event-time column, which is already
    simulated time, so "45 simulated minutes" is literally "45 minutes" to Spark.
    Routing every such string through this function keeps that reasoning in one
    place instead of scattering bare literals through the streaming code.
    """
    if sim_minutes == int(sim_minutes):
        return f"{int(sim_minutes)} minutes"
    return f"{sim_minutes} minutes"


def day_is_closed(day: str, grace_sim_min: int = 0, now: Optional[datetime] = None) -> bool:
    """True once simulated time has passed the end of `day` plus a grace period.

    The batch layer uses this with LATE_GRACE_SIM_MIN (30) which is larger than the
    maximum injected lateness (20), so a day it accepts is guaranteed complete in
    the archive.
    """
    _, end = day_bounds(day)
    current = now or now_sim()
    return current >= end + timedelta(minutes=grace_sim_min)
