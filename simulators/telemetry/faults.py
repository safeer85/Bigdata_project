"""Deliberate fault injection (SPEC 5.1).

Every fault here exists to prove something specific about the pipeline, and each
one is counted in `fleet_faults_injected_total{kind}` so the report can show the
injected rate next to the observed rate:

  malformed (0.5%)  proves the archiver keeps unparseable bytes instead of losing
                    them, and that the speed layer dead-letters rather than crashes.
  invalid   (0.5%)  proves `common.validation` rejects identically in both layers.
  duplicate (1%)    proves `dropDuplicatesWithinWatermark` and the batch layer's
                    dedup both stop double counting.
  late      (2%)    proves the CONSISTENCY ARGUMENT. The speed-layer watermark is
                    10 simulated minutes and lateness runs to 20, so some events
                    are dropped by the speed layer but included by the batch layer.
                    The resulting non-zero `speed_batch_drift` is the evidence that
                    the batch layer is the authoritative one. Do not "fix" this.
"""
from __future__ import annotations

import copy
import json
import random
from datetime import timedelta
from typing import Dict, List, Optional, Tuple

from common import config, metrics

# Kinds, also used as the Prometheus label values.
MALFORMED = "malformed"
INVALID = "schema_invalid"
DUPLICATE = "duplicate"
LATE = "late"


class FaultInjector:
    """Applies the configured fault rates to a stream of clean events."""

    def __init__(self, rng: Optional[random.Random] = None) -> None:
        self.rng = rng or random.Random(config.SIM_SEED + 7)
        # Events held back to be released later, as (release_sim_ts, payload).
        self._delayed: List[Tuple[object, Dict]] = []
        # Pre-create the label series so the metric exists at 0 before the first
        # fault, otherwise Grafana shows "No data" for the first minute.
        for kind in (MALFORMED, INVALID, DUPLICATE, LATE):
            metrics.FAULTS_INJECTED.labels(kind=kind).inc(0)

    # --- individual corruptions -------------------------------------------

    def _make_malformed(self, event: Dict) -> bytes:
        """Truncate the JSON so it cannot be parsed at all.

        Returned as raw bytes, not a dict, because that is the whole point: the
        producer must be able to put bytes on the topic that `from_json` cannot
        read, exercising the archiver's permissive parse path.
        """
        text = json.dumps(event)
        cut = self.rng.randint(len(text) // 3, max(len(text) // 3 + 1, len(text) - 5))
        return text[:cut].encode("utf-8")

    def _make_invalid(self, event: Dict) -> Dict:
        """Corrupt one field so the payload parses but fails validation."""
        broken = copy.deepcopy(event)
        choice = self.rng.choice(["lat", "lon", "speed", "status", "fare"])
        if choice == "lat":
            # Outside the city bounding box -> OUT_OF_BOUNDS.
            broken["lat"] = config.CITY_LAT_MAX + self.rng.uniform(0.5, 5.0)
        elif choice == "lon":
            broken["lon"] = config.CITY_LON_MIN - self.rng.uniform(0.5, 5.0)
        elif choice == "speed":
            broken["speed"] = -abs(self.rng.uniform(1, 50))   # NEGATIVE_SPEED
        elif choice == "fare":
            broken["fare"] = -abs(self.rng.uniform(1, 500))   # NEGATIVE_FARE
        else:
            broken["status"] = "teleporting"                  # BAD_STATUS
        return broken

    # --- the entry point ---------------------------------------------------

    def apply(self, event: Dict, sim_ts) -> List[Tuple[bytes, str, bool]]:
        """Turn one clean event into the list of messages to actually produce.

        Returns a list of (value_bytes, key, is_fault) tuples. Usually one item;
        two when a duplicate is injected; zero when the event is held back as
        late (it is released on a later tick by `due_delayed`).
        """
        key = event["vehicle_id"]
        roll = self.rng.random()

        # Malformed and invalid are mutually exclusive branches: an event that is
        # already unparseable cannot also be "schema-invalid but parseable".
        if roll < config.FAULT_MALFORMED_RATE:
            metrics.FAULTS_INJECTED.labels(kind=MALFORMED).inc()
            return [(self._make_malformed(event), key, True)]

        if roll < config.FAULT_MALFORMED_RATE + config.FAULT_INVALID_RATE:
            metrics.FAULTS_INJECTED.labels(kind=INVALID).inc()
            payload = json.dumps(self._make_invalid(event)).encode("utf-8")
            return [(payload, key, True)]

        payload = json.dumps(event).encode("utf-8")

        # Late: hold the event back by 1-20 simulated minutes and release it then.
        # The event's `timestamp` is NOT changed -- that is what makes it late
        # rather than simply delayed. It arrives carrying an old event time.
        if self.rng.random() < config.FAULT_LATE_RATE:
            delay = self.rng.uniform(
                config.FAULT_LATE_MIN_SIM_MIN, config.FAULT_LATE_MAX_SIM_MIN
            )
            self._delayed.append((sim_ts + timedelta(minutes=delay), event))
            metrics.FAULTS_INJECTED.labels(kind=LATE).inc()
            return []

        out = [(payload, key, False)]

        # Duplicate: the SAME event_id sent twice. Both copies are valid, so only
        # deduplication can stop them inflating trip counts and revenue.
        if self.rng.random() < config.FAULT_DUPLICATE_RATE:
            metrics.FAULTS_INJECTED.labels(kind=DUPLICATE).inc()
            out.append((payload, key, True))

        return out

    def due_delayed(self, sim_ts) -> List[Tuple[bytes, str]]:
        """Release every held-back event whose delay has now elapsed."""
        ready: List[Tuple[bytes, str]] = []
        remaining: List[Tuple[object, Dict]] = []
        for release_at, event in self._delayed:
            if sim_ts >= release_at:
                ready.append(
                    (json.dumps(event).encode("utf-8"), event["vehicle_id"])
                )
            else:
                remaining.append((release_at, event))
        self._delayed = remaining
        return ready

    def pending_late(self) -> int:
        """How many late events are still held back. Surfaced on /health."""
        return len(self._delayed)
