"""Telemetry simulator entry point (SPEC 5.1).

Ties together the four pieces that are each testable on their own:

    FleetSimulator  -> what happened (pure simulation, no I/O)
    FaultInjector   -> deliberate corruption, duplication and lateness
    TelemetryProducer -> Kafka
    ControlServer   -> the demo commands

The loop emits one event per online vehicle every EMIT_INTERVAL_REAL_S real
seconds (2s by default, so about 2 simulated minutes per tick), and writes the
odometer ledger at each simulated day boundary.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime
from typing import Dict, Optional

from common import config, metrics, simclock
from common.logging import get_logger
from simulators.control import ControlServer
from simulators.telemetry.faults import FaultInjector
from simulators.telemetry.fleet import FleetSimulator
from simulators.telemetry.producer import TelemetryProducer

log = get_logger("telemetry-sim", stage="ingestion")


class TelemetryService:
    """The running simulator, plus the state the control endpoint mutates."""

    def __init__(self) -> None:
        self.clock = simclock.load_clock()
        simclock.set_clock(self.clock)

        self.sim = FleetSimulator()
        self.faults = FaultInjector()
        self.producer = TelemetryProducer()

        self.paused = False
        self.running = True
        self.events_sent = 0
        self.last_sim_ts: datetime = self.clock.now_sim()
        # Which simulated date the odometer ledger currently covers.
        self.current_sim_date = simclock.sim_date(self.last_sim_ts)

        os.makedirs(config.ODOMETER_DIR, exist_ok=True)
        self._ticks_since_checkpoint = 0
        self._restore_odometers()
        self._install_signal_handlers()

    # --- lifecycle ---------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        """Flush Kafka on SIGTERM so `make down` does not lose buffered events."""

        def handle(signum, _frame):
            log.info(
                "shutdown signal received",
                extra={"event": "shutdown", "signal": signum},
            )
            self.running = False

        signal.signal(signal.SIGTERM, handle)
        signal.signal(signal.SIGINT, handle)

    # --- odometer ledger ---------------------------------------------------

    def _partial_path(self, sim_date: str) -> str:
        """Where the in-progress ledger for an OPEN simulated day is kept."""
        return os.path.join(config.ODOMETER_DIR, f"{sim_date}.partial.json")

    def _restore_odometers(self) -> None:
        """Reload the current day's odometers after a container restart.

        Without this, a restart mid-day silently resets every vehicle's km to
        zero. The ledger written at midnight would then cover only the time since
        the restart, while the Parquet archive still holds the whole day -- so
        `distance_covered` would come out far below `gps_km` and the batch layer
        would flag half the fleet for a distance mismatch that never happened.
        The check exists to catch a partner misreporting distance; it must not
        fire because of our own restart.
        """
        path = self._partial_path(self.current_sim_date)
        if not os.path.exists(path):
            return
        try:
            with open(path, encoding="utf-8") as handle:
                saved = json.load(handle)
        except (OSError, ValueError):
            log.warning(
                "could not read the partial odometer ledger; starting from zero",
                extra={"event": "odometer_restore_failed",
                       "sim_date": self.current_sim_date},
            )
            return

        restored = 0
        for vehicle_id, km in saved.items():
            state = self.sim.fleet.get(vehicle_id)
            if state is not None:
                state.odometer_km = float(km)
                restored += 1
        log.info(
            "odometer ledger restored after restart",
            extra={"event": "odometer_restored", "sim_date": self.current_sim_date,
                   "vehicles": restored,
                   "total_km": round(sum(saved.values()), 1)},
        )

    def _checkpoint_odometer(self) -> None:
        """Persist the open day's odometers so a restart can resume them."""
        path = self._partial_path(self.current_sim_date)
        tmp = path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(self.sim.odometer_snapshot(), handle)
            os.replace(tmp, path)
        except OSError:
            log.warning(
                "could not checkpoint the odometer ledger",
                extra={"event": "odometer_checkpoint_failed"},
            )


    def _write_odometer(self, sim_date: str) -> None:
        """Write the ground-truth km ledger for a closed simulated day.

        This is what the expense dropper reads to build a believable fuel cost,
        and what the batch layer's `gps_km` is implicitly audited against. Written
        atomically (tmp + rename) for the same reason the expense files are: a
        reader must never see a half-written ledger.
        """
        snapshot = self.sim.odometer_snapshot()
        path = os.path.join(config.ODOMETER_DIR, f"{sim_date}.json")
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, indent=1)
        os.replace(tmp, path)

        # The day is sealed; the in-progress copy is no longer needed.
        partial = self._partial_path(sim_date)
        if os.path.exists(partial):
            os.remove(partial)

        total = round(sum(snapshot.values()), 1)
        log.info(
            "odometer ledger written",
            extra={
                "event": "odometer_written",
                "sim_date": sim_date,
                "vehicles": len(snapshot),
                "total_km": total,
            },
        )
        self.sim.reset_odometers()

    def _maybe_roll_day(self, sim_ts: datetime) -> None:
        """Close the ledger when simulated time crosses midnight."""
        today = simclock.sim_date(sim_ts)
        if today != self.current_sim_date:
            self._write_odometer(self.current_sim_date)
            self.current_sim_date = today
            return

        # Checkpoint the open day every ~30 real seconds. 50 floats is nothing to
        # write, and it bounds how much of the ledger a restart can lose to one
        # checkpoint interval instead of the whole day so far.
        self._ticks_since_checkpoint += 1
        if self._ticks_since_checkpoint * config.EMIT_INTERVAL_REAL_S >= 30:
            self._checkpoint_odometer()
            self._ticks_since_checkpoint = 0

    # --- the main loop -----------------------------------------------------

    def run(self) -> None:
        metrics.serve(config.TELEMETRY_METRICS_PORT)
        self._start_control_server()

        log.info(
            "telemetry simulator starting",
            extra={
                "event": "simulator_start",
                "vehicles": config.N_VEHICLES,
                "lemons": config.N_LEMONS,
                "emit_interval_real_s": config.EMIT_INTERVAL_REAL_S,
                "sim_minutes_per_tick": round(
                    simclock.real_seconds_to_sim_minutes(config.EMIT_INTERVAL_REAL_S), 2
                ),
                "topic": config.TELEMETRY_TOPIC,
                "lemon_ids": [p.vehicle_id for p in self.sim.profiles() if p.is_lemon],
            },
        )

        while self.running:
            started = time.monotonic()
            sim_ts = self.clock.now_sim()

            if not self.paused:
                self._tick(sim_ts)

            metrics.set_sim_time(sim_ts)
            metrics.VEHICLES_ONLINE.set(self.sim.online_count())

            # Sleep the remainder of the interval, so a slow tick does not make the
            # simulated clock drift relative to the real one.
            elapsed = time.monotonic() - started
            time.sleep(max(0.0, config.EMIT_INTERVAL_REAL_S - elapsed))

        log.info("flushing producer", extra={"event": "shutdown_flush"})
        remaining = self.producer.flush(15.0)
        log.info(
            "telemetry simulator stopped",
            extra={"event": "simulator_stop", "undelivered": remaining,
                   "events_sent": self.events_sent},
        )

    def _tick(self, sim_ts: datetime) -> None:
        """One emission tick: advance the fleet, inject faults, produce."""
        self._maybe_roll_day(sim_ts)

        tick_sim_min = simclock.real_seconds_to_sim_minutes(config.EMIT_INTERVAL_REAL_S)
        events = self.sim.tick(sim_ts, tick_sim_min)

        produced = 0
        for event in events:
            for payload, key, _is_fault in self.faults.apply(event, sim_ts):
                if self.producer.send(payload, key, event_type=event["event_type"]):
                    produced += 1

        # Release events that were held back earlier as "late". They carry their
        # ORIGINAL timestamp, which is what makes them late to the speed layer.
        for payload, key in self.faults.due_delayed(sim_ts):
            if self.producer.send(payload, key, event_type="ping"):
                produced += 1

        self.events_sent += produced
        self.last_sim_ts = sim_ts

        # One line per TICK, not per event (SPEC 10.1).
        log.debug(
            "tick produced %s events",
            produced,
            extra={
                "event": "tick",
                "sim_date": self.current_sim_date,
                "online": self.sim.online_count(),
                "late_pending": self.faults.pending_late(),
            },
        )

    # --- demo control endpoint --------------------------------------------

    def _start_control_server(self) -> None:
        server = ControlServer(
            "telemetry-sim", config.TELEMETRY_CONTROL_PORT, stage="ingestion"
        )

        @server.route("health")
        def health(_params: Dict[str, str]) -> Dict:
            return {
                "status": "ok",
                "paused": self.paused,
                "sim_time": self.clock.now_sim().isoformat(),
                "sim_date": self.current_sim_date,
                "vehicles_online": self.sim.online_count(),
                "events_sent": self.events_sent,
                "late_events_pending": self.faults.pending_late(),
            }

        @server.route("force_idle")
        def force_idle(params: Dict[str, str]) -> Dict:
            """`make demo-idle`: pin a vehicle to idle past the alert threshold.

            Defaults to IDLE_ALERT_SIM_MIN + 15 so the alert definitely opens
            rather than landing exactly on the boundary.
            """
            vehicle_id = params.get("vehicle_id") or self._pick_idle_candidate()
            minutes = int(params.get("sim_minutes", config.IDLE_ALERT_SIM_MIN + 15))
            ok = self.sim.force_idle(vehicle_id, minutes, self.clock.now_sim())
            if not ok:
                return {"status": "error", "reason": f"unknown vehicle {vehicle_id}"}
            log.info(
                "vehicle forced idle for the demo",
                extra={
                    "event": "force_idle",
                    "vehicle_id": vehicle_id,
                    "sim_minutes": minutes,
                },
            )
            return {
                "status": "ok",
                "vehicle_id": vehicle_id,
                "sim_minutes": minutes,
                "expect_alert_after_sim_min": config.IDLE_ALERT_SIM_MIN,
                "expect_alert_after_real_s": round(
                    simclock.sim_minutes_to_real_seconds(config.IDLE_ALERT_SIM_MIN), 1
                ),
            }

        @server.route("pause")
        def pause(_params: Dict[str, str]) -> Dict:
            """`make demo-outage`: stop producing, to fire TelemetryNotProduced."""
            self.paused = True
            log.warning(
                "producer paused by demo control",
                extra={"event": "producer_paused"},
            )
            return {"status": "paused",
                    "expect_alert": "TelemetryNotProduced after ~1 real minute"}

        @server.route("resume")
        def resume(_params: Dict[str, str]) -> Dict:
            self.paused = False
            log.info("producer resumed", extra={"event": "producer_resumed"})
            return {"status": "running"}

        server.start()

    def _pick_idle_candidate(self) -> str:
        """Choose a vehicle that is currently online, for `demo-idle` with no id."""
        for vid, state in self.sim.fleet.items():
            if state.status != "offline":
                return vid
        return next(iter(self.sim.fleet))


def main() -> int:
    service = TelemetryService()
    service.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
