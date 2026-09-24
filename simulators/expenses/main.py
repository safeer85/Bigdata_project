"""Expense dropper entry point (SPEC 5.2).

Watches the simulated clock and, EXPENSE_DELAY_SIM_MIN after each simulated day
closes, drops `expenses_<date>_v1.csv` into the landing directory. That models a
partner file arriving at about 01:00 the following morning.

Two demo commands sit on the control endpoint:

    /resubmit?sim_date=YYYY-MM-DD   write a corrected v2 file -> batch recompute
    /delay-next                     hold the next file past its SLA -> late alert
"""
from __future__ import annotations

import os
import random
import signal
import sys
import time
from datetime import timedelta
from typing import Dict, Optional, Set

from common import config, metrics, simclock
from common.logging import get_logger
from simulators.control import ControlServer
from simulators.expenses import generator

log = get_logger("expense-sim", stage="ingestion")

# Check often enough that a file is never noticeably late by accident: 5 real
# seconds is 5 simulated minutes, well inside the SLA.
POLL_REAL_S = 5.0


class ExpenseService:
    def __init__(self) -> None:
        self.clock = simclock.load_clock()
        simclock.set_clock(self.clock)

        self.rng = random.Random(config.SIM_SEED + 11)
        self.running = True
        # Dates whose v1 file we have already written this process lifetime.
        self.written: Set[str] = set()
        # Set by /delay-next: the next due file is held until this sim time.
        self.delay_until: Optional[object] = None
        self.delay_requested = False
        self.files_written = 0

        os.makedirs(config.EXPENSE_DIR, exist_ok=True)
        self._recover_existing()
        self._install_signal_handlers()

    def _recover_existing(self) -> None:
        """Do not rewrite files that survived a restart.

        The landing directory is a Docker volume, so it outlives the container. On
        restart we must not overwrite a file the batch layer has already processed,
        which would silently change a reconciled day.
        """
        for name in os.listdir(config.EXPENSE_DIR):
            if name.startswith("expenses_") and name.endswith(".csv"):
                self.written.add(name.split("_")[1])
        if self.written:
            log.info(
                "found existing expense files, will not rewrite them",
                extra={"event": "expense_recover", "dates": sorted(self.written)},
            )

    def _install_signal_handlers(self) -> None:
        def handle(signum, _frame):
            log.info("shutdown signal received",
                     extra={"event": "shutdown", "signal": signum})
            self.running = False

        signal.signal(signal.SIGTERM, handle)
        signal.signal(signal.SIGINT, handle)

    # --- writing -----------------------------------------------------------

    def _due_date(self, sim_now) -> Optional[str]:
        """The simulated date whose expense file is now due, if any.

        A file for day D is due once simulated time has passed D's end plus
        EXPENSE_DELAY_SIM_MIN. Only the single most recent such date is returned,
        so a long-running stack catches up one day at a time in order.
        """
        # Walk back from yesterday; 30 days is more than any demo will produce.
        # The walk stops at the simulated epoch: days before SIM_EPOCH never
        # happened, so there is no odometer ledger for them and a file would be
        # fabricated from nothing. (An earlier version of this loop did exactly
        # that and cheerfully produced expenses for 2023-12-27.)
        epoch_date = self.clock.sim_epoch.date()
        for days_ago in range(1, 31):
            candidate_date = (sim_now - timedelta(days=days_ago)).date()
            if candidate_date < epoch_date:
                break
            candidate = candidate_date.isoformat()
            if candidate in self.written:
                continue
            _, day_end = simclock.day_bounds(candidate)
            if sim_now >= day_end + timedelta(minutes=config.EXPENSE_DELAY_SIM_MIN):
                return candidate
        return None

    def _write(self, sim_date: str, version: int, corrected: bool) -> str:
        odometer = generator.load_odometer(sim_date)
        rows = generator.build_rows(sim_date, odometer, self.rng, corrected=corrected)
        path = os.path.join(config.EXPENSE_DIR, generator.file_name(sim_date, version))
        generator.write_csv(path, rows)

        self.files_written += 1
        metrics.EXPENSE_FILES_WRITTEN.labels(version=f"v{version}").inc()
        log.info(
            "expense file written",
            extra={
                "event": "expense_file_written",
                "sim_date": sim_date,
                "version": version,
                "corrected": corrected,
                "rows": len(rows),
                "path": path,
                "odometer_vehicles": len(odometer),
            },
        )
        return path

    # --- the main loop -----------------------------------------------------

    def run(self) -> None:
        metrics.serve(config.EXPENSE_METRICS_PORT)
        # Pre-create the v1 series so Prometheus has a 0 to graph from the start.
        # Without this the panel shows "No data" for the first 24 real minutes,
        # which in a demo looks identical to a broken exporter.
        metrics.EXPENSE_FILES_WRITTEN.labels(version="v1").inc(0)
        self._start_control_server()

        log.info(
            "expense dropper starting",
            extra={
                "event": "expense_sim_start",
                "landing_dir": config.EXPENSE_DIR,
                "delay_sim_min": config.EXPENSE_DELAY_SIM_MIN,
                "sla_sim_min": config.EXPENSE_SLA_SIM_MIN,
            },
        )

        while self.running:
            sim_now = self.clock.now_sim()
            metrics.set_sim_time(sim_now)

            due = self._due_date(sim_now)
            if due is not None:
                if self._held_back(sim_now, due):
                    pass  # deliberately late; logged inside _held_back
                else:
                    self._write(due, version=1, corrected=False)
                    self.written.add(due)

            time.sleep(POLL_REAL_S)

        log.info("expense dropper stopped", extra={"event": "expense_sim_stop"})

    def _held_back(self, sim_now, sim_date: str) -> bool:
        """Decide whether this file is deliberately late.

        Two ways a file goes late:
          * `/delay-next` was called (the `make demo-late-file` target), or
          * the random EXPENSE_LATE_FILE_RATE fired, which is the "sometimes the
            partner is just late" case the SLA alert exists for.
        """
        if self.delay_until is not None:
            if sim_now < self.delay_until:
                return True
            log.info(
                "delayed expense file is now being written",
                extra={"event": "expense_delay_released", "sim_date": sim_date},
            )
            self.delay_until = None
            return False

        if self.delay_requested:
            # Hold it past the SLA, plus a margin so the alert definitely fires.
            self.delay_until = sim_now + timedelta(
                minutes=config.EXPENSE_SLA_SIM_MIN + 30
            )
            self.delay_requested = False
            log.warning(
                "expense file held back past its SLA by demo control",
                extra={
                    "event": "expense_file_delayed",
                    "sim_date": sim_date,
                    "release_at_sim": self.delay_until.isoformat(),
                },
            )
            return True

        if self.rng.random() < config.EXPENSE_LATE_FILE_RATE:
            self.delay_until = sim_now + timedelta(
                minutes=config.EXPENSE_SLA_SIM_MIN + 15
            )
            log.warning(
                "expense file randomly late (injected fault)",
                extra={"event": "expense_file_late_random", "sim_date": sim_date},
            )
            return True

        return False

    # --- demo control endpoint --------------------------------------------

    def _start_control_server(self) -> None:
        server = ControlServer(
            "expense-sim", config.EXPENSE_CONTROL_PORT, stage="ingestion"
        )

        @server.route("health")
        def health(_params: Dict[str, str]) -> Dict:
            return {
                "status": "ok",
                "sim_time": self.clock.now_sim().isoformat(),
                "files_written": self.files_written,
                "dates_on_disk": sorted(self.written),
                "next_file_held_until": (
                    self.delay_until.isoformat() if self.delay_until else None
                ),
            }

        @server.route("resubmit")
        def resubmit(params: Dict[str, str]) -> Dict:
            """`make demo-resubmit`: write a corrected v2 for a past date.

            Defaults to the most recent date that already has a file, which is
            what a demo wants: recompute a day the batch layer has already
            reconciled, and show the same row count with different values.
            """
            sim_date = params.get("sim_date") or (
                sorted(self.written)[-1] if self.written else None
            )
            if not sim_date:
                return {"status": "error",
                        "reason": "no expense file has been written yet"}

            version = generator.latest_version(sim_date) + 1
            path = self._write(sim_date, version=version, corrected=True)
            log.info(
                "corrected expense file resubmitted",
                extra={"event": "expense_resubmit", "sim_date": sim_date,
                       "version": version},
            )
            return {
                "status": "ok",
                "sim_date": sim_date,
                "version": version,
                "path": path,
                "expect": "fleet_daily_reconciliation picks this date up within "
                          "2 real minutes and recomputes it",
            }

        @server.route("delay-next")
        def delay_next(_params: Dict[str, str]) -> Dict:
            """`make demo-late-file`: push the next file past its SLA."""
            self.delay_requested = True
            log.warning(
                "next expense file will be held past its SLA",
                extra={"event": "expense_delay_requested"},
            )
            return {
                "status": "ok",
                "sla_sim_min": config.EXPENSE_SLA_SIM_MIN,
                "expect_alert": "ExpenseFileLate once the batch DAG runs for that date",
            }

        server.start()


def main() -> int:
    ExpenseService().run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
