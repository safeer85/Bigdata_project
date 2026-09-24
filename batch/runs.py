"""Run bookkeeping for the batch layer (SPEC 8.2 tasks 1 and 9).

`batch_runs` is the batch layer's memory. It answers three questions that the DAG
asks on every run:

  * Which simulated date is pending? (the newest expense file version we have NOT
    yet processed for a day that is closed plus the late grace period)
  * Did the expense file for that date miss its SLA?
  * Is the most recent run healthy, and how long did each task take?

Keeping this in PostgreSQL rather than in Airflow XCom or Variables matters: the
API exports the batch metrics by reading these rows, so a Pushgateway is not
needed and the metrics survive an Airflow restart.
"""
from __future__ import annotations

import json
import os
import re
import time
import uuid
from datetime import timedelta
from typing import Dict, List, Optional, Tuple

from common import config, db, simclock
from common.logging import get_logger

log = get_logger("batch", stage="orchestration")

FILE_PATTERN = re.compile(r"^expenses_(\d{4}-\d{2}-\d{2})_v(\d+)\.csv$")

STATUS_RUNNING = "running"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"


# --- the landing directory -------------------------------------------------

def available_files() -> Dict[str, int]:
    """Map each simulated date on disk to its highest expense file version."""
    found: Dict[str, int] = {}
    if not os.path.isdir(config.EXPENSE_DIR):
        return found
    for name in os.listdir(config.EXPENSE_DIR):
        match = FILE_PATTERN.match(name)
        if match:
            sim_date, version = match.group(1), int(match.group(2))
            found[sim_date] = max(found.get(sim_date, 0), version)
    return found


def expense_path(sim_date: str, version: int) -> str:
    return os.path.join(config.EXPENSE_DIR, f"expenses_{sim_date}_v{version}.csv")


# --- choosing what to process ---------------------------------------------

def processed_versions() -> Dict[str, int]:
    """The highest expense version SUCCESSFULLY processed per simulated date."""
    rows = db.query(
        "SELECT sim_date::text AS sim_date, max(expense_file_version) AS version "
        "FROM batch_runs WHERE status = %s GROUP BY sim_date",
        (STATUS_SUCCESS,),
    )
    return {row["sim_date"]: int(row["version"] or 0) for row in rows}


def find_pending_date() -> Optional[Tuple[str, int]]:
    """The OLDEST simulated date that still needs reconciling (SPEC 8.2).

    Both conditions must hold:

      1. The day is closed plus LATE_GRACE_SIM_MIN. The grace period is 30
         simulated minutes, deliberately larger than the maximum injected
         lateness of 20, so anything we are going to receive for that day has
         already been archived. Without this the batch layer could reconcile a
         day while events for it were still arriving, and its "exact" claim
         would be false.
      2. There is an expense file version NEWER than the one last processed
         successfully. This is what makes `make demo-resubmit` work: dropping a
         v2 file makes an already-reconciled day pending again.

    OLDEST first, so a stack left running overnight catches up in order rather
    than reconciling the newest day and leaving gaps.
    """
    available = available_files()
    processed = processed_versions()
    now_sim = simclock.now_sim()

    candidates: List[Tuple[str, int]] = []
    for sim_date, version in available.items():
        if not simclock.day_is_closed(sim_date, config.LATE_GRACE_SIM_MIN, now_sim):
            continue
        if version > processed.get(sim_date, 0):
            candidates.append((sim_date, version))

    if not candidates:
        return None
    return sorted(candidates)[0]


def expense_file_is_late(sim_date: str, now_sim=None) -> bool:
    """True when no file has arrived within EXPENSE_SLA_SIM_MIN of day close."""
    if sim_date in available_files():
        return False
    _, day_end = simclock.day_bounds(sim_date)
    now_sim = now_sim or simclock.now_sim()
    return now_sim > day_end + timedelta(minutes=config.EXPENSE_SLA_SIM_MIN)


def overdue_dates() -> List[str]:
    """Closed simulated dates with no expense file at all, past their SLA.

    Used by `check_expense_file` to raise the late flag for a day whose file
    never arrived -- a day that `find_pending_date` cannot see, precisely because
    there is no file for it.
    """
    available = available_files()
    processed = processed_versions()
    now_sim = simclock.now_sim()
    epoch_date = simclock.get_clock().sim_epoch.date()

    out: List[str] = []
    day = epoch_date
    while True:
        sim_date = day.isoformat()
        _, day_end = simclock.day_bounds(sim_date)
        if now_sim <= day_end + timedelta(minutes=config.EXPENSE_SLA_SIM_MIN):
            break
        if sim_date not in available and sim_date not in processed:
            out.append(sim_date)
        day = day + timedelta(days=1)
    return out


# --- writing run rows ------------------------------------------------------

def new_run_id(sim_date: str, version: int) -> str:
    """A run id that is readable in a log AND unique across reruns."""
    return f"{sim_date}_v{version}_{uuid.uuid4().hex[:8]}"


def start_run(run_id: str, sim_date: str, version: int, late: bool = False) -> None:
    db.upsert(
        "batch_runs",
        [{
            "run_id": run_id,
            "sim_date": sim_date,
            "expense_file_version": version,
            "status": STATUS_RUNNING,
            "expense_file_late": late,
        }],
        conflict_keys=["run_id"],
    )
    log.info(
        "batch run started",
        extra={"event": "batch_run_started", "run_id": run_id,
               "sim_date": sim_date, "expense_file_version": version},
    )


def finish_run(run_id: str, status: str, **fields) -> None:
    """Close out a run row with its status, counts and per-task durations."""
    assignments = ["status = %s", "finished_at = now()"]
    params: List[object] = [status]

    for key, value in fields.items():
        if key == "task_durations":
            assignments.append("task_durations = %s::jsonb")
            params.append(json.dumps(value))
        else:
            assignments.append(f"{key} = %s")
            params.append(value)

    params.append(run_id)
    db.execute(
        f"UPDATE batch_runs SET {', '.join(assignments)} WHERE run_id = %s", params
    )
    log.info(
        "batch run finished",
        extra={"event": "batch_run_finished", "run_id": run_id,
               "status": status, **fields},
    )


def record_task_duration(run_id: str, task: str, seconds: float) -> None:
    """Merge one task's duration into the run's JSONB map.

    `||` on jsonb merges at the top level, so each task writes only its own key
    and tasks running in parallel cannot clobber each other's timings.
    """
    db.execute(
        "UPDATE batch_runs SET task_durations = task_durations || %s::jsonb "
        "WHERE run_id = %s",
        (json.dumps({task: round(seconds, 3)}), run_id),
    )


class TaskTimer:
    """Context manager that records a task's duration on exit.

    Used by every DAG task so `fleet_batch_task_duration_seconds` is populated
    without each task remembering to time itself.
    """

    def __init__(self, run_id: str, task: str) -> None:
        self.run_id = run_id
        self.task = task
        self.started = 0.0

    def __enter__(self) -> "TaskTimer":
        self.started = time.monotonic()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        elapsed = time.monotonic() - self.started
        try:
            record_task_duration(self.run_id, self.task, elapsed)
        except Exception:  # noqa: BLE001 - bookkeeping must not mask a real error
            log.warning(
                "could not record task duration",
                extra={"event": "task_duration_write_failed", "task": self.task},
            )
        log.info(
            "task complete",
            extra={"event": "task_complete", "run_id": self.run_id,
                   "task": self.task, "seconds": round(elapsed, 3),
                   "failed": exc_type is not None},
        )
        return False    # never swallow the exception


def mark_failed(run_id: str, reason: str) -> None:
    finish_run(run_id, STATUS_FAILED, notes=reason[:2000])


def last_successful_run() -> Optional[Dict]:
    return db.query_one(
        "SELECT * FROM batch_runs WHERE status = %s "
        "ORDER BY finished_at DESC NULLS LAST LIMIT 1",
        (STATUS_SUCCESS,),
    )
