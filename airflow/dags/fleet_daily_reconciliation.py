"""DAG `fleet_daily_reconciliation` (SPEC 8.2).

Reconciles ONE simulated day per run: the oldest day that is closed (plus the
late grace period) and has an expense file version newer than the one last
processed successfully.

SCHEDULING NOTE. The DAG runs every 2 REAL minutes, not once per simulated day.
Airflow's scheduler lives in real time and knows nothing about our compressed
clock, so the schedule is a POLL and the actual "which day should I process"
decision is made inside `find_pending_date` against the simulated clock. With
`max_active_runs=1` and `catchup=False` this gives us at most one reconciliation
in flight, and a simulated day (24 real minutes) is polled about twelve times
before it is ready -- cheap, because the first task short-circuits in
milliseconds when nothing is pending.

MANUAL TRIGGER. Passing conf `{"sim_date": "2024-01-02", "force": true}`
recomputes that specific date, which is the backfill demo.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from typing import Dict, Optional

# The batch modules and `common` are installed at /opt/fleet in the image.
if "/opt/fleet" not in sys.path:
    sys.path.insert(0, "/opt/fleet")

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models.dag import DAG
from airflow.operators.python import ShortCircuitOperator

from batch import expenses as expense_tasks
from batch import reconcile as reconcile_tasks
from batch import report as report_tasks
from batch import runs as run_tasks
from batch import vehicle_day
from common import config, simclock
from common.logging import get_logger

log = get_logger("airflow", stage="orchestration")

DEFAULT_ARGS = {
    "owner": "fleet",
    "retries": 1,
    # Short retry delay: a transient database hiccup should not stall a pipeline
    # whose whole day takes 24 real minutes.
    "retry_delay": timedelta(seconds=30),
    "depends_on_past": False,
}


with DAG(
    dag_id="fleet_daily_reconciliation",
    description="Reconcile one simulated day of telemetry against the partner expense file",
    default_args=DEFAULT_ARGS,
    # Poll every 2 real minutes; the real gate is find_pending_date.
    schedule=timedelta(minutes=2),
    start_date=datetime(2024, 1, 1),
    catchup=False,           # never backfill real-time dates; our dates are simulated
    max_active_runs=1,       # one reconciliation at a time, so reruns cannot race
    tags=["fleet", "batch", "lambda"],
    doc_md=__doc__,
) as dag:

    # ---------------------------------------------------------------------
    # 1. find_pending_date -- ShortCircuit
    # ---------------------------------------------------------------------
    def _find_pending_date(**context) -> bool:
        """Decide whether there is anything to do, and set up the run row.

        Returns False (short-circuiting every downstream task) when no simulated
        date is pending, which is the common case: the DAG polls roughly twelve
        times per simulated day and only one of those polls has work.
        """
        conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
        forced_date: Optional[str] = conf.get("sim_date")
        force: bool = bool(conf.get("force"))

        if forced_date:
            # Manual trigger: process exactly this date, whatever batch_runs says.
            available = run_tasks.available_files()
            version = available.get(forced_date)
            if version is None:
                if not force:
                    log.warning(
                        "manual trigger for a date with no expense file",
                        extra={"event": "manual_trigger_no_file", "sim_date": forced_date},
                    )
                    return False
                version = 0
            pending = (forced_date, version)
            log.info(
                "manual trigger",
                extra={"event": "manual_trigger", "sim_date": forced_date,
                       "version": version, "force": force},
            )
        else:
            pending = run_tasks.find_pending_date()

        if pending is None:
            log.info(
                "nothing pending, short-circuiting",
                extra={
                    "event": "nothing_pending",
                    "sim_now": simclock.now_sim().isoformat(),
                    "files_on_disk": sorted(run_tasks.available_files()),
                },
            )
            # Even with nothing to reconcile, a day whose file never arrived at
            # all must still raise the late flag -- otherwise the SLA breach is
            # invisible precisely when it matters.
            _flag_overdue_dates()
            return False

        sim_date, version = pending
        run_id = run_tasks.new_run_id(sim_date, version)
        run_tasks.start_run(run_id, sim_date, version)

        task_instance = context["ti"]
        task_instance.xcom_push(key="run_id", value=run_id)
        task_instance.xcom_push(key="sim_date", value=sim_date)
        task_instance.xcom_push(key="expense_version", value=version)
        return True

    def _flag_overdue_dates() -> None:
        """Record an SLA breach for any closed day with no expense file.

        Such a day never becomes "pending" (pending requires a FILE), so without
        this the ExpenseFileLate alert could never fire for a file that simply
        never showed up.
        """
        for sim_date in run_tasks.overdue_dates():
            existing = run_tasks.db.query_one(
                "SELECT run_id FROM batch_runs WHERE sim_date = %s AND expense_file_late",
                (sim_date,),
            )
            if existing:
                continue
            run_id = run_tasks.new_run_id(sim_date, 0)
            run_tasks.start_run(run_id, sim_date, 0, late=True)
            run_tasks.finish_run(
                run_id,
                run_tasks.STATUS_FAILED,
                notes=f"expense file for {sim_date} missed its "
                      f"{config.EXPENSE_SLA_SIM_MIN}-simulated-minute SLA",
            )
            log.warning(
                "expense file missed its SLA",
                extra={"event": "expense_file_late", "sim_date": sim_date},
            )

    find_pending = ShortCircuitOperator(
        task_id="find_pending_date",
        python_callable=_find_pending_date,
        doc_md="Exits the whole run when no simulated date needs reconciling.",
    )

    # ---------------------------------------------------------------------
    # helper: pull the run context that find_pending_date established
    # ---------------------------------------------------------------------
    def _ctx(task_instance) -> Dict:
        return {
            "run_id": task_instance.xcom_pull(task_ids="find_pending_date", key="run_id"),
            "sim_date": task_instance.xcom_pull(task_ids="find_pending_date", key="sim_date"),
            "version": task_instance.xcom_pull(
                task_ids="find_pending_date", key="expense_version"
            ),
        }

    # ---------------------------------------------------------------------
    # 2. check_expense_file
    # ---------------------------------------------------------------------
    @task(task_id="check_expense_file")
    def check_expense_file(**context) -> Dict:
        """Confirm the file is there, and flag it if it arrived past its SLA."""
        ctx = _ctx(context["ti"])
        run_id, sim_date, version = ctx["run_id"], ctx["sim_date"], ctx["version"]

        with run_tasks.TaskTimer(run_id, "check_expense_file"):
            path = run_tasks.expense_path(sim_date, version)
            import os

            if not os.path.exists(path):
                # A forced recompute of a date whose file is gone: fail loudly
                # rather than silently reconciling a day with no costs.
                run_tasks.mark_failed(run_id, f"expense file not found: {path}")
                raise FileNotFoundError(path)

            # Late means "arrived after the SLA", which we judge from the file's
            # own mtime translated back into simulated time.
            written_real = datetime.fromtimestamp(os.path.getmtime(path))
            import pytz

            written_real = written_real.replace(tzinfo=pytz.UTC)
            written_sim = simclock.get_clock().now_sim(written_real)
            _, day_end = simclock.day_bounds(sim_date)
            late = written_sim > day_end + timedelta(minutes=config.EXPENSE_SLA_SIM_MIN)

            if late:
                run_tasks.db.execute(
                    "UPDATE batch_runs SET expense_file_late = TRUE WHERE run_id = %s",
                    (run_id,),
                )
                log.warning(
                    "expense file arrived past its SLA",
                    extra={"event": "expense_file_late", "run_id": run_id,
                           "sim_date": sim_date,
                           "arrived_sim": written_sim.isoformat()},
                )

            return {"path": path, "late": late, **ctx}

    # ---------------------------------------------------------------------
    # 3. validate_expenses
    # ---------------------------------------------------------------------
    @task(task_id="validate_expenses")
    def validate_expenses(upstream: Dict, **context) -> Dict:
        """Split the file into clean and quarantined rows."""
        run_id, sim_date = upstream["run_id"], upstream["sim_date"]

        with run_tasks.TaskTimer(run_id, "validate_expenses"):
            clean, quarantined, reasons = expense_tasks.validate_file(
                upstream["path"], sim_date, run_id
            )
            expense_tasks.store_quarantined(quarantined)

            try:
                # Raises when more than 20% of rows are invalid. Failing here is
                # the point: a file that broken must not become a financial figure.
                ratio = expense_tasks.check_invalid_ratio(clean, quarantined)
            except ValueError as exc:
                run_tasks.mark_failed(run_id, str(exc))
                raise

            run_tasks.db.execute(
                "UPDATE batch_runs SET quarantined_rows = %s, clean_rows = %s "
                "WHERE run_id = %s",
                (len(quarantined), len(clean), run_id),
            )
            return {
                "clean": clean,
                "quarantined": len(quarantined),
                "invalid_ratio": ratio,
                "reasons": reasons,
                **upstream,
            }

    # ---------------------------------------------------------------------
    # 4. compute_vehicle_day (Spark, local mode)
    # ---------------------------------------------------------------------
    @task(task_id="compute_vehicle_day")
    def compute_vehicle_day(upstream: Dict, **context) -> Dict:
        """Recompute the day's telemetry figures from the raw Parquet archive."""
        run_id, sim_date = upstream["run_id"], upstream["sim_date"]

        with run_tasks.TaskTimer(run_id, "compute_vehicle_day"):
            telemetry = vehicle_day.run(sim_date)
            run_tasks.db.execute(
                "UPDATE batch_runs SET telemetry_rows = %s WHERE run_id = %s",
                (sum(r["events"] for r in telemetry), run_id),
            )
            return {"telemetry": telemetry, **upstream}

    # ---------------------------------------------------------------------
    # 5. reconcile_profitability
    # ---------------------------------------------------------------------
    @task(task_id="reconcile_profitability")
    def reconcile_profitability(upstream: Dict, **context) -> Dict:
        run_id, sim_date = upstream["run_id"], upstream["sim_date"]

        with run_tasks.TaskTimer(run_id, "reconcile_profitability"):
            rows = reconcile_tasks.reconcile(
                sim_date=sim_date,
                run_id=run_id,
                expense_version=upstream["version"],
                telemetry=upstream["telemetry"],
                expenses=upstream["clean"],
            )
            return {"rows": rows, **{k: v for k, v in upstream.items()
                                     if k not in ("telemetry", "clean")}}

    # ---------------------------------------------------------------------
    # 6. load_batch_views
    # ---------------------------------------------------------------------
    @task(task_id="load_batch_views")
    def load_batch_views(upstream: Dict, **context) -> Dict:
        """Delete-then-insert the date in one transaction, so reruns are idempotent."""
        run_id, sim_date = upstream["run_id"], upstream["sim_date"]

        with run_tasks.TaskTimer(run_id, "load_batch_views"):
            written = reconcile_tasks.load_batch_views(sim_date, upstream["rows"])
            reconcile_tasks.write_curated_parquet(sim_date, upstream["rows"])
            run_tasks.db.execute(
                "UPDATE batch_runs SET vehicles_out = %s WHERE run_id = %s",
                (written, run_id),
            )
            return {"written": written,
                    **{k: v for k, v in upstream.items() if k != "rows"}}

    # ---------------------------------------------------------------------
    # 7. compute_drift
    # ---------------------------------------------------------------------
    @task(task_id="compute_drift")
    def compute_drift(upstream: Dict, **context) -> Dict:
        """Speed vs batch revenue for the day. Non-zero is expected."""
        run_id, sim_date = upstream["run_id"], upstream["sim_date"]

        with run_tasks.TaskTimer(run_id, "compute_drift"):
            drift = reconcile_tasks.compute_drift(sim_date, run_id)
            return {"drift": drift, **upstream}

    # ---------------------------------------------------------------------
    # 8. generate_report
    # ---------------------------------------------------------------------
    @task(task_id="generate_report")
    def generate_report(upstream: Dict, **context) -> Dict:
        run_id, sim_date = upstream["run_id"], upstream["sim_date"]

        with run_tasks.TaskTimer(run_id, "generate_report"):
            paths = report_tasks.generate(sim_date, run_id)
            return {"report": paths, **upstream}

    # ---------------------------------------------------------------------
    # 9. record_run
    # ---------------------------------------------------------------------
    @task(task_id="record_run")
    def record_run(upstream: Dict, **context) -> Dict:
        """Close the run row out as successful.

        Writing this LAST is what makes `find_pending_date` correct: a date is
        only considered processed once every task above has finished, so a run
        that dies halfway leaves the date pending and the next poll retries it.
        """
        run_id, sim_date = upstream["run_id"], upstream["sim_date"]
        run_tasks.finish_run(
            run_id,
            run_tasks.STATUS_SUCCESS,
            notes=f"reconciled {upstream.get('written', 0)} vehicles for {sim_date}",
        )
        log.info(
            "reconciliation complete",
            extra={
                "event": "reconciliation_complete",
                "run_id": run_id,
                "sim_date": sim_date,
                "vehicles": upstream.get("written"),
                "drift_ratio": (upstream.get("drift") or {}).get("drift_ratio"),
                "report": upstream.get("report"),
            },
        )
        return {"status": "success", "run_id": run_id, "sim_date": sim_date}

    # --- wiring ----------------------------------------------------------
    checked = check_expense_file()
    validated = validate_expenses(checked)
    computed = compute_vehicle_day(validated)
    reconciled = reconcile_profitability(computed)
    loaded = load_batch_views(reconciled)
    drifted = compute_drift(loaded)
    reported = generate_report(drifted)
    recorded = record_run(reported)

    find_pending >> checked >> validated >> computed >> reconciled
    reconciled >> loaded >> drifted >> reported >> recorded
