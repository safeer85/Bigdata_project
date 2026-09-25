"""Batch metrics exported by the API (SPEC 10.2 "Batch").

WHY THE API EXPORTS THEM. Airflow tasks are short-lived processes. Prometheus
pulls, so by the time it scrapes, the task that computed the number is gone. The
usual answer is a Pushgateway, but a Pushgateway is a separate service that holds
job state forever, has no notion of staleness, and is explicitly discouraged for
exactly this pattern.

Instead the batch layer writes its outcomes to PostgreSQL (`batch_runs`,
`speed_batch_drift`) because it needs them there anyway -- the API and the report
read them -- and the API re-exports the latest values on each scrape. One fewer
service, no stale-forever series, and the metric and the report can never
disagree because they read the same row.

Recorded in docs/decisions.md.
"""
from __future__ import annotations

from common import metrics
from common.logging import get_logger
from api import queries

log = get_logger("api", stage="serving")


def refresh() -> None:
    """Re-read the batch tables and update the gauges. Called on every scrape.

    Every failure is swallowed: a database hiccup must degrade the metrics
    endpoint, not break it. A broken /metrics would take Prometheus blind at
    precisely the moment something is wrong.
    """
    try:
        _refresh_batch_run()
        _refresh_drift()
        _refresh_alerts()
    except Exception:  # noqa: BLE001
        log.warning(
            "could not refresh batch metrics from the database",
            extra={"event": "batch_metrics_refresh_failed"},
        )


def _refresh_batch_run() -> None:
    run = queries.last_successful_run()
    if run:
        if run.get("finished_epoch"):
            metrics.BATCH_LAST_SUCCESS.set(float(run["finished_epoch"]))
        metrics.BATCH_QUARANTINED_ROWS.set(float(run.get("quarantined_rows") or 0))

        # Per-task durations, so the pipeline-health dashboard can show which
        # task in the DAG is the slow one.
        for task, seconds in (run.get("task_durations") or {}).items():
            try:
                metrics.BATCH_TASK_DURATION.labels(task=task).set(float(seconds))
            except (TypeError, ValueError):
                continue

    # The late flag is taken from the MOST RECENT run of any status, not the last
    # successful one: a day whose file never arrived is recorded as a failed run,
    # and that is precisely the case ExpenseFileLate must fire on.
    latest = queries.last_batch_run()
    metrics.EXPENSE_FILE_LATE.set(
        1.0 if (latest and latest.get("expense_file_late")) else 0.0
    )
    metrics.BATCH_RUN_FAILED.set(
        1.0 if (latest and latest.get("status") == "failed") else 0.0
    )


def _refresh_drift() -> None:
    drift = queries.latest_drift()
    if drift is not None:
        metrics.SPEED_BATCH_DRIFT.set(float(drift.get("drift_ratio") or 0.0))


def _refresh_alerts() -> None:
    metrics.OPEN_IDLE_ALERTS.set(float(queries.open_idle_alert_count()))
