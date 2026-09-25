"""Prometheus metric definitions (SPEC 10.2). Every name is prefixed `fleet_`.

All metrics live in one module so that names, labels and help strings are decided
once and the Prometheus scrape config, the alert rules and the Grafana dashboards
can be written against a single list.

Note what is NOT here: batch metrics. Airflow tasks are short-lived processes, so
scraping them directly would miss them. Rather than run a Pushgateway (one more
service, and a well-known anti-pattern for job-level state), the API exposes batch
metrics by reading `batch_runs` and `speed_batch_drift` from PostgreSQL. See
`api/metrics.py` and docs/decisions.md.
"""
from __future__ import annotations

from typing import Optional

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# --- Ingestion (simulators) ------------------------------------------------
EVENTS_PRODUCED = Counter(
    "fleet_events_produced_total",
    "Telemetry events successfully produced to Kafka",
    ["event_type"],
)
PRODUCE_ERRORS = Counter(
    "fleet_produce_errors_total",
    "Kafka produce failures (after retries)",
)
FAULTS_INJECTED = Counter(
    "fleet_faults_injected_total",
    "Deliberately injected faults, by kind",
    ["kind"],
)
EXPENSE_FILES_WRITTEN = Counter(
    "fleet_expense_files_written_total",
    "Daily expense CSV files written",
    ["version"],
)
SIM_TIME_GAUGE = Gauge(
    "fleet_sim_time_seconds",
    "Current simulated time as a unix timestamp (lets Grafana show the sim clock)",
)
VEHICLES_ONLINE = Gauge(
    "fleet_sim_vehicles_online",
    "Vehicles currently on shift in the simulator",
)

# --- Processing (Spark query listener) -------------------------------------
STREAM_INPUT_RPS = Gauge(
    "fleet_stream_input_rows_per_second",
    "Rows arriving per second, per streaming query",
    ["query"],
)
STREAM_PROCESSED_RPS = Gauge(
    "fleet_stream_processed_rows_per_second",
    "Rows processed per second, per streaming query",
    ["query"],
)
STREAM_BATCH_DURATION = Histogram(
    "fleet_stream_batch_duration_seconds",
    "Micro-batch wall-clock duration, per streaming query",
    ["query"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)
STREAM_LAST_PROGRESS = Gauge(
    "fleet_stream_last_progress_timestamp",
    "Unix time of the last micro-batch, per query (drives the StreamStalled alert)",
    ["query"],
)
STREAM_OFFSETS_BEHIND = Gauge(
    "fleet_stream_offsets_behind_latest",
    "Kafka offsets behind latest, per query. Spark does not commit consumer-group "
    "offsets, so an ordinary Kafka lag exporter cannot see this: it is read out of "
    "the query progress sourced metrics instead.",
    ["query"],
)
LATE_ROWS_DROPPED = Counter(
    "fleet_late_rows_dropped_total",
    "Rows dropped by a watermark (numRowsDroppedByWatermark). Expected to be "
    "non-zero: ~2% of events are injected late on purpose.",
    ["query"],
)
DLQ_EVENTS = Counter(
    "fleet_dlq_events_total",
    "Events routed to the dead-letter topic",
    ["reason"],
)

# --- Storage ---------------------------------------------------------------
DB_UPSERT_ROWS = Counter(
    "fleet_db_upsert_rows_total",
    "Rows upserted into PostgreSQL, by table",
    ["table"],
)
DB_WRITE_ERRORS = Counter(
    "fleet_db_write_errors_total",
    "PostgreSQL write failures",
    ["table"],
)

# --- Serving and batch metrics live in api/batch_metrics.py, NOT here -------
#
# They used to be defined in this module, and that was a real bug. Every service
# imports `common.metrics`, so every service exported `fleet_batch_last_success_
# timestamp` at its default value of 0. Prometheus then saw FIVE series for it --
# four of them permanently zero -- and `BatchNotRunRecently` fired forever on the
# bogus ones, because `time() - 0 > 2700` is always true.
#
# A permanently-firing false alert is worse than no alert: it teaches people to
# ignore the channel. Metrics that only ONE service can meaningfully report are
# now defined in that service, so no other process can publish a series for them.

def serve(port: int) -> None:
    """Start the Prometheus scrape endpoint on `port` in a background thread."""
    start_http_server(port)


def set_sim_time(sim_time: Optional[object] = None) -> None:
    """Publish the current simulated time so dashboards can display it."""
    try:
        from common import simclock

        value = sim_time or simclock.now_sim()
        SIM_TIME_GAUGE.set(value.timestamp())  # type: ignore[union-attr]
    except Exception:  # noqa: BLE001 - metrics must never break the caller
        pass
