"""`foreachBatch` sinks for the speed layer (SPEC 7.3 "sink correctness").

Every sink here upserts on a NATURAL key. That is the single most important
property of this file, so it is worth stating plainly:

    Structured Streaming's foreachBatch is AT-LEAST-ONCE.

If the driver dies after writing a micro-batch but before committing its offsets,
Spark re-runs that batch on restart and the sink sees the same rows twice. With
plain INSERTs every figure on the dashboard would drift upwards on every restart
and never come back. With `INSERT ... ON CONFLICT (key) DO UPDATE` the second
write sets the same values again and the result is identical -- the sink is
idempotent, which is how we get effectively-exactly-once semantics out of
at-least-once delivery.

The keys are:
    rt_vehicle_state   (vehicle_id)
    rt_zone_hourly     (zone_id, window_start)
    rt_vehicle_daily   (vehicle_id, sim_date)
    idle_alerts        (vehicle_id, opened_at)

Note also that each sink writes from the DRIVER, by collecting a small aggregated
DataFrame. That is deliberate: these outputs are at most a few hundred rows per
batch (50 vehicles, 16 zones), so a driver-side psycopg write is simpler and
faster than opening a connection per executor partition, and it keeps the upsert
SQL in one readable place.
"""
from __future__ import annotations

from typing import Dict, List

from common import db, metrics
from common.logging import get_logger

log = get_logger("speed", stage="storage")


def _rows_of(batch_df, columns: List[str]) -> List[Dict]:
    """Collect a micro-batch into plain dicts, keeping only `columns`."""
    return [
        {column: row[column] for column in columns}
        for row in batch_df.select(*columns).collect()
    ]


def write_vehicle_state(batch_df, batch_id: int) -> None:
    """Upsert the latest state of each vehicle, keyed on vehicle_id."""
    columns = [
        "vehicle_id", "driver_id", "status", "zone_id", "lat", "lon", "speed",
        "trip_id", "last_event_time", "idle_since", "idle_sim_minutes", "alert_open",
    ]
    rows = _rows_of(batch_df, columns)
    if not rows:
        return
    db.upsert("rt_vehicle_state", rows, conflict_keys=["vehicle_id"])
    log.info(
        "rt_vehicle_state upserted",
        extra={"event": "sink_write", "batch_id": batch_id,
               "table": "rt_vehicle_state", "rows": len(rows)},
    )


def write_idle_alerts(batch_df, batch_id: int) -> None:
    """Insert or update idle alerts, keyed on (vehicle_id, opened_at).

    Including `opened_at` in the key is what keeps alert HISTORY: a vehicle that
    goes idle, recovers and goes idle again gets two rows, not one overwritten
    row. Re-processing the same batch still hits the same (vehicle, opened_at)
    pair and merely rewrites it.
    """
    columns = ["vehicle_id", "opened_at", "closed_at", "zone_id",
               "idle_sim_minutes", "status"]
    rows = _rows_of(batch_df, columns)
    if not rows:
        return
    db.upsert("idle_alerts", rows, conflict_keys=["vehicle_id", "opened_at"])

    opened = sum(1 for r in rows if r["status"] == "open")
    closed = len(rows) - opened
    log.info(
        "idle alerts written",
        extra={"event": "idle_alerts_written", "batch_id": batch_id,
               "opened": opened, "closed": closed},
    )


def write_zone_hourly(batch_df, batch_id: int) -> None:
    """Upsert one row per (zone, simulated hour).

    The query runs in UPDATE output mode, so Spark re-emits a window every time it
    changes. Without the upsert key, an hour's earnings would be inserted once per
    micro-batch and the zone table would show roughly 12x the real figure.
    """
    columns = [
        "zone_id", "window_start", "window_end", "sim_date", "sim_hour",
        "trips_started", "trips_completed", "earnings",
        "pings_idle", "pings_enroute", "pings_on_trip", "pings_total",
    ]
    rows = _rows_of(batch_df, columns)
    if not rows:
        return
    db.upsert("rt_zone_hourly", rows, conflict_keys=["zone_id", "window_start"])
    log.info(
        "rt_zone_hourly upserted",
        extra={"event": "sink_write", "batch_id": batch_id,
               "table": "rt_zone_hourly", "rows": len(rows)},
    )


def write_vehicle_daily(batch_df, batch_id: int) -> None:
    """Upsert running per-vehicle daily totals, keyed on (vehicle_id, sim_date).

    This table is what `compute_drift` compares against the batch layer, so an
    upsert bug here would show up as a spurious drift figure and undermine the
    very argument the metric exists to make.
    """
    columns = ["vehicle_id", "sim_date", "trips", "revenue"]
    rows = _rows_of(batch_df, columns)
    if not rows:
        return
    db.upsert("rt_vehicle_daily", rows, conflict_keys=["vehicle_id", "sim_date"])
    log.info(
        "rt_vehicle_daily upserted",
        extra={"event": "sink_write", "batch_id": batch_id,
               "table": "rt_vehicle_daily", "rows": len(rows)},
    )


def count_dlq(batch_df, batch_id: int) -> None:
    """Count dead-lettered events by reason, for `fleet_dlq_events_total`.

    The DLQ rows themselves are written to Kafka by the streaming sink; this runs
    alongside purely to produce the metric, because a Kafka sink cannot increment
    a Prometheus counter on the driver.
    """
    rows = batch_df.groupBy("error_reason").count().collect()
    for row in rows:
        metrics.DLQ_EVENTS.labels(reason=row["error_reason"] or "unknown").inc(row["count"])
    if rows:
        log.warning(
            "events dead-lettered",
            extra={
                "event": "dlq_batch",
                "batch_id": batch_id,
                "by_reason": {r["error_reason"]: r["count"] for r in rows},
            },
        )
