"""SQL the serving layer runs (SPEC 9.2).

Kept separate from the route handlers so the queries can be read (and reviewed)
as a group. Two conventions run through all of them:

  * "now" is the latest SIMULATED event time the pipeline has seen, never
    wall-clock `now()`. The two run at different speeds, and using the database
    clock would make "this hour" mean a real hour, which is 60 simulated hours.
  * Everything is parameterised. No f-string SQL anywhere, even though the only
    caller is our own code.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from common import db


def sim_now() -> Optional[object]:
    """The newest simulated event time in the serving tables, or None."""
    row = db.query_one("SELECT max(last_event_time) AS ts FROM rt_vehicle_state")
    return row["ts"] if row else None


def fleet_live() -> Dict:
    row = db.query_one("SELECT * FROM v_fleet_live")
    return dict(row) if row else {}


def zones_live() -> List[Dict]:
    return [dict(r) for r in db.query("SELECT * FROM v_zone_live")]


def trips_in_hour(offset_hours: int = 0) -> int:
    """Completed trips in the current simulated hour, or a previous one.

    `date_trunc('hour', max(last_event_time))` is the current simulated hour;
    subtracting N hours walks back. Both are simulated times, so "last hour"
    means one simulated hour.
    """
    row = db.query_one(
        """
        WITH sim_now AS (SELECT max(last_event_time) AS ts FROM rt_vehicle_state)
        SELECT COALESCE(sum(z.trips_completed), 0) AS trips
        FROM rt_zone_hourly z, sim_now n
        WHERE z.window_start = date_trunc('hour', n.ts) - make_interval(hours => %s)
        """,
        (offset_hours,),
    )
    return int(row["trips"]) if row and row["trips"] is not None else 0


def earnings_by_zone_this_hour() -> List[Dict]:
    return [
        dict(r)
        for r in db.query(
            """
            WITH sim_now AS (SELECT max(last_event_time) AS ts FROM rt_vehicle_state)
            SELECT z.zone_id,
                   round(z.earnings::numeric, 2) AS earnings,
                   z.trips_completed AS trips
            FROM rt_zone_hourly z, sim_now n
            WHERE z.window_start = date_trunc('hour', n.ts)
            ORDER BY z.earnings DESC
            """
        )
    ]


def zone_hourly(zone_id: str, sim_date: Optional[str]) -> List[Dict]:
    """Hour-of-day profile for one zone on one simulated date."""
    if sim_date is None:
        row = db.query_one(
            "SELECT max(sim_date)::text AS d FROM rt_zone_hourly WHERE zone_id = %s",
            (zone_id,),
        )
        sim_date = row["d"] if row and row["d"] else None
    if sim_date is None:
        return []

    return [
        dict(r)
        for r in db.query(
            """
            SELECT sim_hour, window_start, trips_started, trips_completed,
                   round(earnings::numeric, 2) AS earnings, pings_total,
                   CASE WHEN pings_total > 0
                        THEN round(pings_on_trip::numeric / pings_total, 4)
                        ELSE 0 END AS on_trip_share
            FROM rt_zone_hourly
            WHERE zone_id = %s AND sim_date = %s
            ORDER BY window_start
            """,
            (zone_id, sim_date),
        )
    ], sim_date


def idle_alerts(status: Optional[str], limit: int = 200) -> List[Dict]:
    if status in ("open", "closed"):
        sql = ("SELECT vehicle_id, opened_at, closed_at, zone_id, idle_sim_minutes, "
               "status FROM idle_alerts WHERE status = %s "
               "ORDER BY opened_at DESC LIMIT %s")
        params = (status, limit)
    else:
        sql = ("SELECT vehicle_id, opened_at, closed_at, zone_id, idle_sim_minutes, "
               "status FROM idle_alerts ORDER BY opened_at DESC LIMIT %s")
        params = (limit,)
    return [dict(r) for r in db.query(sql, params)]


def open_idle_alert_count() -> int:
    row = db.query_one("SELECT count(*) AS n FROM idle_alerts WHERE status = 'open'")
    return int(row["n"]) if row else 0


# --- the Lambda merge ------------------------------------------------------

def vehicle_live(vehicle_id: str) -> Optional[Dict]:
    row = db.query_one(
        "SELECT * FROM rt_vehicle_state WHERE vehicle_id = %s", (vehicle_id,)
    )
    return dict(row) if row else None


def vehicle_today(vehicle_id: str) -> Optional[Dict]:
    """Today's running totals, where "today" is the CURRENT SIMULATED date."""
    row = db.query_one(
        """
        WITH sim_now AS (SELECT max(last_event_time) AS ts FROM rt_vehicle_state)
        SELECT d.vehicle_id, d.sim_date::text AS sim_date, d.trips,
               round(d.revenue::numeric, 2) AS revenue
        FROM rt_vehicle_daily d, sim_now n
        WHERE d.vehicle_id = %s AND d.sim_date = n.ts::date
        """,
        (vehicle_id,),
    )
    return dict(row) if row else None


def vehicle_history(vehicle_id: str, days: int = 7) -> List[Dict]:
    """The last N reconciled days from the BATCH layer."""
    return [
        dict(r)
        for r in db.query(
            "SELECT * FROM batch_vehicle_daily WHERE vehicle_id = %s "
            "ORDER BY sim_date DESC LIMIT %s",
            (vehicle_id, days),
        )
    ]


def vehicle_exists(vehicle_id: str) -> bool:
    """True if the vehicle appears in either layer.

    Checked across BOTH tables: a vehicle that is off shift has no live state but
    may well have a reconciled history, and answering 404 for it would be wrong.
    """
    row = db.query_one(
        "SELECT 1 AS found FROM rt_vehicle_state WHERE vehicle_id = %s "
        "UNION ALL SELECT 1 FROM batch_vehicle_daily WHERE vehicle_id = %s LIMIT 1",
        (vehicle_id, vehicle_id),
    )
    return row is not None


# --- batch layer -----------------------------------------------------------

def latest_reconciled_date() -> Optional[str]:
    row = db.query_one("SELECT max(sim_date)::text AS d FROM batch_vehicle_daily")
    return row["d"] if row and row["d"] else None


def unprofitable(sim_date: str) -> List[Dict]:
    """Vehicles needing attention on a reconciled date.

    Includes `becoming_unprofitable` deliberately: the business question asks
    which vehicles are BECOMING unprofitable, so a vehicle that is still just
    about profitable but sliding is exactly what should be surfaced.
    """
    return [
        dict(r)
        for r in db.query(
            """
            SELECT * FROM batch_vehicle_daily
            WHERE sim_date = %s
              AND (unprofitable OR becoming_unprofitable OR low_margin
                   OR distance_mismatch OR missing_costs OR missing_telemetry)
            ORDER BY net_profit ASC
            """,
            (sim_date,),
        )
    ]


def reconciled_dates(limit: int = 30) -> List[str]:
    return [
        r["d"]
        for r in db.query(
            "SELECT DISTINCT sim_date::text AS d FROM batch_vehicle_daily "
            "ORDER BY d DESC LIMIT %s",
            (limit,),
        )
    ]


def last_batch_run() -> Optional[Dict]:
    row = db.query_one(
        "SELECT run_id, sim_date::text AS sim_date, expense_file_version, status, "
        "expense_file_late, quarantined_rows, clean_rows, vehicles_out, "
        "task_durations, started_at, finished_at, notes "
        "FROM batch_runs ORDER BY started_at DESC LIMIT 1"
    )
    return dict(row) if row else None


def last_successful_run() -> Optional[Dict]:
    row = db.query_one(
        "SELECT run_id, sim_date::text AS sim_date, expense_file_version, "
        "task_durations, quarantined_rows, expense_file_late, "
        "extract(epoch FROM finished_at) AS finished_epoch "
        "FROM batch_runs WHERE status = 'success' "
        "ORDER BY finished_at DESC NULLS LAST LIMIT 1"
    )
    return dict(row) if row else None


def latest_drift() -> Optional[Dict]:
    row = db.query_one(
        "SELECT sim_date::text AS sim_date, speed_revenue, batch_revenue, "
        "drift_abs, drift_ratio, speed_trips, batch_trips "
        "FROM speed_batch_drift ORDER BY sim_date DESC LIMIT 1"
    )
    return dict(row) if row else None


def dq_issue_count(run_id: Optional[str]) -> int:
    if not run_id:
        return 0
    row = db.query_one(
        "SELECT count(*) AS n FROM dq_issues WHERE run_id = %s", (run_id,)
    )
    return int(row["n"]) if row else 0


def store_alert(alert: Dict) -> None:
    """Persist one Alertmanager notification."""
    import json

    labels = alert.get("labels") or {}
    annotations = alert.get("annotations") or {}
    db.execute(
        "INSERT INTO alert_notifications "
        "(fingerprint, alertname, severity, status, summary, description, "
        " starts_at, ends_at, labels) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)",
        (
            alert.get("fingerprint"),
            labels.get("alertname", "unknown"),
            labels.get("severity"),
            alert.get("status"),
            annotations.get("summary"),
            annotations.get("description"),
            alert.get("startsAt") if alert.get("startsAt", "").startswith("2") else None,
            alert.get("endsAt") if alert.get("endsAt", "").startswith("2") else None,
            json.dumps(labels),
        ),
    )
