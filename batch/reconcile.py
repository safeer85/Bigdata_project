"""`reconcile_profitability`, `load_batch_views` and `compute_drift`
(SPEC 8.2 tasks 5, 6 and 7).

This is where the business question is finally answered: "which vehicles are
becoming unprofitable once yesterday's fuel and maintenance costs are factored
in?" The arithmetic and every flag live in `common/profitability.py`, shared with
the API and the tests; this module only joins the two sources and persists the
result.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from common import config, db, profitability
from common.logging import get_logger

log = get_logger("batch", stage="processing")

# Columns written to batch_vehicle_daily, in schema order.
OUTPUT_COLUMNS = [
    "vehicle_id", "sim_date", "trips", "revenue", "gps_km",
    "online_min", "on_trip_min", "idle_min", "utilization",
    "fuel_cost", "maintenance_cost", "distance_covered",
    "cost", "net_profit", "margin", "revenue_per_km", "cost_per_km",
    "distance_mismatch_pct",
    "unprofitable", "low_margin", "becoming_unprofitable", "distance_mismatch",
    "in_service", "missing_costs", "missing_telemetry",
    "expense_file_version", "run_id",
]


def net_profit_history(vehicle_id: str, sim_date: str, days: int = 3) -> List[float]:
    """The vehicle's net profit on the `days` reconciled days BEFORE sim_date.

    Read from `batch_vehicle_daily`, i.e. from previously reconciled days, not
    from a rolling in-memory state. That keeps the trend flag rerunnable: a
    recompute of an old day sees the same history it saw the first time.

    Returned OLDEST first, because that is the order
    `profitability.becoming_unprofitable` expects.
    """
    rows = db.query(
        "SELECT net_profit FROM batch_vehicle_daily "
        "WHERE vehicle_id = %s AND sim_date < %s "
        "ORDER BY sim_date DESC LIMIT %s",
        (vehicle_id, sim_date, days),
    )
    return [float(row["net_profit"]) for row in reversed(rows)]


def reconcile(
    sim_date: str,
    run_id: str,
    expense_version: int,
    telemetry: List[Dict],
    expenses: List[Dict],
) -> List[Dict]:
    """Full outer join of telemetry-days and expense rows, then flag each vehicle.

    A FULL OUTER join, deliberately. An inner join would silently drop exactly
    the two cases the business most needs to see:
      * telemetry but no expense row  -> `missing_costs` (we are flying blind on
        this vehicle's costs)
      * an expense row but no telemetry -> `missing_telemetry` (we are being
        billed for a vehicle that never reported)
    """
    telemetry_by_id = {row["vehicle_id"]: row for row in telemetry}
    expenses_by_id = {row["vehicle_id"]: row for row in expenses}
    all_ids = sorted(set(telemetry_by_id) | set(expenses_by_id))

    results: List[Dict] = []
    for vehicle_id in all_ids:
        tele = telemetry_by_id.get(vehicle_id)
        exp = expenses_by_id.get(vehicle_id)

        result = profitability.reconcile(
            vehicle_id=vehicle_id,
            sim_date=sim_date,
            revenue=tele["revenue"] if tele else 0.0,
            gps_km=tele["gps_km"] if tele else 0.0,
            fuel_cost=exp["fuel_cost"] if exp else 0.0,
            maintenance_cost=exp["maintenance_cost"] if exp else 0.0,
            distance_covered=exp["distance_covered"] if exp else 0.0,
            service_flag=exp["service_flag"] if exp else False,
            has_telemetry=tele is not None,
            has_expenses=exp is not None,
        ).as_dict()

        # The trend flag needs history, so it is applied here rather than inside
        # `profitability.reconcile`, which is a pure function of one day.
        history = net_profit_history(vehicle_id, sim_date)
        result["becoming_unprofitable"] = profitability.becoming_unprofitable(
            history + [result["net_profit"]]
        )

        result.update(
            {
                "trips": tele["trips"] if tele else 0,
                "online_min": tele["online_min"] if tele else 0.0,
                "on_trip_min": tele["on_trip_min"] if tele else 0.0,
                "idle_min": tele["idle_min"] if tele else 0.0,
                "utilization": tele["utilization"] if tele else 0.0,
                "expense_file_version": expense_version,
                "run_id": run_id,
            }
        )
        results.append({column: result[column] for column in OUTPUT_COLUMNS})

    flagged = {
        flag: sum(1 for r in results if r[flag])
        for flag in ("unprofitable", "low_margin", "becoming_unprofitable",
                     "distance_mismatch", "in_service", "missing_costs",
                     "missing_telemetry")
    }
    log.info(
        "profitability reconciled",
        extra={
            "event": "profitability_reconciled",
            "run_id": run_id,
            "sim_date": sim_date,
            "vehicles": len(results),
            "fleet_net_profit": round(sum(r["net_profit"] for r in results), 2),
            "flags": flagged,
        },
    )
    return results


def load_batch_views(sim_date: str, rows: List[Dict]) -> int:
    """Delete-then-insert one simulated date in a single transaction (task 6).

    This is what makes a rerun idempotent. After `make demo-resubmit` drops a
    corrected v2 file, this must leave the SAME number of rows with UPDATED
    values -- never a mixture of old and new. Both statements run in one
    transaction, so a reader never sees the date half-deleted either.
    """
    written = db.replace_date_partition("batch_vehicle_daily", sim_date, rows)
    log.info(
        "batch views loaded",
        extra={"event": "batch_views_loaded", "sim_date": sim_date, "rows": written},
    )
    return written


def write_curated_parquet(sim_date: str, rows: List[Dict]) -> Optional[str]:
    """Optionally mirror the reconciled day into the lake (task 6).

    Useful because it makes the curated layer readable by anything that speaks
    Parquet, without going through PostgreSQL. Failures are logged and swallowed:
    the database copy is authoritative, and losing the mirror must not fail a
    financial reconciliation.
    """
    if not rows:
        return None
    try:
        import pandas as pd

        path = f"{config.CURATED_VEHICLE_DAILY_PATH}/sim_date={sim_date}"
        import os

        os.makedirs(path, exist_ok=True)
        target = f"{path}/part-000.parquet"
        pd.DataFrame(rows).to_parquet(target, index=False)
        log.info(
            "curated parquet written",
            extra={"event": "curated_written", "sim_date": sim_date, "path": target},
        )
        return target
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "could not write the curated parquet mirror (database copy is authoritative)",
            extra={"event": "curated_write_failed", "sim_date": sim_date,
                   "error": str(exc)},
        )
        return None


def compute_drift(sim_date: str, run_id: str) -> Dict:
    """Speed-layer revenue vs batch-layer revenue for the same day (task 7).

    THE NUMBER THIS PROJECT IS ABOUT. A non-zero drift is expected and is the
    measured evidence for the architecture argument:

      * the speed layer applies a 10-simulated-minute watermark, so it silently
        drops events that arrive later than that;
      * the simulator injects ~2% of events with 1-20 minutes of lateness;
      * so the speed layer misses some trip_end events, and its revenue for the
        day is slightly LOW.

    A small POSITIVE drift (batch above speed) is therefore healthy. A negative
    drift would mean the speed layer counted something twice -- an upsert bug --
    and a large drift would mean the archive is incomplete. Both are alerted on.
    """
    speed = db.query_one(
        "SELECT COALESCE(sum(revenue), 0) AS revenue, COALESCE(sum(trips), 0) AS trips "
        "FROM rt_vehicle_daily WHERE sim_date = %s",
        (sim_date,),
    ) or {"revenue": 0.0, "trips": 0}

    batch = db.query_one(
        "SELECT COALESCE(sum(revenue), 0) AS revenue, COALESCE(sum(trips), 0) AS trips "
        "FROM batch_vehicle_daily WHERE sim_date = %s",
        (sim_date,),
    ) or {"revenue": 0.0, "trips": 0}

    speed_revenue = float(speed["revenue"] or 0.0)
    batch_revenue = float(batch["revenue"] or 0.0)
    drift_abs = round(batch_revenue - speed_revenue, 2)
    # Relative to the BATCH figure, because the batch layer is the authoritative
    # one: the question is "what share of the true revenue did the speed layer
    # miss", not "by what share was the speed layer wrong about itself".
    drift_ratio = round(drift_abs / batch_revenue, 6) if batch_revenue else 0.0

    row = {
        "sim_date": sim_date,
        "speed_revenue": round(speed_revenue, 2),
        "batch_revenue": round(batch_revenue, 2),
        "drift_abs": drift_abs,
        "drift_ratio": drift_ratio,
        "speed_trips": int(speed["trips"] or 0),
        "batch_trips": int(batch["trips"] or 0),
        "run_id": run_id,
    }
    db.upsert("speed_batch_drift", [row], conflict_keys=["sim_date"])

    log.info(
        "speed/batch drift computed (non-zero is EXPECTED: watermark drops)",
        extra={"event": "drift_computed", **row},
    )
    return row
