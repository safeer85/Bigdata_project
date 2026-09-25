"""FastAPI serving layer (SPEC 9.2).

The endpoint that matters most is `GET /vehicles/{vehicle_id}`: it is where the
two layers of the Lambda architecture meet, and it deliberately does NOT blend
them into one number. Live state and today's running earnings come from the speed
layer; the last seven days of profitability come from the batch layer; each block
says which it is and how fresh it is.

"Today" always means the current SIMULATED date, everywhere.
"""
from __future__ import annotations

import os
import time
from datetime import timedelta
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    generate_latest,
)

from common import __version__, config, db, profitability, simclock
from common.logging import get_logger
from api import batch_metrics, models, queries

log = get_logger("api", stage="serving")

app = FastAPI(
    title="Fleet operations API",
    version=__version__,
    description=(
        "Serving layer for a Lambda-architecture fleet pipeline.\n\n"
        "**Speed layer** (`rt_*` tables) answers live utilization questions: fast, "
        "approximate, may miss events that arrived after the watermark.\n\n"
        "**Batch layer** (`batch_*` tables) answers financial questions: exact, "
        "complete, recomputable when a partner resubmits a corrected file.\n\n"
        "Every response that mixes them labels each block with its `source`."
    ),
)

# --- HTTP metrics (SPEC 10.2 "Serving") ------------------------------------
REQUESTS = Counter(
    "fleet_api_requests_total", "API requests", ["method", "path", "status"]
)
LATENCY = Histogram(
    "fleet_api_request_duration_seconds",
    "API request latency",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)


@app.middleware("http")
async def record_metrics(request: Request, call_next):
    """Count and time every request.

    Labelled with the ROUTE TEMPLATE (`/vehicles/{vehicle_id}`), not the raw path.
    Using the raw path would create one time series per vehicle id, which is the
    classic way to blow up a Prometheus instance with unbounded cardinality.
    """
    started = time.perf_counter()
    response = await call_next(request)

    route = request.scope.get("route")
    path = getattr(route, "path", request.url.path)

    LATENCY.labels(method=request.method, path=path).observe(
        time.perf_counter() - started
    )
    REQUESTS.labels(
        method=request.method, path=path, status=str(response.status_code)
    ).inc()
    return response


def _sim_now():
    try:
        return simclock.now_sim()
    except Exception:  # noqa: BLE001 - the clock file may not be mounted yet
        return None


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health", response_model=models.HealthResponse, tags=["ops"])
def health() -> models.HealthResponse:
    """Service and database health. Also the container's healthcheck target."""
    database = "ok"
    try:
        db.query_one("SELECT 1 AS ok")
    except Exception as exc:  # noqa: BLE001
        database = f"error: {exc}"

    sim_time = _sim_now()
    return models.HealthResponse(
        status="ok" if database == "ok" else "degraded",
        database=database,
        sim_time=sim_time,
        sim_date=simclock.sim_date(sim_time) if sim_time else None,
        compression=config.COMPRESSION,
        version=__version__,
    )


# ---------------------------------------------------------------------------
# Speed layer
# ---------------------------------------------------------------------------

@app.get("/fleet/live", response_model=models.FleetLive, tags=["speed layer"])
def fleet_live() -> models.FleetLive:
    """Active vehicles, idle ratio, trips this and last simulated hour, and
    earnings by zone. Entirely speed layer, so seconds old and approximate."""
    live = queries.fleet_live()
    return models.FleetLive(
        as_of=live.get("as_of"),
        active_vehicles=int(live.get("active_vehicles") or 0),
        idle_vehicles=int(live.get("idle_vehicles") or 0),
        on_trip_vehicles=int(live.get("on_trip_vehicles") or 0),
        enroute_vehicles=int(live.get("enroute_vehicles") or 0),
        idle_ratio=float(live.get("idle_ratio") or 0.0),
        open_idle_alerts=queries.open_idle_alert_count(),
        trips_this_hour=queries.trips_in_hour(0),
        trips_last_hour=queries.trips_in_hour(1),
        earnings_by_zone=[
            models.ZoneEarnings(
                zone_id=r["zone_id"],
                earnings=float(r["earnings"]),
                trips=int(r["trips"]),
            )
            for r in queries.earnings_by_zone_this_hour()
        ],
        currency=config.CURRENCY_LABEL,
    )


@app.get("/zones/live", response_model=models.ZonesLive, tags=["speed layer"])
def zones_live() -> models.ZonesLive:
    """Per zone: active vehicles, idle ratio, trips and earnings this hour."""
    rows = queries.zones_live()
    return models.ZonesLive(
        as_of=rows[0]["as_of"] if rows else None,
        zones=[
            models.ZoneLive(
                zone_id=r["zone_id"],
                active_vehicles=int(r["active_vehicles"]),
                idle_vehicles=int(r["idle_vehicles"]),
                on_trip_vehicles=int(r["on_trip_vehicles"]),
                idle_ratio=float(r["idle_ratio"]),
                trips_this_hour=int(r["trips_this_hour"]),
                earnings_this_hour=float(r["earnings_this_hour"]),
            )
            for r in rows
        ],
        currency=config.CURRENCY_LABEL,
    )


@app.get("/zones/{zone_id}/hourly", response_model=models.ZoneHourly,
         tags=["speed layer"])
def zone_hourly(
    zone_id: str,
    sim_date: Optional[str] = Query(
        None, description="Simulated date YYYY-MM-DD. Defaults to the latest."
    ),
) -> models.ZoneHourly:
    """Hour-of-day earnings and trip profile for one zone."""
    rows, resolved = queries.zone_hourly(zone_id, sim_date)
    if not rows:
        raise HTTPException(404, f"no hourly data for zone {zone_id}")
    return models.ZoneHourly(
        zone_id=zone_id,
        sim_date=resolved,
        hours=[models.ZoneHour(**r) for r in rows],
        currency=config.CURRENCY_LABEL,
    )


@app.get("/alerts/idle", response_model=models.IdleAlerts, tags=["speed layer"])
def idle_alerts(
    status: Optional[str] = Query("open", description="open | closed | all"),
) -> models.IdleAlerts:
    """Idle alerts raised by the speed layer's per-vehicle state machine."""
    rows = queries.idle_alerts(None if status == "all" else status)
    return models.IdleAlerts(
        threshold_sim_minutes=config.IDLE_ALERT_SIM_MIN,
        count=len(rows),
        alerts=[models.IdleAlert(**r) for r in rows],
    )


# ---------------------------------------------------------------------------
# THE LAMBDA MERGE
# ---------------------------------------------------------------------------

@app.get("/vehicles/unprofitable", response_model=models.UnprofitableResponse,
         tags=["batch layer"])
def unprofitable(
    sim_date: Optional[str] = Query(
        None, description="Simulated date. Defaults to the latest reconciled date."
    ),
) -> models.UnprofitableResponse:
    """Vehicles needing attention on a reconciled day.

    Declared BEFORE `/vehicles/{vehicle_id}`: FastAPI matches routes in
    declaration order, so the dynamic route would otherwise swallow
    `/vehicles/unprofitable` and look for a vehicle called "unprofitable".
    """
    sim_date = sim_date or queries.latest_reconciled_date()
    if sim_date is None:
        raise HTTPException(
            404,
            "no simulated day has been reconciled yet. A simulated day takes "
            "24 real minutes; the batch layer runs once it has closed.",
        )

    rows = queries.unprofitable(sim_date)
    return models.UnprofitableResponse(
        sim_date=sim_date,
        threshold_margin=config.MARGIN_THRESHOLD,
        count=len(rows),
        vehicles=[
            models.UnprofitableVehicle(
                vehicle_id=r["vehicle_id"],
                revenue=float(r["revenue"]),
                cost=float(r["cost"]),
                net_profit=float(r["net_profit"]),
                margin=float(r["margin"]),
                utilization=float(r["utilization"]),
                unprofitable=r["unprofitable"],
                low_margin=r["low_margin"],
                becoming_unprofitable=r["becoming_unprofitable"],
                distance_mismatch=r["distance_mismatch"],
                in_service=r["in_service"],
                reasons=profitability.reasons_for(r),
            )
            for r in rows
        ],
        currency=config.CURRENCY_LABEL,
    )


@app.get("/vehicles/{vehicle_id}", response_model=models.VehicleDetail,
         tags=["lambda merge"])
def vehicle_detail(
    vehicle_id: str,
    days: int = Query(7, ge=1, le=60, description="Days of batch history"),
) -> models.VehicleDetail:
    """**The Lambda merge.** Speed-layer live state and today's running earnings,
    plus batch-layer profitability for the last N reconciled days.

    Each block carries its own `source` and `as_of`. They are reported side by
    side rather than merged into one figure, because they answer different
    questions with different guarantees: the speed blocks are fast but may be
    missing late events, the batch blocks are exact but only exist once a
    simulated day has closed and been reconciled.
    """
    if not queries.vehicle_exists(vehicle_id):
        raise HTTPException(404, f"unknown vehicle {vehicle_id}")

    live_row = queries.vehicle_live(vehicle_id)
    today_row = queries.vehicle_today(vehicle_id)
    history_rows = queries.vehicle_history(vehicle_id, days)

    live = models.VehicleLiveBlock()
    if live_row:
        live = models.VehicleLiveBlock(
            as_of=live_row["last_event_time"],
            status=live_row["status"],
            zone_id=live_row["zone_id"],
            lat=live_row["lat"],
            lon=live_row["lon"],
            speed=live_row["speed"],
            trip_id=live_row["trip_id"],
            idle_sim_minutes=float(live_row["idle_sim_minutes"] or 0.0),
            alert_open=bool(live_row["alert_open"]),
        )

    today = models.VehicleTodayBlock()
    if today_row:
        today = models.VehicleTodayBlock(
            sim_date=today_row["sim_date"],
            trips=int(today_row["trips"]),
            revenue=float(today_row["revenue"]),
        )

    history = [
        models.VehicleDayBlock(
            sim_date=str(r["sim_date"]),
            as_of=r["computed_at"],
            trips=int(r["trips"]),
            revenue=float(r["revenue"]),
            gps_km=float(r["gps_km"]),
            distance_covered=float(r["distance_covered"]),
            fuel_cost=float(r["fuel_cost"]),
            maintenance_cost=float(r["maintenance_cost"]),
            cost=float(r["cost"]),
            net_profit=float(r["net_profit"]),
            margin=float(r["margin"]),
            revenue_per_km=float(r["revenue_per_km"]),
            cost_per_km=float(r["cost_per_km"]),
            utilization=float(r["utilization"]),
            unprofitable=r["unprofitable"],
            low_margin=r["low_margin"],
            becoming_unprofitable=r["becoming_unprofitable"],
            distance_mismatch=r["distance_mismatch"],
            in_service=r["in_service"],
            missing_costs=r["missing_costs"],
            missing_telemetry=r["missing_telemetry"],
            expense_file_version=int(r["expense_file_version"]),
            reasons=profitability.reasons_for(r),
        )
        for r in history_rows
    ]

    return models.VehicleDetail(
        vehicle_id=vehicle_id,
        live=live,
        today=today,
        history=history,
        currency=config.CURRENCY_LABEL,
    )


# ---------------------------------------------------------------------------
# Reports and pipeline status
# ---------------------------------------------------------------------------

@app.get("/reports/{sim_date}", tags=["batch layer"], response_class=FileResponse)
def report(sim_date: str, fmt: str = Query("html", description="html | csv")):
    """Serve the generated profitability report for a simulated date."""
    suffix = "csv" if fmt == "csv" else "html"
    path = os.path.join(config.REPORTS_DIR, f"profitability_{sim_date}.{suffix}")
    if not os.path.exists(path):
        available = sorted(
            f.replace("profitability_", "").replace(".html", "")
            for f in os.listdir(config.REPORTS_DIR)
            if f.endswith(".html")
        ) if os.path.isdir(config.REPORTS_DIR) else []
        raise HTTPException(
            404, f"no report for {sim_date}. Available: {available or 'none yet'}"
        )
    return FileResponse(
        path,
        media_type="text/csv" if suffix == "csv" else "text/html",
        filename=os.path.basename(path) if suffix == "csv" else None,
    )


@app.get("/pipeline/status", response_model=models.PipelineStatus, tags=["ops"])
def pipeline_status() -> models.PipelineStatus:
    """One call that says whether the whole pipeline is healthy."""
    sim_time = _sim_now()
    latest_event = queries.sim_now()

    lag = None
    if sim_time and latest_event:
        # How far behind simulated "now" the freshest processed event is. This is
        # end-to-end pipeline lag expressed in simulated minutes, which is the
        # unit every threshold in the project uses.
        lag = round((sim_time - latest_event).total_seconds() / 60.0, 2)

    last_run = queries.last_batch_run()
    last_ok = queries.last_successful_run()

    return models.PipelineStatus(
        sim_time=sim_time,
        latest_event_time=latest_event,
        lag_sim_minutes=lag,
        last_batch_run=last_run,
        last_successful_sim_date=(last_ok or {}).get("sim_date"),
        drift=queries.latest_drift(),
        open_idle_alerts=queries.open_idle_alert_count(),
        dq_issues_last_run=queries.dq_issue_count((last_run or {}).get("run_id")),
        reconciled_dates=queries.reconciled_dates(),
    )


# ---------------------------------------------------------------------------
# Alertmanager receiver
# ---------------------------------------------------------------------------

@app.post("/alerts/webhook", response_model=models.AlertWebhookResponse, tags=["ops"])
async def alerts_webhook(request: Request) -> models.AlertWebhookResponse:
    """Receive Alertmanager notifications and store them (SPEC 10.3).

    Routing alerts here makes DELIVERY visible in the demo: a single SQL query
    over `alert_notifications` shows that the alert did not merely fire in
    Prometheus but actually reached a receiver.
    """
    payload = await request.json()
    alerts = payload.get("alerts", [])

    stored = 0
    for alert in alerts:
        try:
            queries.store_alert(alert)
            stored += 1
        except Exception:  # noqa: BLE001 - never NACK an alert over a write error
            log.warning(
                "could not store an alert notification",
                extra={"event": "alert_store_failed",
                       "alertname": (alert.get("labels") or {}).get("alertname")},
            )

    for alert in alerts:
        labels = alert.get("labels") or {}
        log.warning(
            "alert received from Alertmanager",
            extra={
                "event": "alert_received",
                "alertname": labels.get("alertname"),
                "severity": labels.get("severity"),
                "status": alert.get("status"),
                "summary": (alert.get("annotations") or {}).get("summary"),
            },
        )

    return models.AlertWebhookResponse(received=len(alerts), stored=stored)


# ---------------------------------------------------------------------------
# Prometheus
# ---------------------------------------------------------------------------

@app.get("/metrics", tags=["ops"], response_class=PlainTextResponse)
def prometheus_metrics() -> Response:
    """Prometheus scrape endpoint.

    Batch metrics are refreshed from PostgreSQL on each scrape rather than pushed
    by Airflow: the tasks that produce them are short-lived processes that
    Prometheus could never catch. See api/batch_metrics.py.
    """
    batch_metrics.refresh()
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/", include_in_schema=False)
def root() -> JSONResponse:
    return JSONResponse(
        {
            "service": "fleet operations API",
            "version": __version__,
            "docs": "/docs",
            "speed_layer": ["/fleet/live", "/zones/live", "/zones/{zone_id}/hourly",
                            "/alerts/idle"],
            "batch_layer": ["/vehicles/unprofitable", "/reports/{sim_date}"],
            "lambda_merge": ["/vehicles/{vehicle_id}"],
            "ops": ["/health", "/pipeline/status", "/metrics", "/alerts/webhook"],
        }
    )
