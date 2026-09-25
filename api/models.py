"""Pydantic response models (SPEC 9.2).

Every response that mixes the two layers carries an explicit `source` and `as_of`.
That is not decoration: in a Lambda architecture the same question has two answers
with different freshness and different accuracy, and an API that blurs them is
exactly how a fleet manager ends up quoting a dashboard figure as a financial one.

  source="speed"  approximate, seconds old, may be missing late events
  source="batch"  exact, complete, recomputable, only after the day closes
"""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field

Source = Literal["speed", "batch"]


class HealthResponse(BaseModel):
    status: str = Field(description="ok | degraded")
    database: str
    sim_time: Optional[datetime] = Field(
        None, description="Current SIMULATED time; the clock every window uses"
    )
    sim_date: Optional[str] = None
    compression: Optional[int] = Field(
        None, description="Simulated seconds per real second"
    )
    version: str


class ZoneEarnings(BaseModel):
    zone_id: str
    earnings: float
    trips: int


class FleetLive(BaseModel):
    """GET /fleet/live -- entirely speed layer."""

    source: Source = "speed"
    as_of: Optional[datetime] = Field(
        None, description="Latest simulated event time the pipeline has seen"
    )
    active_vehicles: int
    idle_vehicles: int
    on_trip_vehicles: int
    enroute_vehicles: int
    idle_ratio: float
    open_idle_alerts: int
    trips_this_hour: int
    trips_last_hour: int
    earnings_by_zone: List[ZoneEarnings]
    currency: str


class ZoneLive(BaseModel):
    zone_id: str
    active_vehicles: int
    idle_vehicles: int
    on_trip_vehicles: int
    idle_ratio: float
    trips_this_hour: int
    earnings_this_hour: float


class ZonesLive(BaseModel):
    source: Source = "speed"
    as_of: Optional[datetime] = None
    zones: List[ZoneLive]
    currency: str


class ZoneHour(BaseModel):
    sim_hour: int
    window_start: datetime
    trips_started: int
    trips_completed: int
    earnings: float
    pings_total: int
    on_trip_share: float = Field(
        description="Share of pings in this zone-hour that were on_trip. A "
                    "utilization PROXY: exact distinct-vehicle counts are not "
                    "supported in streaming aggregations."
    )


class ZoneHourly(BaseModel):
    source: Source = "speed"
    zone_id: str
    sim_date: str
    hours: List[ZoneHour]
    currency: str


class IdleAlert(BaseModel):
    vehicle_id: str
    opened_at: datetime
    closed_at: Optional[datetime]
    zone_id: Optional[str]
    idle_sim_minutes: float
    status: str


class IdleAlerts(BaseModel):
    source: Source = "speed"
    threshold_sim_minutes: int
    count: int
    alerts: List[IdleAlert]


class VehicleLiveBlock(BaseModel):
    """The speed-layer half of the Lambda merge."""

    source: Source = "speed"
    as_of: Optional[datetime] = None
    status: Optional[str] = None
    zone_id: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    speed: Optional[float] = None
    trip_id: Optional[str] = None
    idle_sim_minutes: float = 0.0
    alert_open: bool = False
    note: str = Field(
        "Approximate. The speed layer applies a watermark and may not include "
        "events that arrived late.",
        description="Why this block is not authoritative",
    )


class VehicleTodayBlock(BaseModel):
    """Running totals for the CURRENT simulated day, also speed layer."""

    source: Source = "speed"
    sim_date: Optional[str] = None
    trips: int = 0
    revenue: float = 0.0
    note: str = (
        "Running total for the simulated day so far. Superseded by the batch "
        "figure once the day closes and is reconciled."
    )


class VehicleDayBlock(BaseModel):
    """One reconciled day -- the batch-layer half of the merge."""

    source: Source = "batch"
    sim_date: str
    as_of: Optional[datetime] = None
    trips: int
    revenue: float
    gps_km: float
    distance_covered: float
    fuel_cost: float
    maintenance_cost: float
    cost: float
    net_profit: float
    margin: float
    revenue_per_km: float
    cost_per_km: float
    utilization: float
    unprofitable: bool
    low_margin: bool
    becoming_unprofitable: bool
    distance_mismatch: bool
    in_service: bool
    missing_costs: bool
    missing_telemetry: bool
    expense_file_version: int
    reasons: List[str] = []


class VehicleDetail(BaseModel):
    """GET /vehicles/{id} -- THE Lambda merge (SPEC 9.2).

    Three blocks, each labelled with its source, deliberately NOT blended into
    one number. The live state and today's running earnings come from the speed
    layer; the last 7 days of profitability come from the batch layer. A caller
    can see at a glance which figures they may quote to finance.
    """

    vehicle_id: str
    live: VehicleLiveBlock
    today: VehicleTodayBlock
    history: List[VehicleDayBlock]
    currency: str
    lambda_note: str = (
        "Live state and today's earnings are SPEED-layer figures: fast but "
        "approximate. The history is BATCH-layer: exact, complete and "
        "recomputable. They are reported separately on purpose."
    )


class UnprofitableVehicle(BaseModel):
    vehicle_id: str
    revenue: float
    cost: float
    net_profit: float
    margin: float
    utilization: float
    unprofitable: bool
    low_margin: bool
    becoming_unprofitable: bool
    distance_mismatch: bool
    in_service: bool
    reasons: List[str]


class UnprofitableResponse(BaseModel):
    source: Source = "batch"
    sim_date: str
    threshold_margin: float
    count: int
    vehicles: List[UnprofitableVehicle]
    currency: str


class PipelineStatus(BaseModel):
    """GET /pipeline/status -- one call that says whether the pipeline is well."""

    sim_time: Optional[datetime]
    latest_event_time: Optional[datetime] = Field(
        None, description="Newest simulated event time the speed layer has seen"
    )
    lag_sim_minutes: Optional[float] = Field(
        None, description="How far behind simulated now the pipeline is"
    )
    last_batch_run: Optional[Dict] = None
    last_successful_sim_date: Optional[str] = None
    drift: Optional[Dict] = Field(
        None, description="Speed vs batch revenue. Non-zero is EXPECTED."
    )
    open_idle_alerts: int = 0
    dq_issues_last_run: int = 0
    reconciled_dates: List[str] = []


class AlertWebhookResponse(BaseModel):
    received: int
    stored: int
