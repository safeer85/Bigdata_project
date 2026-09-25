"""PySpark tests for the batch layer (SPEC 11).

The batch layer is the authoritative one -- its numbers go in a financial report
and are recomputed when a partner resubmits. These tests cover the three things
that would silently corrupt those numbers: duplicates, late events and GPS glitches.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from common import config, geo

pytest.importorskip("pyspark")

DAY = "2024-01-01"
BASE = datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc)


def _row(event_id, vehicle="V001", event_type="ping", status="idle", fare=0.0,
         lat=12.98, lon=77.60, minutes=0, speed=0.0):
    ts = BASE + timedelta(minutes=minutes)
    return {
        "event_id": event_id,
        "event_type": event_type,
        "trip_id": None if event_type == "ping" else "T1",
        "driver_id": "D001",
        "vehicle_id": vehicle,
        "lat": float(lat),
        "lon": float(lon),
        "speed": float(speed),
        "status": status,
        "fare": float(fare),
        "timestamp": ts.isoformat().replace("+00:00", "Z"),
        "schema_version": 1,
        "event_time": ts,
    }


def _archive(spark, rows):
    """Build a DataFrame shaped like the raw Parquet archive."""
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("event_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("trip_id", T.StringType()),
        T.StructField("driver_id", T.StringType()),
        T.StructField("vehicle_id", T.StringType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("speed", T.DoubleType()),
        T.StructField("status", T.StringType()),
        T.StructField("fare", T.DoubleType()),
        T.StructField("timestamp", T.StringType()),
        T.StructField("schema_version", T.IntegerType()),
        T.StructField("event_time", T.TimestampType()),
    ])
    return spark.createDataFrame(rows, schema)


def _compute(spark, rows, tmp_path):
    """Write rows as a real partition and run the real `compute` over them."""
    from batch import vehicle_day

    path = f"{tmp_path}/raw/telemetry/sim_date={DAY}"
    _archive(spark, rows).write.mode("overwrite").parquet(path)

    original = config.RAW_TELEMETRY_PATH
    config.RAW_TELEMETRY_PATH = f"{tmp_path}/raw/telemetry"
    vehicle_day.config.RAW_TELEMETRY_PATH = config.RAW_TELEMETRY_PATH
    try:
        return {r["vehicle_id"]: r for r in vehicle_day.compute(spark, DAY)}
    finally:
        config.RAW_TELEMETRY_PATH = original
        vehicle_day.config.RAW_TELEMETRY_PATH = original


# ---------------------------------------------------------------------------
# Duplicates
# ---------------------------------------------------------------------------

def test_duplicate_events_are_counted_once(spark, tmp_path):
    """The simulator duplicates ~1% of event_ids; the batch layer must dedup.

    Critically the batch layer dedups over the WHOLE day with no watermark, so it
    catches duplicates that arrived too late for the speed layer to notice.
    """
    rows = [
        _row("A", event_type="trip_end", status="on_trip", fare=200.0, minutes=10),
        _row("A", event_type="trip_end", status="on_trip", fare=200.0, minutes=10),
        _row("B", event_type="trip_end", status="on_trip", fare=150.0, minutes=20),
    ]
    result = _compute(spark, rows, str(tmp_path))
    assert result["V001"]["trips"] == 2
    assert result["V001"]["revenue"] == pytest.approx(350.0)


def test_a_duplicate_arriving_hours_later_is_still_caught(spark, tmp_path):
    """No watermark means no time limit on dedup -- the key batch-layer advantage."""
    rows = [
        _row("A", event_type="trip_end", status="on_trip", fare=200.0, minutes=0),
        # The same event_id, re-delivered 6 simulated hours later.
        _row("A", event_type="trip_end", status="on_trip", fare=200.0, minutes=360),
    ]
    result = _compute(spark, rows, str(tmp_path))
    assert result["V001"]["trips"] == 1
    assert result["V001"]["revenue"] == pytest.approx(200.0)


# ---------------------------------------------------------------------------
# Late events -- the consistency argument
# ---------------------------------------------------------------------------

def test_late_events_are_included_by_the_batch_layer(spark, tmp_path):
    """A 20-minute-late event is dropped by the speed layer and kept here.

    This is the whole reason the batch layer exists, so it gets an explicit test.
    The archive is read after the day closes plus the grace period, and the batch
    job applies no watermark at all, so lateness is simply irrelevant to it.
    """
    rows = [
        _row("ON_TIME", event_type="trip_end", status="on_trip", fare=100.0, minutes=0),
        # Event time 5 minutes in, but it reached Kafka 20 simulated minutes later.
        # The archive stores it under its EVENT time, which is what matters.
        _row("LATE", event_type="trip_end", status="on_trip", fare=250.0, minutes=5),
    ]
    result = _compute(spark, rows, str(tmp_path))
    assert result["V001"]["trips"] == 2
    assert result["V001"]["revenue"] == pytest.approx(350.0)


# ---------------------------------------------------------------------------
# Invalid events
# ---------------------------------------------------------------------------

def test_invalid_events_are_excluded_from_the_figures(spark, tmp_path):
    """The same validation the speed layer uses, re-applied to the archive."""
    rows = [
        _row("GOOD", event_type="trip_end", status="on_trip", fare=100.0, minutes=0),
        # Outside the city bounding box -> rejected.
        _row("BAD_COORDS", event_type="trip_end", status="on_trip", fare=500.0,
             lat=config.CITY_LAT_MAX + 5, minutes=5),
        # Negative speed -> rejected.
        _row("BAD_SPEED", event_type="trip_end", status="on_trip", fare=700.0,
             speed=-10.0, minutes=8),
    ]
    result = _compute(spark, rows, str(tmp_path))
    assert result["V001"]["trips"] == 1
    assert result["V001"]["revenue"] == pytest.approx(100.0)


def test_revenue_ignores_fare_on_non_trip_end_rows(spark, tmp_path):
    """`fare` is cumulative per trip; only the trip_end value is final."""
    rows = [
        _row("P1", event_type="ping", status="on_trip", fare=0.0, minutes=0),
        _row("P2", event_type="ping", status="on_trip", fare=0.0, minutes=2),
        _row("E1", event_type="trip_end", status="on_trip", fare=180.0, minutes=4),
    ]
    result = _compute(spark, rows, str(tmp_path))
    assert result["V001"]["revenue"] == pytest.approx(180.0)
    assert result["V001"]["trips"] == 1


# ---------------------------------------------------------------------------
# gps_km
# ---------------------------------------------------------------------------

def test_gps_km_matches_the_python_haversine(spark, tmp_path):
    """Spark SQL haversine must agree with common.geo.haversine_km.

    The formula is written twice -- once in Python for the simulator and tests,
    once in Spark SQL so it does not run a UDF per ping. This test is what makes
    that duplication safe.
    """
    points = [(12.95, 77.55), (12.96, 77.56), (12.97, 77.57)]
    rows = [
        _row(f"P{i}", lat=lat, lon=lon, minutes=i * 5, status="on_trip", speed=30.0)
        for i, (lat, lon) in enumerate(points)
    ]
    result = _compute(spark, rows, str(tmp_path))

    expected = sum(
        geo.haversine_km(points[i][0], points[i][1], points[i + 1][0], points[i + 1][1])
        for i in range(len(points) - 1)
    )
    assert result["V001"]["gps_km"] == pytest.approx(expected, rel=1e-4)


def test_impossible_gps_jumps_are_dropped(spark, tmp_path):
    """One glitched coordinate must not fabricate hundreds of kilometres.

    Without this filter a single bad ping inflates gps_km past a full day of
    driving and the vehicle is wrongly flagged for a distance mismatch that the
    partner did not cause.
    """
    rows = [
        _row("P0", lat=12.95, lon=77.55, minutes=0, status="on_trip", speed=30.0),
        # 20 km away two simulated minutes later => ~600 km/h. Implausible.
        _row("P1", lat=13.05, lon=77.68, minutes=2, status="on_trip", speed=30.0),
        _row("P2", lat=12.951, lon=77.551, minutes=4, status="on_trip", speed=30.0),
    ]
    result = _compute(spark, rows, str(tmp_path))
    # Both hops into and out of the glitch are implausible, so the surviving
    # distance is small -- certainly nowhere near the 40 km the glitch implies.
    assert result["V001"]["gps_km"] < 5.0


def test_a_parked_vehicle_accumulates_almost_no_distance(spark, tmp_path):
    """GPS jitter on an idle vehicle must not register as driving."""
    rows = [
        _row(f"P{i}", lat=12.98 + i * 1e-5, lon=77.60 + i * 1e-5, minutes=i * 2)
        for i in range(60)
    ]
    result = _compute(spark, rows, str(tmp_path))
    assert result["V001"]["gps_km"] < 0.2


# ---------------------------------------------------------------------------
# Time in status and utilization
# ---------------------------------------------------------------------------

def test_time_in_status_is_charged_until_the_next_event(spark, tmp_path):
    rows = [
        _row("A", status="on_trip", minutes=0, speed=30.0),
        _row("B", status="on_trip", minutes=10, speed=30.0),
        _row("C", status="idle", minutes=20),
        _row("D", status="idle", minutes=30),
    ]
    result = _compute(spark, rows, str(tmp_path))
    # A->B and B->C are on_trip (10 + 10), C->D is idle (10). D has no successor.
    assert result["V001"]["on_trip_min"] == pytest.approx(20.0)
    assert result["V001"]["idle_min"] == pytest.approx(10.0)
    assert result["V001"]["online_min"] == pytest.approx(30.0)
    assert result["V001"]["utilization"] == pytest.approx(20.0 / 30.0, abs=1e-3)


def test_an_off_shift_gap_is_not_charged_to_the_previous_status(spark, tmp_path):
    """A vehicle that goes quiet for hours must not be credited with that time.

    Charging the gap to its last status would credit a parked vehicle with an
    entire night of "on trip" time and push utilization to nonsense.
    """
    rows = [
        _row("A", status="on_trip", minutes=0, speed=30.0),
        # 8 simulated hours later -- the vehicle was off shift in between.
        _row("B", status="on_trip", minutes=480, speed=30.0),
        _row("C", status="idle", minutes=485),
    ]
    result = _compute(spark, rows, str(tmp_path))
    assert result["V001"]["on_trip_min"] == pytest.approx(5.0)
    assert result["V001"]["online_min"] < config.OFFLINE_SIM_MIN * 2


# ---------------------------------------------------------------------------
# Multiple vehicles
# ---------------------------------------------------------------------------

def test_vehicles_are_computed_independently(spark, tmp_path):
    """Window functions partition by vehicle; a leak would mix their distances."""
    rows = [
        _row("A1", vehicle="V001", lat=12.95, lon=77.55, minutes=0,
             status="on_trip", speed=30.0),
        _row("A2", vehicle="V001", lat=12.96, lon=77.55, minutes=5,
             status="on_trip", speed=30.0),
        _row("B1", vehicle="V002", lat=13.02, lon=77.65, minutes=0,
             status="on_trip", speed=30.0),
        _row("B2", vehicle="V002", lat=13.03, lon=77.65, minutes=5,
             status="on_trip", speed=30.0),
    ]
    result = _compute(spark, rows, str(tmp_path))
    assert set(result) == {"V001", "V002"}
    # Each vehicle moved 0.01 degrees of latitude, about 1.11 km.
    for vehicle_id in ("V001", "V002"):
        assert result[vehicle_id]["gps_km"] == pytest.approx(1.11, abs=0.05)


def test_an_empty_partition_yields_no_rows_rather_than_raising(spark, tmp_path):
    """A day with an expense file but no telemetry is a real case."""
    from batch import vehicle_day

    original = config.RAW_TELEMETRY_PATH
    config.RAW_TELEMETRY_PATH = f"{tmp_path}/does-not-exist"
    vehicle_day.config.RAW_TELEMETRY_PATH = config.RAW_TELEMETRY_PATH
    try:
        assert vehicle_day.compute(spark, DAY) == []
    finally:
        config.RAW_TELEMETRY_PATH = original
        vehicle_day.config.RAW_TELEMETRY_PATH = original


# ---------------------------------------------------------------------------
# Reconciliation over the joined data
# ---------------------------------------------------------------------------

def test_full_outer_join_keeps_both_one_sided_cases():
    """An inner join would silently drop exactly what the business needs to see."""
    from batch import reconcile as reconcile_module

    telemetry = [
        {"vehicle_id": "V001", "trips": 5, "revenue": 800.0, "gps_km": 90.0,
         "online_min": 400.0, "on_trip_min": 200.0, "idle_min": 150.0,
         "utilization": 0.5},
        # V002 reported telemetry but has no expense row -> missing_costs.
        {"vehicle_id": "V002", "trips": 3, "revenue": 400.0, "gps_km": 50.0,
         "online_min": 300.0, "on_trip_min": 120.0, "idle_min": 150.0,
         "utilization": 0.4},
    ]
    expenses = [
        {"vehicle_id": "V001", "fuel_cost": 250.0, "maintenance_cost": 90.0,
         "distance_covered": 92.0, "service_flag": False},
        # V003 was billed but never reported -> missing_telemetry.
        {"vehicle_id": "V003", "fuel_cost": 180.0, "maintenance_cost": 90.0,
         "distance_covered": 60.0, "service_flag": False},
    ]

    # `net_profit_history` hits the database; stub it out for a pure unit test.
    original = reconcile_module.net_profit_history
    reconcile_module.net_profit_history = lambda *a, **k: []
    try:
        rows = {r["vehicle_id"]: r for r in reconcile_module.reconcile(
            DAY, "run-1", 1, telemetry, expenses
        )}
    finally:
        reconcile_module.net_profit_history = original

    assert set(rows) == {"V001", "V002", "V003"}
    assert rows["V002"]["missing_costs"] and not rows["V002"]["missing_telemetry"]
    assert rows["V003"]["missing_telemetry"] and not rows["V003"]["missing_costs"]
    # V001: 800 revenue - 340 cost = 460 net.
    assert rows["V001"]["net_profit"] == pytest.approx(460.0)
    assert not rows["V001"]["unprofitable"]
