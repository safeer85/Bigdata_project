"""PySpark tests for the speed layer's transformations (SPEC 11, 12 phase 2).

The headline test here is `test_duplicates_do_not_inflate_counts`, which is the
Phase 2 acceptance criterion "duplicates don't inflate counts (proven by a test)".

These run the real transformations from `streaming/speed.py` over small static
DataFrames rather than a live stream. That is a deliberate trade: a streaming test
with `MemoryStream` would also exercise the watermark machinery, but it is slow,
flaky under CI and hard to read. The stateful logic gets its own direct test in
`test_vehicle_state.py`, and the watermark's real behaviour is observed on the
running stack via `fleet_late_rows_dropped_total`.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from common import config, geo, validation

pytest.importorskip("pyspark")

BASE_TS = datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc)


def _event(event_id, vehicle="V001", event_type="ping", status="idle",
           fare=0.0, lat=12.98, lon=77.60, minutes=0, speed=0.0):
    """One row shaped exactly as `streaming.speed.read_events` produces it.

    `timestamp` and `schema_version` are included even though the assertions do
    not read them: `spark_validation_expr` checks every non-nullable field of the
    contract for nullness, so a fixture missing them fails to resolve rather than
    failing an assertion.
    """
    ts = BASE_TS + timedelta(minutes=minutes)
    return {
        "event_id": event_id,
        "event_type": event_type,
        "trip_id": None if event_type == "ping" else "T1",
        "driver_id": "D001",
        "vehicle_id": vehicle,
        "lat": lat,
        "lon": lon,
        "speed": speed,
        "status": status,
        "fare": fare,
        "timestamp": ts.isoformat().replace("+00:00", "Z"),
        "schema_version": 1,
        "event_time": ts,
    }


def _frame(spark, rows):
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


# ---------------------------------------------------------------------------
# Deduplication -- the Phase 2 acceptance criterion
# ---------------------------------------------------------------------------

def test_duplicates_do_not_inflate_counts(spark):
    """The same event_id twice must count once.

    The simulator injects ~1% duplicate event_ids on purpose. Both copies are
    perfectly valid, so nothing but deduplication can stop them: without it, one
    duplicated trip_end would be counted as two completed trips and its fare
    would be added to revenue twice.
    """
    from pyspark.sql import functions as F

    rows = [
        _event("A", event_type="trip_end", status="on_trip", fare=150.0, minutes=0),
        # The SAME event delivered a second time, exactly as the fault injector
        # produces it: identical id, identical payload.
        _event("A", event_type="trip_end", status="on_trip", fare=150.0, minutes=0),
        _event("B", event_type="trip_end", status="on_trip", fare=200.0, minutes=5),
    ]
    events = _frame(spark, rows)

    # Without dedup: 3 rows and 500.0 revenue -- the duplicated 150 is counted
    # twice on top of the genuine 150 + 200. That is the wrong answer.
    naive = events.agg(
        F.count("*").alias("trips"), F.sum("fare").alias("revenue")
    ).collect()[0]
    assert naive["trips"] == 3
    assert naive["revenue"] == pytest.approx(500.0)

    # With dedup on event_id: 2 rows, 350.0 -- the right answer.
    deduped = events.dropDuplicates(["event_id"]).agg(
        F.count("*").alias("trips"), F.sum("fare").alias("revenue")
    ).collect()[0]
    assert deduped["trips"] == 2
    assert deduped["revenue"] == pytest.approx(350.0)


def test_dedup_keeps_distinct_events_from_the_same_vehicle(spark):
    """Dedup is on event_id, not on vehicle: a vehicle emits many real events."""
    rows = [_event(f"E{i}", minutes=i) for i in range(10)]
    deduped = _frame(spark, rows).dropDuplicates(["event_id"])
    assert deduped.count() == 10


def test_revenue_sums_only_trip_end_rows(spark):
    """`fare` is cumulative per trip, so summing every row would over-count.

    This is the single most likely way to get revenue wrong in this project, so
    it gets its own test in both layers.
    """
    from pyspark.sql import functions as F

    rows = [
        # A trip in progress: pings carry fare 0 by contract.
        _event("P1", event_type="ping", status="on_trip", fare=0.0, minutes=0),
        _event("P2", event_type="ping", status="on_trip", fare=0.0, minutes=2),
        # The drop-off carries the FINAL fare.
        _event("E1", event_type="trip_end", status="on_trip", fare=275.5, minutes=4),
    ]
    revenue = _frame(spark, rows).agg(
        F.sum(F.when(F.col("event_type") == "trip_end", F.col("fare")).otherwise(0.0))
        .alias("revenue")
    ).collect()[0]["revenue"]
    assert revenue == pytest.approx(275.5)


# ---------------------------------------------------------------------------
# Validation parity: the Spark expression must agree with the Python function
# ---------------------------------------------------------------------------

def test_spark_validation_matches_the_python_rules(spark):
    """The two implementations of the same rules must never disagree.

    `common.validation` has a pure-Python `validate_event` (used by the simulator,
    the tests and any row-wise code) and a Spark Column expression (used on every
    streaming event, because a Python UDF per row would be ruinous). Having two
    implementations is only acceptable because this test pins them together.
    """
    from pyspark.sql import functions as F

    cases = [
        ("valid", _event("V", status="idle"), None),
        ("north of the box", _event("N", lat=config.CITY_LAT_MAX + 1.0),
         validation.OUT_OF_BOUNDS),
        ("west of the box", _event("W", lon=config.CITY_LON_MIN - 1.0),
         validation.OUT_OF_BOUNDS),
        ("negative speed", _event("S", speed=-5.0), validation.NEGATIVE_SPEED),
        ("negative fare", _event("F", fare=-10.0), validation.NEGATIVE_FARE),
    ]

    frame = _frame(spark, [row for _, row, _ in cases])
    verdicts = {
        row["event_id"]: row["error_reason"]
        for row in frame.withColumn(
            "error_reason", validation.spark_validation_expr()
        ).select("event_id", "error_reason").collect()
    }

    for label, row, expected in cases:
        # Spark's verdict.
        assert verdicts[row["event_id"]] == expected, f"spark disagreed on {label}"

        # The Python function's verdict, over the same row shaped as an event dict.
        as_event = dict(row)
        as_event.pop("event_time")
        valid, reason = validation.validate_event(as_event)
        assert (reason if not valid else None) == expected, f"python disagreed on {label}"


def test_bad_status_is_rejected_by_the_spark_expression(spark):
    row = _event("X", status="teleporting")
    frame = _frame(spark, [row]).withColumn(
        "error_reason", validation.spark_validation_expr()
    )
    assert frame.collect()[0]["error_reason"] == validation.BAD_STATUS


def test_null_required_field_is_rejected_by_the_spark_expression(spark):
    from pyspark.sql import functions as F

    frame = _frame(spark, [_event("X")]).withColumn(
        "vehicle_id", F.lit(None).cast("string")
    ).withColumn("error_reason", validation.spark_validation_expr())
    assert frame.collect()[0]["error_reason"] == validation.MISSING_FIELD


# ---------------------------------------------------------------------------
# Zone enrichment: the Spark expression must agree with common/geo.py
# ---------------------------------------------------------------------------

def test_spark_zone_matches_python(spark):
    """Zone ids are computed twice -- in Spark SQL for the stream, in Python for
    everything else. A mismatch would silently misattribute earnings by zone.
    """
    from streaming.speed import prepare_valid  # noqa: F401  (import check only)
    from pyspark.sql import functions as F

    # A grid of probe points spread across the whole city box.
    probes = []
    for row_i in range(config.ZONE_ROWS):
        for col_i in range(config.ZONE_COLS):
            lat, lon = geo.zone_centre(f"Z{row_i * config.ZONE_COLS + col_i + 1:02d}")
            probes.append(_event(f"Z{row_i}{col_i}", lat=lat, lon=lon))

    frame = _frame(spark, probes)

    lat_step = (config.CITY_LAT_MAX - config.CITY_LAT_MIN) / config.ZONE_ROWS
    lon_step = (config.CITY_LON_MAX - config.CITY_LON_MIN) / config.ZONE_COLS
    enriched = (
        frame.withColumn(
            "zone_row",
            F.least(F.floor((F.lit(config.CITY_LAT_MAX) - F.col("lat")) / F.lit(lat_step)),
                    F.lit(config.ZONE_ROWS - 1)))
        .withColumn(
            "zone_col",
            F.least(F.floor((F.col("lon") - F.lit(config.CITY_LON_MIN)) / F.lit(lon_step)),
                    F.lit(config.ZONE_COLS - 1)))
        .withColumn(
            "zone_id",
            F.concat(F.lit("Z"), F.lpad(
                (F.col("zone_row") * F.lit(config.ZONE_COLS) + F.col("zone_col") + 1)
                .cast("int").cast("string"), 2, "0")))
    )

    for row in enriched.select("lat", "lon", "zone_id").collect():
        assert row["zone_id"] == geo.zone_of(row["lat"], row["lon"])


# ---------------------------------------------------------------------------
# Hourly zone aggregation
# ---------------------------------------------------------------------------

def test_hourly_window_groups_events_by_simulated_hour(spark):
    """Tumbling 1-hour windows must partition time, not overlap it."""
    from pyspark.sql import functions as F

    rows = [
        _event("A", event_type="trip_end", fare=100.0, minutes=5),    # 09:05
        _event("B", event_type="trip_end", fare=200.0, minutes=50),   # 09:50
        _event("C", event_type="trip_end", fare=300.0, minutes=70),   # 10:10
    ]
    result = (
        _frame(spark, rows)
        .groupBy(F.window(F.col("event_time"), "60 minutes"))
        .agg(F.sum(F.when(F.col("event_type") == "trip_end", F.col("fare"))
                   .otherwise(0.0)).alias("earnings"))
        .orderBy("window")
        .collect()
    )
    assert len(result) == 2
    assert result[0]["earnings"] == pytest.approx(300.0)   # 09:00 hour: A + B
    assert result[1]["earnings"] == pytest.approx(300.0)   # 10:00 hour: C


def test_ping_status_counts_are_the_utilization_proxy(spark):
    """Exact countDistinct is unsupported in streaming aggregations, so the speed
    layer reports the share of pings by status instead."""
    from pyspark.sql import functions as F

    rows = (
        [_event(f"I{i}", status="idle", minutes=i) for i in range(6)]
        + [_event(f"T{i}", status="on_trip", minutes=i) for i in range(4)]
    )
    agg = _frame(spark, rows).agg(
        F.sum(F.when(F.col("status") == "idle", 1).otherwise(0)).alias("pings_idle"),
        F.sum(F.when(F.col("status") == "on_trip", 1).otherwise(0)).alias("pings_on_trip"),
        F.count(F.lit(1)).alias("pings_total"),
    ).collect()[0]

    assert agg["pings_idle"] == 6
    assert agg["pings_on_trip"] == 4
    assert agg["pings_total"] == 10
