"""`compute_vehicle_day`: the authoritative per-vehicle daily figures (SPEC 8.2 task 4).

Reads the raw Parquet archive for ONE simulated date and recomputes, from scratch:

    trips, revenue, gps_km, online_min, on_trip_min, idle_min, utilization

Three design points are worth defending in the viva.

1. IT RE-APPLIES `common.validation` AND RE-DEDUPLICATES. It does not trust the
   speed layer at all -- it does not even read the speed layer's output. The
   archiver writes whatever Kafka delivered, malformed payloads included, so the
   batch layer has to clean the data itself. Because it uses the SAME validation
   function, an event rejected here is rejected identically in the speed layer;
   only the watermark makes them differ.

2. IT HAS NO WATERMARK. This is the whole reason the batch layer exists. The
   speed layer drops events more than 10 simulated minutes late; this job reads
   the day's complete partition after the grace period, so it sees every event
   including the ~2% injected late. The difference is the `speed_batch_drift`
   metric.

3. IT IS RERUNNABLE. Nothing here depends on previous state, so recomputing a day
   after a corrected expense file gives exactly the same telemetry figures.

Runs in Spark LOCAL mode inside the Airflow worker. One simulated day is roughly
35k events, which a single JVM handles in seconds, and local mode avoids the
client-mode networking that would otherwise have to be debugged in Compose.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from common import config, earnings, simclock, validation
from common.logging import get_logger

log = get_logger("batch", stage="processing")


def build_spark(app_name: str = "fleet-batch"):
    """A small local-mode SparkSession for a single day's reconciliation."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        # The archiver writes one directory per sim_date; reading the whole lake
        # and filtering would work but would scan every past day on every run.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def read_day(spark, sim_date: str):
    """Load one simulated date's partition from the raw archive.

    Returns None when the partition does not exist, which is a real case: a day
    can have an expense file and no telemetry at all (the whole fleet was off, or
    ingestion was down). That becomes the `missing_telemetry` flag rather than a
    crash.
    """
    from pyspark.sql.utils import AnalysisException

    path = f"{config.RAW_TELEMETRY_PATH}/sim_date={sim_date}"
    try:
        return spark.read.parquet(path)
    except AnalysisException:
        log.warning(
            "no telemetry partition for this date",
            extra={"event": "missing_partition", "sim_date": sim_date, "path": path},
        )
        return None


def clean_events(raw, sim_date: str):
    """Validate and deduplicate the day's events.

    Deduplication is on `event_id` with NO watermark, i.e. across the entire day.
    That is affordable here (one day's events fit in memory) and it is strictly
    more correct than the speed layer's watermark-scoped dedup: a duplicate that
    arrived 15 simulated minutes late is caught here and missed there.
    """
    from pyspark.sql import functions as F

    events = raw.withColumn("error_reason", validation.spark_validation_expr())

    valid = (
        events.filter(F.col("error_reason").isNull())
        # Guard against an event whose timestamp puts it in a different day than
        # the partition it was written to. The partition is derived from the
        # event time, so this should never fire -- but a day's revenue is a
        # financial figure and "should never" is not a control.
        .filter(F.date_format(F.col("event_time"), "yyyy-MM-dd") == F.lit(sim_date))
        .dropDuplicates(["event_id"])
    )
    return events, valid


def compute(spark, sim_date: str) -> List[Dict]:
    """Per-vehicle figures for one simulated date."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    raw = read_day(spark, sim_date)
    if raw is None:
        return []

    all_events, valid = clean_events(raw, sim_date)
    total_rows = raw.count()
    valid_rows = valid.count()

    log.info(
        "telemetry loaded for reconciliation",
        extra={
            "event": "telemetry_loaded",
            "sim_date": sim_date,
            "archived_rows": total_rows,
            "valid_deduplicated_rows": valid_rows,
            "rejected_or_duplicate": total_rows - valid_rows,
        },
    )
    if valid_rows == 0:
        return []

    # --- trips and revenue ------------------------------------------------
    # Revenue is the sum of `fare` over trip_end rows ONLY. `fare` is cumulative
    # within a trip, so summing every row would multiply revenue by roughly the
    # number of pings per trip.
    totals = valid.groupBy("vehicle_id").agg(
        F.sum(F.when(F.col("event_type") == "trip_end", 1).otherwise(0)).alias("trips"),
        F.sum(F.when(F.col("event_type") == "trip_end", F.col("fare")).otherwise(0.0))
        .alias("revenue"),
        F.min("event_time").alias("first_seen"),
        F.max("event_time").alias("last_seen"),
        F.count(F.lit(1)).alias("events"),
    )

    # --- gps_km -----------------------------------------------------------
    # Haversine between CONSECUTIVE pings, ordered by event time per vehicle.
    by_vehicle = Window.partitionBy("vehicle_id").orderBy("event_time")

    hops = (
        valid.select("vehicle_id", "event_time", "lat", "lon")
        .withColumn("prev_lat", F.lag("lat").over(by_vehicle))
        .withColumn("prev_lon", F.lag("lon").over(by_vehicle))
        .withColumn("prev_time", F.lag("event_time").over(by_vehicle))
        .filter(F.col("prev_lat").isNotNull())
    )

    # Haversine in Spark SQL rather than a Python UDF: this runs over every ping
    # in the day. The formula is the same as common.geo.haversine_km, and
    # tests/test_spark_batch.py asserts the two agree on real coordinates.
    radius = F.lit(6371.0088)
    d_phi = F.radians(F.col("lat") - F.col("prev_lat"))
    d_lambda = F.radians(F.col("lon") - F.col("prev_lon"))
    a = (
        F.pow(F.sin(d_phi / 2), 2)
        + F.cos(F.radians(F.col("prev_lat")))
        * F.cos(F.radians(F.col("lat")))
        * F.pow(F.sin(d_lambda / 2), 2)
    )

    hops = (
        hops.withColumn("hop_km", 2 * radius * F.asin(F.sqrt(a)))
        .withColumn(
            "gap_hours",
            (F.col("event_time").cast("double") - F.col("prev_time").cast("double")) / 3600.0,
        )
        # Implied speed for this hop. A GPS glitch produces a hop of tens of km in
        # two simulated minutes, which is hundreds of km/h.
        .withColumn(
            "implied_kmh",
            F.when(F.col("gap_hours") > 0, F.col("hop_km") / F.col("gap_hours"))
            .otherwise(F.lit(0.0)),
        )
        # Drop physically impossible jumps (SPEC 8.2 task 4). Without this filter
        # one bad coordinate inflates gps_km by more than a day of real driving
        # and the vehicle is wrongly flagged for a distance mismatch.
        .filter(F.col("implied_kmh") <= F.lit(config.MAX_PLAUSIBLE_SPEED_KMH))
    )

    distance = hops.groupBy("vehicle_id").agg(
        F.sum("hop_km").alias("gps_km"),
        F.count(F.lit(1)).alias("hops"),
    )

    # --- time in each status ---------------------------------------------
    # Each event is charged the interval UNTIL the next event from the same
    # vehicle, which is the standard way to turn a point-in-time status stream
    # into durations. The final event of the day has no successor and is charged
    # nothing, which under-counts by at most one emission interval (~2 simulated
    # minutes) per vehicle per day.
    durations = (
        valid.select("vehicle_id", "event_time", "status")
        .withColumn("next_time", F.lead("event_time").over(by_vehicle))
        .withColumn(
            "minutes",
            (F.col("next_time").cast("double") - F.col("event_time").cast("double")) / 60.0,
        )
        .filter(F.col("minutes").isNotNull())
        # A gap longer than OFFLINE_SIM_MIN means the vehicle went off shift or
        # went silent; charging that whole gap to its last status would credit a
        # parked vehicle with hours of "on trip" time.
        .filter(F.col("minutes") <= F.lit(config.OFFLINE_SIM_MIN))
    )

    time_by_status = durations.groupBy("vehicle_id").agg(
        F.sum("minutes").alias("online_min"),
        F.sum(F.when(F.col("status") == "on_trip", F.col("minutes")).otherwise(0.0))
        .alias("on_trip_min"),
        F.sum(F.when(F.col("status") == "idle", F.col("minutes")).otherwise(0.0))
        .alias("idle_min"),
    )

    combined = (
        totals.join(distance, on="vehicle_id", how="left")
        .join(time_by_status, on="vehicle_id", how="left")
        .fillna(0.0, subset=["gps_km", "online_min", "on_trip_min", "idle_min"])
    )

    results: List[Dict] = []
    for row in combined.collect():
        online = float(row["online_min"] or 0.0)
        on_trip = float(row["on_trip_min"] or 0.0)
        results.append(
            {
                "vehicle_id": row["vehicle_id"],
                "sim_date": sim_date,
                "trips": int(row["trips"] or 0),
                "revenue": round(float(row["revenue"] or 0.0), 2),
                "gps_km": round(float(row["gps_km"] or 0.0), 3),
                "online_min": round(online, 2),
                "on_trip_min": round(on_trip, 2),
                "idle_min": round(float(row["idle_min"] or 0.0), 2),
                # Shared with the speed layer and the API: one definition.
                "utilization": earnings.utilization(on_trip, online),
                "events": int(row["events"] or 0),
            }
        )

    log.info(
        "vehicle-day figures computed",
        extra={
            "event": "vehicle_day_computed",
            "sim_date": sim_date,
            "vehicles": len(results),
            "fleet_revenue": round(sum(r["revenue"] for r in results), 2),
            "fleet_trips": sum(r["trips"] for r in results),
            "fleet_gps_km": round(sum(r["gps_km"] for r in results), 1),
        },
    )
    return results


def run(sim_date: str) -> List[Dict]:
    """Entry point used by the DAG. Owns the SparkSession lifecycle."""
    spark = build_spark(f"fleet-batch-{sim_date}")
    try:
        return compute(spark, sim_date)
    finally:
        # Always stopped: an Airflow task leaving a SparkContext alive means the
        # next task in the same worker cannot create one.
        spark.stop()
