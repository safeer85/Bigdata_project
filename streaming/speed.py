"""App `speed`: the speed layer (SPEC 7.3).

Four independent streaming queries, each with its own checkpoint, all reading the
same Kafka topic:

    dlq                   invalid events -> fleet.telemetry.dlq
    vehicle_state         per-vehicle state machine + idle alerts
    zone_hourly           1-simulated-hour tumbling window per zone
    vehicle_daily_running 1-simulated-day window per vehicle

Separate queries rather than one, for two reasons. First, Structured Streaming
allows only one stateful operator chain per query, and we need a
`applyInPandasWithState` AND two different aggregations. Second, separate
checkpoints mean a failure in one query does not force the others to replay.

WHAT THIS LAYER IS FOR, and what it is not for. Everything here is approximate by
design: it has a 10-simulated-minute watermark, so events later than that are
dropped. That is the correct trade for a live utilization dashboard, where
"roughly right, two seconds old" beats "exactly right, a day late". Money is a
different question, and it is answered by the batch layer.
"""
from __future__ import annotations

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout

from common import config, schemas, simclock, validation
from common.logging import get_logger
from streaming import sinks, state
from streaming.listener import PrometheusQueryListener, start_metrics_server

log = get_logger("speed", stage="processing")

WATERMARK = simclock.spark_interval(config.WATERMARK_SIM_MIN)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("fleet-speed")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # RocksDB rather than the default in-memory state store. The default keeps
        # every vehicle's state on the JVM heap and snapshots it to the checkpoint;
        # with a 900m executor and a state machine that must survive restarts,
        # RocksDB is both smaller and faster to recover.
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )


def read_events(spark: SparkSession):
    """Kafka -> parsed, typed rows, with a validation verdict attached."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.TELEMETRY_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 20000)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        F.col("value").cast("string").alias("raw_value"),
        F.from_json(F.col("value").cast("string"), schemas.telemetry_struct()).alias("p"),
    )

    return (
        parsed.select("raw_value", "p.*")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
        # The single source of truth for "is this event usable", shared with the
        # batch layer through common/validation.py. Returns a reason code or null.
        .withColumn("error_reason", validation.spark_validation_expr())
        # from_json gives an all-null struct for unparseable bytes; that is a
        # distinct failure from a parseable-but-invalid payload, and the DLQ
        # records which it was.
        .withColumn(
            "error_reason",
            F.when(F.col("event_id").isNull() & F.col("raw_value").isNotNull(),
                   F.lit(validation.UNPARSEABLE))
            .otherwise(F.col("error_reason")),
        )
    )


def prepare_valid(events):
    """Watermark, deduplicate and enrich the valid events (SPEC 7.3).

    The ORDER of these three operations matters and is worth explaining:

    1. `withWatermark` first, because `dropDuplicatesWithinWatermark` needs one.
    2. Dedup second, so the zone lookup is not computed twice for a duplicate.
    3. Zone enrichment last.

    On the watermark: 10 simulated minutes. Chosen to be deliberately SMALLER
    than the maximum injected lateness of 20 minutes. A watermark of 25 would
    catch everything and make the speed layer agree perfectly with the batch
    layer -- and the project would then have no evidence for why the batch layer
    is needed. 10 is also a defensible operational choice: it bounds state size
    and keeps the dashboard within seconds of live.

    On `dropDuplicatesWithinWatermark` rather than plain `dropDuplicates`: the
    plain version must remember every event_id it has ever seen, so its state
    grows without bound and eventually kills the executor. The watermark-scoped
    version only remembers ids within the watermark window, which is exactly as
    long as a duplicate could plausibly arrive.
    """
    valid = events.filter(F.col("error_reason").isNull())

    return (
        valid.withWatermark("event_time", WATERMARK)
        .dropDuplicatesWithinWatermark(["event_id"])
        # Zone lookup expressed in Spark SQL rather than as a Python UDF: this
        # runs on every event, and a UDF here would serialise every row to Python.
        # The arithmetic is the same as common/geo.zone_of, and
        # tests/test_geo.py::test_spark_zone_matches_python asserts they agree.
        .withColumn("zone_row",
                    F.least(
                        F.floor(
                            (F.lit(config.CITY_LAT_MAX) - F.col("lat"))
                            / F.lit((config.CITY_LAT_MAX - config.CITY_LAT_MIN) / config.ZONE_ROWS)
                        ),
                        F.lit(config.ZONE_ROWS - 1),
                    ))
        .withColumn("zone_col",
                    F.least(
                        F.floor(
                            (F.col("lon") - F.lit(config.CITY_LON_MIN))
                            / F.lit((config.CITY_LON_MAX - config.CITY_LON_MIN) / config.ZONE_COLS)
                        ),
                        F.lit(config.ZONE_COLS - 1),
                    ))
        .withColumn(
            "zone_id",
            F.concat(
                F.lit("Z"),
                F.lpad(
                    (F.col("zone_row") * F.lit(config.ZONE_COLS) + F.col("zone_col") + 1)
                    .cast("int").cast("string"),
                    2, "0",
                ),
            ),
        )
        .drop("zone_row", "zone_col")
    )


# ---------------------------------------------------------------------------
# Query 1: dead-letter queue
# ---------------------------------------------------------------------------

def start_dlq(events):
    """Route invalid events to `fleet.telemetry.dlq` with a reason code.

    The ORIGINAL payload is preserved verbatim alongside the reason, so a bad
    event can be replayed once the producer is fixed. A DLQ that stored only the
    reason would be a log, not a queue.
    """
    invalid = events.filter(F.col("error_reason").isNotNull())

    dlq_payload = invalid.select(
        F.col("vehicle_id").alias("key"),
        F.to_json(
            F.struct(
                F.col("error_reason"),
                F.col("raw_value").alias("original_payload"),
                F.current_timestamp().alias("rejected_at"),
                F.col("vehicle_id"),
                F.col("event_id"),
            )
        ).alias("value"),
        F.col("error_reason"),
    )

    # Two sinks for one stream: Kafka for the payload, and a foreachBatch purely
    # to increment the Prometheus counter. A Kafka sink cannot touch the driver's
    # metrics registry, so the counting has to happen in its own query.
    kafka_query = (
        dlq_payload.select("key", "value")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("topic", config.DLQ_TOPIC)
        .option("checkpointLocation", f"{config.CHECKPOINT_ROOT}/dlq")
        .queryName("dlq")
        .outputMode("append")
        .trigger(processingTime=f"{config.SPEED_TRIGGER_REAL_S} seconds")
        .start()
    )

    metrics_query = (
        dlq_payload.select("error_reason")
        .writeStream.foreachBatch(sinks.count_dlq)
        .option("checkpointLocation", f"{config.CHECKPOINT_ROOT}/dlq_metrics")
        .queryName("dlq_metrics")
        .outputMode("append")
        .trigger(processingTime=f"{config.SPEED_TRIGGER_REAL_S} seconds")
        .start()
    )

    return [kafka_query, metrics_query]


# ---------------------------------------------------------------------------
# Query 2: per-vehicle state and idle alerts
# ---------------------------------------------------------------------------

def start_vehicle_state(valid):
    """Stateful per-vehicle processing (see streaming/state.py)."""
    stateful = (
        valid.select(
            "vehicle_id", "driver_id", "status", "zone_id",
            "lat", "lon", "speed", "trip_id", "event_time",
        )
        # Grouped by vehicle_id, which matches the Kafka partition key, so each
        # group's events arrive in order and no cross-partition shuffle is needed
        # to establish that order.
        .groupBy("vehicle_id")
        .applyInPandasWithState(
            func=state.update_vehicle_state,
            outputStructType=state.OUTPUT_SCHEMA,
            stateStructType=state.STATE_SCHEMA,
            outputMode="update",
            timeoutConf=GroupStateTimeout.EventTimeTimeout,
        )
    )

    def write_batch(batch_df, batch_id: int) -> None:
        """One micro-batch -> two tables.

        Cached because the DataFrame is consumed twice (state and alerts) and a
        streaming micro-batch DataFrame would otherwise be recomputed from the
        state store for the second action.
        """
        batch_df.persist()
        try:
            sinks.write_vehicle_state(batch_df, batch_id)

            alerts = batch_df.filter(F.col("alert_event") != F.lit(state.ALERT_NONE))
            if alerts.take(1):
                alert_rows = alerts.select(
                    F.col("vehicle_id"),
                    F.col("alert_opened_at").alias("opened_at"),
                    F.when(F.col("alert_event") == F.lit(state.ALERT_CLOSED),
                           F.col("last_event_time"))
                    .otherwise(F.lit(None).cast("timestamp")).alias("closed_at"),
                    F.col("zone_id"),
                    F.col("idle_sim_minutes"),
                    F.when(F.col("alert_event") == F.lit(state.ALERT_CLOSED),
                           F.lit("closed")).otherwise(F.lit("open")).alias("status"),
                ).filter(F.col("opened_at").isNotNull())
                sinks.write_idle_alerts(alert_rows, batch_id)
        finally:
            batch_df.unpersist()

    return [
        stateful.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", f"{config.CHECKPOINT_ROOT}/vehicle_state")
        .queryName("vehicle_state")
        .outputMode("update")
        .trigger(processingTime=f"{config.SPEED_TRIGGER_REAL_S} seconds")
        .start()
    ]


# ---------------------------------------------------------------------------
# Query 3: hourly zone aggregates
# ---------------------------------------------------------------------------

def start_zone_hourly(valid):
    """Tumbling 1-simulated-hour window per zone (SPEC 7.3).

    TUMBLING, not sliding: the business question is "earnings by zone and hour of
    day", which is a partition of time, not a smoothed rolling figure. Overlapping
    windows would double-count a trip into two buckets.

    Note what is NOT computed here: a distinct count of vehicles. Exact
    `countDistinct` is unsupported in a streaming aggregation (it would need
    unbounded state), so "active vehicles" is derived in SQL from
    `rt_vehicle_state` instead -- see v_zone_live. The ping counts below are an
    honest utilization PROXY: the share of pings in a zone that were on_trip.
    """
    hourly = (
        valid.groupBy(
            F.window(F.col("event_time"), simclock.spark_interval(60)),
            F.col("zone_id"),
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "trip_start", 1).otherwise(0))
            .alias("trips_started"),
            F.sum(F.when(F.col("event_type") == "trip_end", 1).otherwise(0))
            .alias("trips_completed"),
            # Revenue is the sum of `fare` over trip_end rows ONLY. `fare` is
            # cumulative-per-trip, so summing it over all rows would count a
            # trip's fare once per ping.
            F.sum(F.when(F.col("event_type") == "trip_end", F.col("fare")).otherwise(0.0))
            .alias("earnings"),
            F.sum(F.when(F.col("status") == "idle", 1).otherwise(0)).alias("pings_idle"),
            F.sum(F.when(F.col("status") == "enroute", 1).otherwise(0)).alias("pings_enroute"),
            F.sum(F.when(F.col("status") == "on_trip", 1).otherwise(0)).alias("pings_on_trip"),
            F.count(F.lit(1)).alias("pings_total"),
        )
        .select(
            F.col("zone_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.to_date(F.col("window.start")).alias("sim_date"),
            F.hour(F.col("window.start")).cast("short").alias("sim_hour"),
            F.col("trips_started").cast("int"),
            F.col("trips_completed").cast("int"),
            F.round(F.col("earnings"), 2).alias("earnings"),
            F.col("pings_idle").cast("int"),
            F.col("pings_enroute").cast("int"),
            F.col("pings_on_trip").cast("int"),
            F.col("pings_total").cast("int"),
        )
    )

    # UPDATE mode, not APPEND: append would emit a window only once the watermark
    # has passed its end, so the CURRENT hour -- the one the live dashboard is
    # about -- would never be shown. Update re-emits the growing window every
    # batch, which is safe precisely because the sink upserts on the window key.
    return [
        hourly.writeStream.foreachBatch(sinks.write_zone_hourly)
        .option("checkpointLocation", f"{config.CHECKPOINT_ROOT}/zone_hourly")
        .queryName("zone_hourly")
        .outputMode("update")
        .trigger(processingTime=f"{config.SPEED_TRIGGER_REAL_S} seconds")
        .start()
    ]


# ---------------------------------------------------------------------------
# Query 4: running daily earnings per vehicle
# ---------------------------------------------------------------------------

def start_vehicle_daily(valid):
    """1-simulated-day window per vehicle: running revenue and trips.

    Feeds two consumers:
      * the API, for the "today so far" block of the Lambda merge, and
      * `compute_drift`, which compares this against the batch figure for the
        same day. The difference is the watermark's effect, made measurable.
    """
    daily = (
        valid.groupBy(
            F.window(F.col("event_time"), simclock.spark_interval(24 * 60)),
            F.col("vehicle_id"),
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "trip_end", 1).otherwise(0)).alias("trips"),
            F.sum(F.when(F.col("event_type") == "trip_end", F.col("fare")).otherwise(0.0))
            .alias("revenue"),
        )
        .select(
            F.col("vehicle_id"),
            # The window is exactly one simulated calendar day (windows are
            # aligned to the epoch, and 24h windows therefore start at midnight),
            # so its start date IS the sim_date.
            F.to_date(F.col("window.start")).alias("sim_date"),
            F.col("trips").cast("int"),
            F.round(F.col("revenue"), 2).alias("revenue"),
        )
    )

    return [
        daily.writeStream.foreachBatch(sinks.write_vehicle_daily)
        .option("checkpointLocation", f"{config.CHECKPOINT_ROOT}/vehicle_daily")
        .queryName("vehicle_daily_running")
        .outputMode("update")
        .trigger(processingTime=f"{config.SPEED_TRIGGER_REAL_S} seconds")
        .start()
    ]


def main() -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    start_metrics_server(config.SPEED_METRICS_PORT)
    spark.streams.addListener(PrometheusQueryListener(log))

    # Fail fast and loudly if the database is not there: every sink writes to it,
    # and a speed layer that runs for ten minutes before its first write fails is
    # much harder to diagnose than one that refuses to start.
    from common import db

    db.wait_for_db()

    log.info(
        "speed layer starting",
        extra={
            "event": "speed_start",
            "watermark_sim_min": config.WATERMARK_SIM_MIN,
            "idle_alert_sim_min": config.IDLE_ALERT_SIM_MIN,
            "offline_sim_min": config.OFFLINE_SIM_MIN,
            "trigger_real_s": config.SPEED_TRIGGER_REAL_S,
        },
    )

    events = read_events(spark)
    valid = prepare_valid(events)

    queries = []
    queries += start_dlq(events)
    queries += start_vehicle_state(valid)
    queries += start_zone_hourly(valid)
    queries += start_vehicle_daily(valid)

    log.info(
        "all speed-layer queries started",
        extra={"event": "queries_started",
               "queries": [q.name for q in queries]},
    )

    # Block on ANY query terminating, so that one failed query takes the whole
    # application down and Docker restarts it, rather than leaving a half-dead
    # speed layer silently serving stale tables.
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
