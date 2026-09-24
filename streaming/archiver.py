"""App `archiver`: Kafka -> raw Parquet master dataset (SPEC 7.2).

This application contains NO business logic, and that is its whole point.

In a Lambda architecture the master dataset is the source of truth: the batch
layer recomputes everything from it, so if it is wrong, every past day is wrong
and there is nothing to recover from. Keeping the archiver as a separate Spark
application with its own checkpoint means a bug in the speed layer's validation,
windowing or state handling cannot corrupt it -- the worst a speed-layer bug can
do is produce wrong dashboards, which the next batch run overwrites.

The parse is PERMISSIVE. Records that cannot be parsed are still written, with
their raw bytes intact, into `sim_date=unknown`. An archive that silently dropped
malformed records would make the "how much bad data did we receive?" question
unanswerable forever.
"""
from __future__ import annotations

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import config, schemas
from common.logging import get_logger
from streaming.listener import PrometheusQueryListener, start_metrics_server

log = get_logger("archiver", stage="processing")


def build_spark() -> SparkSession:
    """Session tuned for a tiny cluster."""
    return (
        SparkSession.builder.appName("fleet-archiver")
        # 4 shuffle partitions, not the default 200: this job has no shuffle at
        # all, but the default would still create 200 empty task slots per batch
        # and dominate the runtime on a 1-core executor.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        # Event timestamps are ISO-8601 with a Z suffix; CORRECTED mode rejects
        # the legacy SimpleDateFormat patterns and fails loudly instead of
        # returning nulls for a pattern Spark 3 no longer supports.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )


def main() -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    start_metrics_server(config.ARCHIVER_METRICS_PORT)
    spark.streams.addListener(PrometheusQueryListener(log))

    log.info(
        "archiver starting",
        extra={
            "event": "archiver_start",
            "topic": config.TELEMETRY_TOPIC,
            "output": config.RAW_TELEMETRY_PATH,
            "trigger_real_s": config.ARCHIVER_TRIGGER_REAL_S,
        },
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.TELEMETRY_TOPIC)
        # `earliest` so that a restart, or a first start after the simulator has
        # been running, archives everything Kafka still holds rather than silently
        # skipping it. Offsets after the first batch come from the checkpoint.
        .option("startingOffsets", "earliest")
        # Bounds the size of a catch-up batch. Without it, a restart after an
        # outage reads the entire retained topic in one micro-batch and the
        # executor dies on memory.
        .option("maxOffsetsPerTrigger", 20000)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(
            # Kafka coordinates: kept so any archived row can be traced back to
            # the exact broker offset it came from.
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("kafka_key"),
            # The raw bytes, always. This is the line that makes the archive
            # lossless: even an unparseable payload is recoverable from here.
            F.col("value").cast("string").alias("raw_value"),
            # Permissive parse: from_json returns NULL for the whole struct when
            # the payload is not valid JSON, rather than throwing.
            F.from_json(F.col("value").cast("string"), schemas.telemetry_struct())
            .alias("payload"),
        )
        .select(
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "kafka_key",
            "raw_value",
            "payload.*",
        )
        # to_timestamp returns NULL on an unparseable value instead of failing the
        # batch, which is what we want here: a bad timestamp is data to keep, not
        # a reason to stop archiving.
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
        .withColumn("ingested_at", F.current_timestamp())
    )

    archived = parsed.withColumn(
        # The lake's partition key. Derived from the EVENT timestamp, not from
        # ingestion time, so a late event lands in the day it actually belongs to
        # -- which is precisely why the batch layer can be complete where the
        # speed layer cannot.
        "sim_date",
        F.coalesce(F.date_format(F.col("event_time"), "yyyy-MM-dd"), F.lit("unknown")),
    )

    query = (
        archived.writeStream.format("parquet")
        .outputMode("append")
        .partitionBy("sim_date")
        .option("path", config.RAW_TELEMETRY_PATH)
        # Its OWN checkpoint directory, separate from every speed-layer query.
        # Sharing one would make the two applications fight over offsets.
        .option("checkpointLocation", f"{config.CHECKPOINT_ROOT}/archiver")
        .queryName("archiver")
        # ~30 real seconds per file. Faster would produce thousands of tiny
        # Parquet files per simulated day, which is the classic small-files
        # problem and would make the batch read slower than the batch compute.
        .trigger(processingTime=f"{config.ARCHIVER_TRIGGER_REAL_S} seconds")
        .start()
    )

    log.info("archiver query started", extra={"event": "query_started", "id": str(query.id)})
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
