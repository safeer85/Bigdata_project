"""StreamingQueryListener that exports Spark progress to Prometheus (SPEC 10.2).

Spark's own metrics system cannot answer the questions a streaming pipeline is
actually judged on: is the query making progress, how far behind Kafka is it, and
how many rows is the watermark throwing away. Those live in the `QueryProgress`
object, which is only reachable from a listener.

The Kafka lag figure deserves a note. Structured Streaming does NOT commit
consumer-group offsets -- it tracks them in its own checkpoint -- so
`kafka-consumer-groups.sh` and every off-the-shelf lag exporter report nothing for
a Spark consumer. The only place the lag exists is in the progress event's source
metrics, which is where this listener reads it from.
"""
from __future__ import annotations

import json
import threading
from typing import Dict, Optional

from pyspark.sql.streaming import StreamingQueryListener

from common import metrics

_metrics_started = threading.Event()


def start_metrics_server(port: int) -> None:
    """Start the Prometheus endpoint on the DRIVER, once per process.

    On the driver, not the executors: query progress is a driver-side concept, and
    a per-executor endpoint would report nothing useful and be unscrapeable
    anyway (executors get ephemeral container ports).
    """
    if not _metrics_started.is_set():
        metrics.serve(port)
        _metrics_started.set()


def _as_float(value) -> Optional[float]:
    """Coerce a progress field that may be a string, a number, or missing."""
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    # Spark reports NaN for a rate it cannot compute yet (the very first batch).
    return None if result != result else result


class PrometheusQueryListener(StreamingQueryListener):
    """Turns each micro-batch's progress into Prometheus samples."""

    def __init__(self, log) -> None:
        self.log = log
        # Counters need deltas, but progress reports cumulative-per-batch values,
        # so the previous total per query is remembered here.
        self._last_dropped: Dict[str, float] = {}

    def onQueryStarted(self, event) -> None:
        self.log.info(
            "streaming query started",
            extra={"event": "query_started", "query": event.name, "id": str(event.id)},
        )

    def onQueryTerminated(self, event) -> None:
        self.log.warning(
            "streaming query terminated",
            extra={
                "event": "query_terminated",
                "id": str(event.id),
                "exception": str(event.exception) if event.exception else None,
            },
        )

    def onQueryProgress(self, event) -> None:
        progress = event.progress
        name = progress.name or "unnamed"

        input_rps = _as_float(progress.inputRowsPerSecond)
        processed_rps = _as_float(progress.processedRowsPerSecond)
        duration_ms = _as_float(progress.batchDuration)

        if input_rps is not None:
            metrics.STREAM_INPUT_RPS.labels(query=name).set(input_rps)
        if processed_rps is not None:
            metrics.STREAM_PROCESSED_RPS.labels(query=name).set(processed_rps)
        if duration_ms is not None:
            metrics.STREAM_BATCH_DURATION.labels(query=name).observe(duration_ms / 1000.0)

        # Freshness. The StreamStalled alert is `time() - this > 120`, so it must
        # be set on EVERY progress event, including empty batches: an idle query
        # is still a healthy query.
        import time as _time

        metrics.STREAM_LAST_PROGRESS.labels(query=name).set(_time.time())

        self._record_kafka_lag(progress, name)
        self._record_watermark_drops(progress, name)

        # One INFO line per micro-batch is the right granularity (SPEC 10.1):
        # frequent enough to see the pipeline breathing, rare enough to read.
        num_input = _as_float(progress.numInputRows) or 0
        if num_input > 0:
            self.log.info(
                "micro-batch complete",
                extra={
                    "event": "micro_batch",
                    "query": name,
                    "batch_id": progress.batchId,
                    "input_rows": int(num_input),
                    "duration_ms": duration_ms,
                    "watermark": str(getattr(progress, "eventTime", {}).get("watermark"))
                    if isinstance(getattr(progress, "eventTime", None), dict)
                    else None,
                },
            )

    # --- the two interesting metrics ---------------------------------------

    def _record_kafka_lag(self, progress, name: str) -> None:
        """Offsets behind latest, summed across the query's Kafka sources."""
        total = 0.0
        found = False
        for source in progress.sources or []:
            source_metrics = getattr(source, "metrics", None) or {}
            # Spark exposes this as `maxOffsetsBehindLatest` on newer versions and
            # as the per-partition `offsetsBehindLatest` on others; accept either
            # rather than pinning the listener to one Spark patch release.
            for key in ("maxOffsetsBehindLatest", "offsetsBehindLatest"):
                value = _as_float(source_metrics.get(key))
                if value is not None:
                    total += value
                    found = True
                    break
        if found:
            metrics.STREAM_OFFSETS_BEHIND.labels(query=name).set(total)

    def _record_watermark_drops(self, progress, name: str) -> None:
        """Rows the watermark discarded, from the stateful operators.

        `numRowsDroppedByWatermark` is cumulative per operator for the lifetime of
        the query, so the Prometheus COUNTER is incremented by the delta.

        A non-zero value here is expected, not a fault: about 2% of events are
        injected late by up to 20 simulated minutes against a 10-minute watermark.
        This metric is the direct, measured evidence for the report's claim that
        the speed layer is approximate and the batch layer is authoritative.
        """
        current = 0.0
        for operator in getattr(progress, "stateOperators", None) or []:
            value = _as_float(getattr(operator, "numRowsDroppedByWatermark", None))
            if value is not None:
                current += value

        previous = self._last_dropped.get(name, 0.0)
        # A restart resets the query's counters; treat a decrease as a fresh start
        # rather than incrementing by a negative number (which Prometheus rejects).
        delta = current - previous if current >= previous else current
        if delta > 0:
            metrics.LATE_ROWS_DROPPED.labels(query=name).inc(delta)
            self.log.info(
                "watermark dropped late rows (expected: ~2%% of events are injected late)",
                extra={
                    "event": "late_rows_dropped",
                    "query": name,
                    "rows": int(delta),
                },
            )
        self._last_dropped[name] = current
