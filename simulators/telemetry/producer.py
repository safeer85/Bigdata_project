"""Kafka producer wrapper (SPEC 5.1).

The settings here are the ones that matter for a pipeline whose downstream
correctness depends on ordering and on not losing events:

  acks=all            the broker confirms only once the write is on the log. With
                      acks=1 a broker restart could silently drop events that the
                      simulator already counted as produced, and the drift metric
                      would then show a difference the pipeline did not cause.
  enable.idempotence  stops a retry from writing the same record twice at the
                      BROKER level. Note this is separate from the duplicate
                      faults we inject on purpose: those are two distinct
                      producer calls with the same event_id, and idempotence does
                      not (and must not) remove them.
  key=vehicle_id      puts each vehicle's events in one partition, which is what
                      gives the per-vehicle state machine ordered input.

Broker restarts must not kill the simulator (SPEC 5.1), so every failure path
logs, counts and backs off rather than raising.
"""
from __future__ import annotations

import time
from typing import Optional

from confluent_kafka import KafkaException, Producer

from common import config, metrics
from common.logging import get_logger, should_log_event

log = get_logger("telemetry-sim", stage="ingestion")


class TelemetryProducer:
    """Thin wrapper over confluent_kafka.Producer with the project's settings."""

    def __init__(self, topic: Optional[str] = None) -> None:
        self.topic = topic or config.TELEMETRY_TOPIC
        self.producer = Producer(
            {
                "bootstrap.servers": config.KAFKA_BOOTSTRAP,
                "acks": "all",
                "enable.idempotence": True,
                # With idempotence on, librdkafka caps in-flight requests at 5 and
                # preserves ordering per partition even across retries.
                "retries": 10,
                "retry.backoff.ms": 250,
                "delivery.timeout.ms": 60000,
                # Small linger: batching helps throughput, but a whole second of
                # linger would be a whole simulated minute of added latency.
                "linger.ms": 20,
                "compression.type": "lz4",
                "client.id": "fleet-telemetry-sim",
                "socket.keepalive.enable": True,
            }
        )

    def _delivery_report(self, err, msg) -> None:
        """Called by librdkafka once per message, on the polling thread."""
        if err is not None:
            metrics.PRODUCE_ERRORS.inc()
            log.warning(
                "kafka delivery failed",
                extra={"event": "produce_failed", "error": str(err)},
            )
            return
        if should_log_event():
            log.info(
                "event delivered (sampled 1 in %s)",
                config.LOG_SAMPLE_EVERY,
                extra={
                    "event": "event_delivered",
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )

    def send(self, value: bytes, key: str, event_type: str = "ping") -> bool:
        """Produce one message. Returns False if it could not even be queued."""
        try:
            self.producer.produce(
                topic=self.topic,
                key=key.encode("utf-8"),
                value=value,
                on_delivery=self._delivery_report,
            )
            metrics.EVENTS_PRODUCED.labels(event_type=event_type).inc()
            # poll(0) services delivery callbacks without blocking. Without it the
            # callback queue grows without bound and memory climbs for hours.
            self.producer.poll(0)
            return True
        except BufferError:
            # The local queue is full, which means the broker is slow or gone.
            # Block briefly to drain rather than dropping the event.
            log.warning(
                "producer queue full, flushing",
                extra={"event": "produce_queue_full"},
            )
            self.producer.poll(1.0)
            return False
        except KafkaException as exc:
            metrics.PRODUCE_ERRORS.inc()
            log.warning(
                "kafka produce error, backing off",
                extra={"event": "produce_error", "error": str(exc)},
            )
            time.sleep(1.0)
            return False

    def flush(self, timeout: float = 10.0) -> int:
        """Block until queued messages are delivered. Returns the number left."""
        return self.producer.flush(timeout)
