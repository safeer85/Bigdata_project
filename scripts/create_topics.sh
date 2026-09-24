#!/usr/bin/env bash
# Create the project's Kafka topics (SPEC 6). Idempotent: `--if-not-exists` means
# `make up` on an existing stack is a no-op rather than an error.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
TOPIC="${TELEMETRY_TOPIC:-fleet.telemetry}"
DLQ="${DLQ_TOPIC:-fleet.telemetry.dlq}"
PARTITIONS="${TELEMETRY_PARTITIONS:-6}"
RETENTION_MS="${TELEMETRY_RETENTION_MS:-604800000}"   # 7 days

KAFKA_BIN=/opt/kafka/bin

echo "waiting for broker ${BOOTSTRAP}"
for _ in $(seq 1 60); do
  if "${KAFKA_BIN}/kafka-broker-api-versions.sh" --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# fleet.telemetry: 6 partitions, keyed by vehicle_id.
#
# Partitioning by vehicle_id is a correctness requirement, not a throughput one.
# Kafka guarantees order only within a partition, and the speed layer's per-vehicle
# state machine (idle_since, alert_open) assumes it sees one vehicle's events in
# the order they happened. Keying by vehicle_id puts every event for a vehicle in
# the same partition, which gives us that order.
#
# Retention is 7 days, and that is all it needs to be: it covers speed-layer
# recovery and archiver catch-up. Long-term history lives in the Parquet lake.
# This is precisely the Lambda argument -- Kappa would need retention long enough
# to replay an arbitrary past day, which for a financial recompute means forever.
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" \
  --create --if-not-exists \
  --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor 1 \
  --config "retention.ms=${RETENTION_MS}" \
  --config "cleanup.policy=delete"

# The dead-letter topic needs no ordering and carries a trickle of traffic, so one
# partition keeps every rejected event in a single readable stream for the demo.
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" \
  --create --if-not-exists \
  --topic "${DLQ}" \
  --partitions 1 \
  --replication-factor 1 \
  --config "retention.ms=${RETENTION_MS}"

echo "--- topics ---"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC}"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe --topic "${DLQ}"
echo "topic setup complete"
