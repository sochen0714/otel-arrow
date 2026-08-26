#!/usr/bin/env bash
# Creates the single-partition topic used by the offset-handling example.
#
# One partition makes the committed-offset watermark deterministic: the three
# produced Kafka messages receive offsets 0, 1, and 2.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
TOPIC="${KAFKA_TOPIC:-otlp-logs}"

echo "Creating single-partition topic '${TOPIC}' on ${BOOTSTRAP}"
kafka-topics \
  --bootstrap-server "${BOOTSTRAP}" \
  --create \
  --if-not-exists \
  --topic "${TOPIC}" \
  --partitions 1 \
  --replication-factor 1

echo "Topic '${TOPIC}' ready:"
kafka-topics --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC}"
