#!/usr/bin/env bash
# Creates the OTLP logs topic used by the consumer-group example.
#
# The topic is created with multiple partitions so that a single consumer
# group with several members can demonstrate coordinated consumption and
# partition assignment. Auto topic creation is disabled on the exporter, so
# the partition count established here is authoritative for the demo.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
TOPIC="${KAFKA_TOPIC:-otlp-logs}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"

echo "Creating topic '${TOPIC}' with ${PARTITIONS} partitions on ${BOOTSTRAP}"
kafka-topics \
  --bootstrap-server "${BOOTSTRAP}" \
  --create \
  --if-not-exists \
  --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor 1

echo "Topic '${TOPIC}' ready:"
kafka-topics --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC}"
