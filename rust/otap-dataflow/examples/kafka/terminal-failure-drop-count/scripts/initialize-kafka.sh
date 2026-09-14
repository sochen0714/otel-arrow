#!/usr/bin/env bash
set -euo pipefail

kafka-topics \
  --bootstrap-server "${KAFKA_BOOTSTRAP}" \
  --create \
  --if-not-exists \
  --topic "${KAFKA_TOPIC}" \
  --partitions 1 \
  --replication-factor 1
