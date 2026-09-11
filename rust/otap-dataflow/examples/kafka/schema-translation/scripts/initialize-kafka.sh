#!/bin/bash
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic schema-demo-logs --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server kafka:29092 --describe --topic schema-demo-logs
