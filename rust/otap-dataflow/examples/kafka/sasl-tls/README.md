# Kafka SASL over TLS: end-to-end validation

This example validates the OTAP Kafka **exporter and receiver over an
authenticated, encrypted connection**: SASL authentication (PLAIN,
SCRAM-SHA-256, SCRAM-SHA-512) layered on TLS transport encryption. It runs a
fully containerized stack, so no local Rust toolchain, OpenSSL, or `libclang`
is required.

Consumer-group coordination is **out of scope** here and is validated
separately. Each mechanism runs an independent producer/consumer pair against
its own topic so the three authentication paths are verified in isolation.

| Mechanism       | Topic                | Consumer group          |
| --------------- | -------------------- | ----------------------- |
| `PLAIN`         | `otlp-logs-plain`    | `otap-plain-consumer`   |
| `SCRAM-SHA-256` | `otlp-logs-scram-256`| `otap-scram-256-consumer` |
| `SCRAM-SHA-512` | `otlp-logs-scram-512`| `otap-scram-512-consumer` |

The credentials and generated certificates are for local development only.

## What runs

The stack runs this path independently for each mechanism:

```mermaid
flowchart LR
  gen[traffic-generator<br/>synthetic OTLP logs] --> pex[kafka exporter]
  pex -->|SASL over TLS| topic[(mechanism topic)]
  topic -->|SASL over TLS| rec[kafka receiver]
  rec --> con[console exporter]
  auth[Test-KafkaAuth.ps1] -. broker-only preflight .-> topic
```

| Service      | Role                                                                     |
| ------------ | ------------------------------------------------------------------------ |
| `certgen`    | Generates the local CA + broker cert (SAN covers `kafka`), JKS stores, JAAS |
| `kafka`      | Single-node KRaft broker; `SASL_SSL` listener advertised as `kafka:9093` |
| `kafka-init` | Creates the SCRAM users and the three per-mechanism topics               |
| `dataflow`   | `df_engine`: runs all six pipelines (a producer + consumer per mechanism)|

All six pipelines run in the **single** `dataflow` container. Each producer is a
traffic generator feeding a Kafka exporter; each consumer is a Kafka receiver
feeding a console exporter. Every node authenticates with SASL over TLS and
trusts the CA that `certgen` wrote into `./certs` (mounted read-only at
`/home/nonroot/certs`).

## Prerequisites

- Docker with Compose v2 (`docker compose version`).
- No local Rust toolchain needed; the engine image is built by Compose. The
  first build compiles `df_engine` and can take 10-30+ minutes. Subsequent runs
  are cached.

## Quick start

```bash
cd rust/otap-dataflow/examples/kafka/sasl-tls
docker compose -f compose.yaml -f compose.dataflow.yaml up --build
```

`certgen` mints the certificates, `kafka-init` creates the SCRAM users and
topics, then the `dataflow` engine connects to `kafka:9093` over SASL/TLS. Each
producer emits a bounded batch (20 signals, `pre_generated`) and stops; the
receivers keep running so you can inspect the groups. Use a second terminal for
the checks below.

## Validation

### 1. The broker accepts each mechanism over TLS (optional preflight)

This is a broker-only check using Kafka's own client tools; it does not involve
`df_engine`. On Windows PowerShell:

```powershell
./scripts/Test-KafkaAuth.ps1
```

Expected:

```text
PASS: PLAIN over TLS - kafka:9093 ...
PASS: SCRAM-SHA-256 over TLS - kafka:9093 ...
PASS: SCRAM-SHA-512 over TLS - kafka:9093 ...
```

Portable equivalent for a single mechanism (repeat with the other users):

```bash
docker compose exec -T kafka bash -lc '
cat >/tmp/client.properties <<EOF
security.protocol=SASL_SSL
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.jks
ssl.truststore.password=changeit
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="scram256" password="scram256-secret";
EOF
kafka-broker-api-versions --bootstrap-server localhost:9093 \
  --command-config /tmp/client.properties >/dev/null && echo "PASS: SCRAM-SHA-256 over TLS"'
```

### 2. The engine produced and consumed over SASL/TLS

The `dataflow` container's log is the end-to-end proof: every receiver had to
complete a SASL/TLS handshake to be assigned its partition, and the console
exporter only prints records the receiver decoded.

```bash
# Each consumer pipeline acquired its topic partition after authenticating.
docker compose logs dataflow | grep -i partitions_assigned

# The console exporters emitted decoded OTLP logs (resource/scope present).
docker compose logs dataflow | grep -iE "resource|scope" | head
```

Expected: a partition-assignment line for each of `plain-consumer`,
`scram-256-consumer`, and `scram-512-consumer`, and decoded log output. Console
output from the concurrent pipelines is interleaved, so use the per-group check
below to confirm each mechanism drained its topic.

### 3. Each consumer group reached zero lag

Ask the broker to describe each group directly. Zero lag proves the receiver
authenticated, consumed every produced message, and committed its offsets.

```bash
for g in otap-plain-consumer otap-scram-256-consumer otap-scram-512-consumer; do
  docker compose exec -T kafka \
    kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group "$g"
done
```

Expected for every group: `CURRENT-OFFSET` equals `LOG-END-OFFSET` and `LAG` is
`0` for the partition. If the engine is stopped before you check, the broker may
report the group has no active members; committed offsets and zero lag remain
valid delivery evidence.

## Configuration knobs

| Variable         | Default                      | Effect                                   |
| ---------------- | ---------------------------- | ---------------------------------------- |
| `KAFKA_BROKERS`  | `kafka:9093`                 | Broker bootstrap address (SASL/TLS listener) |
| `KAFKA_CA_FILE`  | `/home/nonroot/certs/ca.crt` | CA the engine trusts for the broker cert |

Both are set on the `dataflow` service in `compose.dataflow.yaml` and consumed
by `kafka-sasl-tls.yaml` via `${env:...}` substitution.

## Troubleshooting

Inspect container logs:

```bash
docker compose logs --no-log-prefix certgen
docker compose logs --no-log-prefix kafka
docker compose logs --no-log-prefix dataflow
```

Regenerate certificates and broker state from scratch:

```bash
docker compose -f compose.yaml -f compose.dataflow.yaml down -v
rm -rf certs
docker compose -f compose.yaml -f compose.dataflow.yaml up --build
```

## Cleanup

```bash
docker compose -f compose.yaml -f compose.dataflow.yaml down -v
rm -rf certs
```
