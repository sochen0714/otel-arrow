# Kafka consumer groups: end-to-end validation

This example validates the OTAP Kafka **receiver's consumer-group behavior**
with a fully containerized stack: coordinated consumption and partition
assignment across multiple receiver instances, the required consumer
`group_id`, and rebalance-aware offset handling.

Authentication and TLS are **out of scope** here and are validated separately;
this stack uses a single plaintext KRaft broker so the consumer-group behavior
is isolated from transport security.

## What runs

```mermaid
flowchart LR
  gen[traffic-generator<br/>synthetic OTLP logs] --> pex[kafka exporter]
  pex -->|otlp-logs<br/>3 partitions| topic[(Kafka topic)]
  topic --> ca[consumer-a<br/>kafka receiver]
  topic --> cb[consumer-b<br/>kafka receiver]
  subgraph group["consumer group: otap-consumer-group"]
    ca
    cb
  end
  ca --> na[noop sink]
  cb --> nb[noop sink]
```

| Service      | Role                                                        |
| ------------ | ----------------------------------------------------------- |
| `kafka`      | Single-node plaintext KRaft broker (`cp-kafka:8.3.1`)       |
| `kafka-init` | Creates topic `otlp-logs` with **3 partitions**             |
| `producer`   | `df_engine`: traffic generator -> Kafka exporter            |
| `consumer-a` | `df_engine`: Kafka receiver (group `otap-consumer-group`)   |
| `consumer-b` | `df_engine`: Kafka receiver (same group, distinct client)   |

Both consumers run the **identical** `consumer.yaml`; only their
`KAFKA_CLIENT_ID` differs. Joining the same `group_id` is what makes the broker
distribute the topic's 3 partitions across them.

## Prerequisites

- Docker with Compose v2 (`docker compose version`).
- No local Rust toolchain needed; the engine image is built by Compose. The
  first build compiles `df_engine` and can take 10-30+ minutes. Subsequent runs
  are cached.

## Quick start

```bash
cd rust/otap-dataflow/examples/kafka/consumer-groups
docker compose -f compose.yaml -f compose.dataflow.yaml up --build
```

The producer sends a bounded batch (`KAFKA_MAX_SIGNAL_COUNT`, default 300) and
exits; the consumers keep running so you can inspect the group. `up` streams
logs in the foreground, so run the checks below from a **second** terminal.

The checks below use PowerShell (VS Code's default terminal). Set the compose
file list once so every command can omit the `-f` flags (otherwise Compose
fails with `no such service: consumer-a`):

```powershell
$env:COMPOSE_FILE = "compose.yaml;compose.dataflow.yaml"
```

## Web UI (Redpanda Console)

The stack includes a [Redpanda Console](https://github.com/redpanda-data/console)
container for browsing topics, messages, and consumer groups (with per-partition
lag) from a browser. It starts automatically with the broker; open
<http://localhost:8082>.

Under **Consumer Groups**, watch `otap-consumer-group` split its 3 partitions
across `consumer-a`/`consumer-b` and drain to zero lag - the same evidence as the
CLI checks below. Under **Topics** you can inspect `otlp-logs` and its messages.

## Validation

Run these from a second terminal (not the one running `up`, whose streaming
output mangles typed input).

### 1. Partition assignment is split across instances

Each receiver logs the partitions it is assigned during every rebalance.

```powershell
docker compose logs consumer-a | Select-String kafka.rebalance.partitions_assigned
docker compose logs consumer-b | Select-String kafka.rebalance.partitions_assigned
```

Expected: each consumer is assigned a **disjoint** subset of partitions
`{0,1,2}`, and the **union across both consumers covers all 3 partitions**. For
example one instance owns `{0,2}` and the other owns `{1}`. The exact split
depends on join timing; what matters is that the partitions are partitioned
(no overlap) and fully covered.

### 2. Coordinated consumption drains group lag to zero

Ask the broker to describe the group directly:

```powershell
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group otap-consumer-group
```

Expected:

- Two rows with distinct `CONSUMER-ID` / `CLIENT-ID` (`consumer-a`,
  `consumer-b`), each owning a subset of the partitions.
- Once the producer's bounded run is fully consumed, `LAG` is `0` for every
  partition. That confirms the members collectively consumed the whole topic
  and committed their offsets.

Lag is also exported per receiver as the `group_lag` gauge (OTel scope
`receiver.kafka.consumer`, refreshed every 5s) on each consumer's admin
endpoint:

```powershell
curl.exe -s localhost:8080/api/v1/telemetry/metrics | Select-String group_lag   # consumer-a
curl.exe -s localhost:8081/api/v1/telemetry/metrics | Select-String group_lag   # consumer-b
```

### 3. `group_id` is required

The receiver rejects an empty `group_id` at config validation. `consumer.yaml`
sets it via `KAFKA_GROUP_ID` (default `otap-consumer-group`). To see the guard,
run one engine with an empty value:

```powershell
docker compose run --rm --no-deps -e KAFKA_GROUP_ID= consumer-a --config file:/home/nonroot/consumer.yaml --validate-and-exit
```

Expected: validation fails because the consumer group id must be non-empty.

### 4. Rebalance-aware offset handling

Trigger a membership change and watch the surviving instance take over the
revoked partitions with no gap and no re-consumption.

```powershell
# Stop one member; consumer-a should be assigned the freed partitions.
docker compose stop consumer-b
docker compose logs --since 30s consumer-a | Select-String kafka.rebalance.partitions_assigned

# Re-describe: consumer-a now owns all 3 partitions and lag returns to 0.
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group otap-consumer-group

# Bring the second member back; partitions re-split across both.
docker compose start consumer-b
```

Because the receiver commits owned partitions **before** they are revoked
(commit-before-revoke) and uses manual commit (offsets committed only after the
downstream sink accepts the batch), a rebalance does not drop or double-count
in-flight data. `consumer-b`'s graceful stop commits its progress before
leaving, so `consumer-a` resumes exactly where it left off. The
`cooperative_sticky` strategy means only the partitions that actually move are
revoked and reassigned, not the entire assignment.

### 5. One instance, multiple cores (each core is a group member)

The engine is thread-per-core: `--num-cores N` runs the whole pipeline on N
pinned threads, and the Kafka receiver is instantiated per core. So a single
container joins the group as **N distinct members** - same `client_id`, but each
with its own broker-assigned `member.id`. Recreate `consumer-a` with 2 cores:

```powershell
$env:CONSUMER_A_CORES = "2"
docker compose up -d --no-deps consumer-a
```

Its two cores each log a partition assignment - note `core.id=0` vs `core.id=1`
in the same container's output:

```powershell
docker compose logs consumer-a | Select-String kafka.rebalance.partitions_assigned
```

```
partitions=otlp-logs:1 ... core.id=0
partitions=otlp-logs:0 ... core.id=1
```

`--describe` now shows three members - two of them are the single `consumer-a`
container (same HOST and CLIENT-ID, different CONSUMER-ID) plus `consumer-b`:

```powershell
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group otap-consumer-group
```

```
PARTITION  LAG  CONSUMER-ID              HOST         CLIENT-ID
1          0    consumer-a-00a790ef-...  /172.19.0.4  consumer-a   # core 0
0          0    consumer-a-78cb37e2-...  /172.19.0.4  consumer-a   # core 1
2          0    consumer-b-b04ddc6a-...  /172.19.0.6  consumer-b
```

Stop `consumer-b` and `consumer-a`'s two cores cover all three partitions by
themselves (for example core 0 owns `{1,2}` and core 1 owns `{0}`). Parallelism
is still capped by the partition count (3): members across every container and
its cores beyond 3 sit idle.

Return to the single-core default when done:

```powershell
Remove-Item Env:\CONSUMER_A_CORES
docker compose up -d --no-deps consumer-a
```

## Configuration knobs

| Variable                   | Default               | Effect                                         |
| -------------------------- | --------------------- | ---------------------------------------------- |
| `KAFKA_MAX_SIGNAL_COUNT`   | `300`                 | Total log records to produce; `null` for a continuous stream |
| `KAFKA_SIGNALS_PER_SECOND` | `50`                  | Producer emit rate                             |
| `KAFKA_GROUP_ID`           | `otap-consumer-group` | Consumer group all instances join              |
| `KAFKA_CLIENT_ID`          | per service           | Distinguishes instances in the group           |
| `KAFKA_TOPIC`              | `otlp-logs`           | Topic produced to / consumed from              |
| `KAFKA_BROKERS`            | `kafka:9092`          | Broker bootstrap address                       |
| `CONSUMER_A_CORES`         | `1`                   | Cores for `consumer-a`; each core joins the group as its own member (set `2`+ for case 5) |

## Cleanup

```powershell
docker compose down -v
```
