# Kafka configuration demo with Redpanda Console

This demo illustrates how the proposed **User-Facing Kafka Configuration
Schema** maps to native OTEL-Arrow configuration. It runs two Kafka consumers
and provides Redpanda Console for inspecting topics, partitions, and consumer
groups.

The setup supports schema design review and validates configuration translation,
using local substitutes for platform deployment and secret resolution.

## Start with one command

From this directory, with Docker Desktop running Linux containers:

```powershell
docker compose -f compose.ui.yaml up --build --wait --wait-timeout 300
```

Then open **<http://localhost:8085>**.

**No Python, pip or Rust installation is needed on the host.** Python and its
dependencies run inside Docker. No Docker socket is mounted into containers.

The default reuses the local `otap-kafka-consumer-groups:latest` engine
image; it builds the Python wrapper image, not the Rust engine. This engine
image must already exist locally. The demonstrated binary is version 0.51.0.

The command waits until both consumers have partitions, 600 new messages have
been produced and committed, and the UI is healthy. It then returns, leaving
Kafka, both consumers and the UI running for inspection.

## What happens

```text
customer.yaml + bindings.yaml
    -> Python translator inside Docker
    -> generated/ui/consumer-a.yaml and consumer-b.yaml
    -> two OTEL-Arrow consumer containers
    -> Kafka topic shared across the consumers
    -> Redpanda Console displays topics, partitions and consumer groups
```

Kafka remains the broker. **Redpanda Console is the UI**, not a replacement
broker. Both consumers use **dynamic membership**, the same consumer group and
the same topic. A and B are two instances of one customer receiver definition.

## Demo containers

In Docker Desktop, `kafka-schema-ui` is the Compose project that groups the
containers below. The `-1` suffix identifies the first container for a service.

| Container | Role in Demo |
| --- | --- |
| `prepare-1` | Translates `customer.yaml` into two native consumer configs, copies the producer template, and creates or reuses local credentials in a private volume. |
| `certgen-1` | Uses OpenSSL and Java `keytool` to prepare the local CA, broker certificate and Java keystores needed for Kafka TLS. Reuses matching, unexpired material on subsequent starts. |
| `kafka-1` | Runs the Kafka broker. Stores messages and manages partitions, consumer-group membership and committed offsets. |
| `kafka-init-1` | Uses Kafka's command-line tools to create the `schema-demo-logs` topic with three partitions. This is a setup job, not a second broker. |
| `consumer-a-1` | Loads `consumer-a.yaml` and runs the Rust OTEL-Arrow engine. Consumes its assigned partitions and sends decoded logs to a `noop` sink. |
| `consumer-b-1` | Loads `consumer-b.yaml` and runs a second engine instance in the same group. Shares the topic's partitions with A. |
| `verify-1` | Waits for both consumers to receive assignments, runs an OTEL-Arrow producer process to send 600 sample logs, checks committed-offset progress, and saves `evidence.json`. |
| `console-1` | Runs Redpanda Console at `localhost:8085` to display Kafka topics, messages, partitions and consumer groups. |

## Which files matter

| File | Purpose |
| --- | --- |
| `customer.yaml` | User-facing input; its example values are written here |
| `bindings.yaml` | Local lookup of credential and certificate references |
| `translate.py` | Converts customer fields into native receiver fields |
| `scripts/container_demo.py` | Runs setup, engine startup and progress checks inside Docker |
| `compose.ui.yaml` | Starts and connects all containers |
| `generated/ui/consumer-a.yaml` | Generated native config for consumer A |
| `generated/ui/consumer-b.yaml` | Generated native config for consumer B |
| `generated/ui/evidence.json` | Before/after offsets and the result of the latest run |

Generated files are saved on your machine, not hidden inside the containers.
Credentials are kept separately in a private Docker volume, not those YAML
files. The example is logs-only and omits optional enrichment.

## What to look at in the UI

1. **Topics -> `schema-demo-logs`**: three partitions and sample messages.
2. **Consumer Groups -> `schema-demo-group`**: two members, their partition
   assignments and lag.

Messages contain binary OTLP protobuf, not plain JSON. Without a protobuf
decoder, use the UI's binary/hex representation to inspect payloads. The
receiver's `noop` sink acknowledges and discards data; this is not a downstream
durability or exactly-once demonstration.

Each successful startup sends one bounded batch of 600 messages, not continuous
traffic. Repeating the command sends another batch and refreshes the evidence.
For automated stop/rejoin experiments, see [ADVANCED.md](ADVANCED.md).

## Stop and remove the demo

```powershell
docker compose -f compose.ui.yaml down --volumes
```

This removes this demo's containers and volumes, including its local secrets.
The files in `generated/ui/` remain. Other Docker projects are not affected.
Certificates expire after two days; remove the volumes and start again to
generate fresh certificates.

## Local settings

Optional settings can go in a local `.env` file, which Git ignores:

| Setting | Purpose |
| --- | --- |
| `KAFKA_DEMO_UI_PORT=8086` | Use another loopback port if 8085 is occupied |
| `KAFKA_DEMO_IMAGE=your-existing-image:tag` | Choose another compatible Kafka-enabled engine image |
| `KAFKA_DEMO_PIP_INDEX_URL=https://your-mirror/simple/` | Use an approved, unauthenticated Python package mirror during the build |

The build defaults to PyPI. On restricted networks, use your organization's
approved mirror; do not disable TLS checks or put credentials in build
arguments. A local `.env` keeps the startup command unchanged.

Only the UI is published, on `127.0.0.1`. Kafka and engines stay on an internal
Docker network; Console also uses a bridge for the loopback UI port. Console
analytics is disabled. Its broker administration uses the internal plaintext
listener; producer and receiver data connections use SASL/TLS. This is local
demo infrastructure, not a hardened production deployment.

The demo does not resolve production membership policy, secret-reference
contracts, signal scope or the default consumer-lag decision. Detailed mappings
and the original host-Python workflow are in [ADVANCED.md](ADVANCED.md).
