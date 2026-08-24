# Kafka end-to-end validation examples

Self-contained, containerized examples that validate the OTAP Kafka receiver
and exporter against a real broker. Each example targets one Kafka capability
and lives in its own folder with the Compose files, pipeline configs, and a
focused README needed to run it. The `df_engine` Dockerfile
([`Dockerfile.dataflow`](./Dockerfile.dataflow)) is shared by every example and
lives here at the `kafka/` root.

Every example follows the same pattern:

- a `compose.yaml` that stands up a Kafka broker (and any topic bootstrap), and
- a `compose.dataflow.yaml` overlay that builds one `df_engine` image (from the
  shared Dockerfile) and runs the producer/consumer engine containers on top of
  it.

```bash
cd rust/otap-dataflow/examples/kafka/<feature>
docker compose -f compose.yaml -f compose.dataflow.yaml up --build
```

Both features run entirely in containers; no local Rust toolchain or `cargo`
invocation is required.

Each stack also includes a [Redpanda Console](https://github.com/redpanda-data/console)
web UI (on <http://localhost:8082>) for browsing topics, messages, and consumer
groups from a browser. See each feature's README for details.

## Feature validation

| Feature                | Folder                                 | What it validates                                                                                   | Status    |
| ---------------------- | -------------------------------------- | --------------------------------------------------------------------------------------------------- | --------- |
| Consumer groups        | [`consumer-groups/`](./consumer-groups/) | Coordinated consumption and partition assignment across receiver instances, required `group_id`, and rebalance-aware offset handling | Available |
| SASL over TLS          | [`sasl-tls/`](./sasl-tls/)             | Broker authentication (SASL PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512) and transport encryption (TLS) for the receiver and exporter | Available |

Each capability is validated in isolation so results are unambiguous. For
example, the consumer-groups stack deliberately uses a plaintext broker so that
authentication and TLS do not affect the group-coordination assessment; SASL
and TLS are validated on their own in a separate stack.

## Prerequisites

- Docker with Compose v2 (`docker compose version`).
- No local Rust toolchain is required to run the examples; the `df_engine`
  image is built by Compose. The first build compiles the engine and can take
  10-30+ minutes, then is cached for subsequent runs.

See each feature folder's README for step-by-step validation instructions.
