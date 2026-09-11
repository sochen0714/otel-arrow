# Advanced translation and host-driven validation

For the Docker-only demo and Redpanda Console UI, start with
[README.md](README.md). This page describes the translator contract and the
optional host-Python workflow, including automated consumer stop/rejoin cases.

This local design tool translates the proposed **User-Facing Kafka
Configuration Schema** into a complete OTEL-Arrow engine configuration. It
follows the draft's customer object, validation rules and native mappings in
Sections 2-6, as reviewed on 2026-09-10. It is not a production Operator,
a supported public API, or a replacement for the design document.

```text
customer.yaml                     bindings.yaml + explicit platform policy
       |                                           |
       +---------------- translate.py -------------+
                              |
             generated/<project>/consumer-a.yaml
             generated/<project>/consumer-b.yaml
                              |
                  df_engine --validate-and-exit
                              |
                 Kafka over SASL/TLS -> receivers -> noop
```

The Kafka demos supply reusable local infrastructure, **not product
requirements or authoritative defaults**. Each generated file runs one
receiver pipeline; two processes share a consumer group and split partitions.
The `noop` sink acknowledges and discards data. It does not represent durable
downstream storage.

## Prerequisites

- Python 3.10+ and PyYAML (`requirements.txt`).
- Docker Desktop running Linux containers, with Docker Compose v2 for the
  native and live demonstrations.
- Enough resources to build the engine, or an explicitly chosen existing
  engine image with Kafka receiver/exporter support.

Run commands from this directory. Windows examples use `py -3`; on other
platforms substitute `python3`.

```powershell
py -3 -m pip install -r requirements.txt
```

## Translate the proposed customer object

`customer.yaml` is a customer receiver object, **not native YAML**.
`bindings.yaml` is a local stand-in for the Operator's secret resolution.

```powershell
py -3 translate.py customer.yaml --bindings bindings.yaml --instance-id consumer-a --membership dynamic --rebalance-strategy cooperative_sticky --auth-policy sasl-only --output generated\consumer-a.yaml
py -3 translate.py customer.yaml --bindings bindings.yaml --instance-id consumer-b --membership dynamic --rebalance-strategy cooperative_sticky --auth-policy sasl-only --output generated\consumer-b.yaml
```

Translation does not connect to Kafka or read secret values. Generated files
contain environment placeholders and runtime certificate paths, never raw
credentials. Repeating translation with the same inputs produces the same
configuration.

For immediate native validation, add `--validate-image <existing-engine-image>`.
Set the raw environment variables named in the bindings first. The script
prepares the encoded environment in memory and runs the engine with no network
access. This checks native syntax and component configuration, not secret-file
availability or broker authentication.

### Customer-to-native coverage

| Customer field | Native translation |
| --- | --- |
| `name`, `type: Kafka` | Receiver node name and Kafka receiver URN; harness derives `client_id` using source name and process identity |
| `kafka.brokers[]` | Comma-separated `brokers` |
| `auth.sasl.mechanism` | PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512 |
| `auth.sasl.credentialReference` | Environment references for `username` and `password` |
| `tls.caReference` | `tls.ca_file`; `tls: {}` retains system trust |
| `tls.mtlsClientCertificateReference` | `tls.cert_file`, `tls.key_file`, optional `tls.key_password` |
| `subscription.consumerGroup` | `group_id`, unchanged for every process |
| `subscription.signals.{traces,metrics,logs}` | Separate native signal objects; `otlpProto`/`otapProto` become `otlp_proto`/`otap_proto` |
| `excludeTopics` | Per-signal `exclude_topics` |
| `delivery.readCommitted` | `read_committed` or `read_uncommitted` |
| `enrichment.resourceAttributesFromHeaders[]` | Header-keyed map of `key` and `value_type` |
| `observability.consumerLag` | Emit positive `lag_refresh_interval_ms` only when enabled |

The draft's fixed behavior is preserved: manual commits, `latest` for a group
without valid committed offsets, and native idempotency disabled. Removed
settings such as `startAt`, `deduplicate`, `atMostOnce` and `delivery.guarantee`
are rejected, as are unknown customer fields and raw native passthrough.

### Explicit local platform policy

These are translator arguments, **not new customer fields**:

| Argument | Meaning |
| --- | --- |
| `--instance-id` | Local process identity; generates a client label distinct from Kafka's broker-assigned member ID |
| `--membership dynamic` | Omit `group_instance_id` |
| `--membership static` | Generate `group_instance_id` from source name and instance identity; the native receiver adds a core suffix in multi-core mode |
| `--rebalance-strategy` | Explicitly select `range`, `round_robin`, `cooperative_sticky`, or `native-default` (omit the native field) |
| `--auth-policy sasl-only` | Require SASL over TLS for this local run |
| `--auth-policy authenticated` | Permit SASL and/or an mTLS client certificate |
| `--auth-policy allow-anonymous` | Also permit TLS without client authentication, for deliberate local experiments |

There is no implicit policy selection. The runnable example chooses dynamic
membership, cooperative-sticky assignment and SASL over TLS. Those choices
do **not** resolve the production membership, assignor or authentication
decisions. A static ID must be unique per member within a group and stable
across intended restarts; copying one static config to multiple processes is
not safe. This harness cannot verify cross-deployment identity uniqueness.

### Local secrets, not a finalized reference contract

For this prototype, opaque reference strings are lookup keys in
`bindings.yaml`. The referenced bindings are exactly one of:

```yaml
references:
  credentials:
    usernameEnv: MY_KAFKA_USERNAME
    passwordEnv: MY_KAFKA_PASSWORD
  custom-ca:
    caFile: /runtime/ca.crt
  client-certificate:
    certFile: /runtime/client.crt
    keyFile: /runtime/client.key
    keyPasswordEnv: MY_KEY_PASSWORD  # Optional
```

These paths must exist **inside the engine's runtime**, not necessarily on
the host. The final platform reference type, provider, access controls and
rotation behavior remain out of scope. Kubernetes Secret creation and mounts
are not implemented.

The engine substitutes environment variables into raw text before parsing
YAML. Simply placing a raw password inside quoted YAML would mishandle quotes
and newlines. `runtime_environment()` therefore prepares in-memory
`<VARIABLE>__JSON` values containing complete JSON-quoted strings; generated
YAML uses unquoted `${env:<VARIABLE>__JSON}` placeholders. JSON strings are
valid YAML scalars. Literal dollar signs in customer data are escaped for
the engine's substitution pass.

The launcher prepares and forwards these values. Do not start a generated
file without preparing its runtime environment. Environment projection
requires a process restart for rotation; it is not live secret reload.

## Local multi-consumer demonstration

The launcher generates both receiver configs, starts an isolated SASL/TLS
broker, checks the configs with the native engine, then starts two consumers
and produces a bounded batch **after partitions are assigned**. This ordering
is necessary
because the customer contract uses `latest`, not the demos' older `earliest`
setting. It checks shared partition ownership, committed-offset advancement
and lag, then exercises a member leaving and rejoining. Each phase produces
600 single-record Kafka messages, requires all three partitions to advance,
and waits for committed offsets to reach the new ends. The bounded traffic
generator stops emitting but keeps its engine process alive; the launcher
explicitly stops it after observing progress, rather than waiting for exit.

```powershell
py -3 scripts\run_demo.py
```

The default builds the engine from this checkout. To deliberately exercise
an existing demo image instead:

```powershell
py -3 scripts\run_demo.py --image otap-kafka-consumer-groups:latest
```

A prebuilt image validates compatibility with **that image's native schema**,
not necessarily the current checkout. The launcher prints its image identity
so evidence can be tied to the actual binary.

Every run uses a unique `kafka-schema-<id>` Compose project and writes configs
and `evidence.json` to `generated/<project>/`. Evidence contains image identity,
platform choices, assignments, member IDs and before/after offsets. Generated
output is ignored by Git.

The broker has three partitions. Default consumers have one engine core each;
this alternative exercises two cores in one process and static membership:

```powershell
py -3 scripts\run_demo.py --image otap-kafka-consumer-groups:latest --consumer-a-cores 2 --membership static
```

Consumer core counts are local harness settings, not customer schema fields.
`--no-restart` runs only the initial two-consumer phase. `--timeout` controls
the startup and per-phase deadline (240 seconds by default); `--build-timeout`
controls the source build deadline (3600 seconds by default).

Containers, networks and volumes are project-scoped; no ports are published
to the host. Broker administration uses a plaintext listener inside the
private Docker network, while receivers and producer use SASL/TLS. Credentials
and certificates are freshly generated per run. Engines mount only the public
CA; broker private keys stay in a separate volume. This is local infrastructure,
not a hardened production deployment.

Normal completion and handled failures remove the project's containers,
network and volumes, including private certificate material. Configs and
evidence remain on disk. To keep containers for inspection, use `--keep-running`;
after inspection or an abrupt launcher termination, clean up the exact printed
project name:

```powershell
py -3 scripts\run_demo.py --cleanup kafka-schema-0123456789ab
```

Replace the example project name with the one printed by your run. This does
not stop Docker Desktop or touch unrelated Compose projects.

## Focused checks

```powershell
py -3 -m unittest discover -p "test*.py" -v

# Additionally invoke an installed engine's real component validators.
$env:OTAP_SCHEMA_TEST_IMAGE = "otap-kafka-consumer-groups:latest"
py -3 -m unittest discover -p "test*.py" -v
Remove-Item Env:OTAP_SCHEMA_TEST_IMAGE
```

Unit cases cover every mapped customer field, invalid/removed fields,
membership policies, SASL and mTLS shapes, header conversion, durations and
secret escaping. Native checks include an invalid regex that must fail.
Launcher cases reject incomplete assignments, zero-lag false positives, partial
production and lagging commits, and cover producer shutdown and scoped cleanup.
`--validate-and-exit` checks registered components and native configuration;
it does not establish broker reachability, authentication success or
downstream durability. The live scenario covers those broker interactions
only for its selected SASL mechanism and log encoding.

## Design boundaries surfaced by translation

- The draft's short required-fields sentence omits `type` and `tls`, but
  its field table and security rules require them. The prototype follows
  the table and rejects missing TLS rather than silently using plaintext.
- A new group still starts at `latest`; creating a new group alone does not
  replay retained history. Offset resetting remains outside this schema.
- Native regex validation is authoritative. Python and Rust/Kafka regex
  dialects differ, so the translator does not claim to validate patterns
  using Python's regex engine. Exact cross-signal overlaps are rejected;
  overlaps between different regexes remain unsupported.
- The prototype accepts positive durations with `h`, `m`, `s`, and `ms`,
  including compound/fractional values representable as whole milliseconds.
  The final duration grammar still needs to be documented in the contract.
- Duplicate YAML keys, aliases, duplicate header sources, duplicate topic
  entries and ambiguous local bindings are rejected. These are deliberate
  prototype safeguards, not claims that the final API has adopted them.
- The local source/instance naming convention is 1-128 ASCII letters,
  digits, dots, hyphens or underscores, starting with a letter or digit.
  This is not a finalized customer name restriction.
- Lag is explicitly enabled at 5 seconds in the example so progress is
  visible. Omitted observability follows the draft's disabled default;
  this does not settle the PM's default-observability question.
- Supporting traces/metrics mappings and mTLS shape translation does not
  establish initial product scope or prove live mTLS authentication.
