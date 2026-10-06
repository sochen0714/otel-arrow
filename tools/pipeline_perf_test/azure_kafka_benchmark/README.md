# Azure Kafka latency probes

Bounded latency probes for an **existing** Kafka -> OTAP Dataflow Engine (DFE)
-> Azure Monitor / Log Analytics pipeline. This is a standalone measurement
utility, not a throughput orchestrator or completed dashboard CSV integration.
It does not provision Azure resources, alter a DCR, rebuild images, or change
the receiver, main load generator, broker, or consumer containers.

## What is measured

Each probe receives a fresh UTC timestamp immediately before its producer
enqueue attempt. The query measures the difference to Log Analytics
[`ingestion_time()`](https://learn.microsoft.com/en-us/kusto/query/ingestion-time-function?view=azure-monitor)
in milliseconds. This is **sampled source-to-ingestion latency**: it includes
producer queueing, Kafka, receiver processing, exporter batching/retries, and
Azure ingestion. It is not Azure-only latency, HTTP request latency, or exact
first-query visibility. `ingestion_time()` is approximate and uses the Azure
service clock; microsecond timestamp precision does not imply that accuracy.

Probes are separate RFC5424 records in the **same one-partition topic** as the
main workload. Each is exactly 1024 ASCII bytes, with a microsecond UTC time,
UUID run ID, and zero-based sequence in JSON in `SyslogMessage`.
`SyslogAppName=kbench-probe` and `SyslogMsgId=LA-LATENCY`. The existing
top-level `RunId` column is not populated or required. No DCR schema change is
needed. Unlike a generator that reuses a pool of records, timestamps are fresh
per send.

The default sends 1200 probes at 2/s (about ten minutes; 1,228,800 Kafka payload
bytes before Kafka/JSON/HTTP overhead). These are distinct probe records, not a
per-record measurement of the main synthetic workload. Their JSON/padding is
highly compressible and shares exporter batches with main traffic, but is not
payload-identical to random logs. Include probes in total load/row counts or
explicitly exclude them by app/run ID. At 100 logs/s, 2 probes/s adds 2% to the
record rate. Start a loaded-latency probe run only during a separately managed,
recorded measurement window. A probe-only run is an unloaded baseline.

## Prerequisites and defaults

Run from this checkout on an Ubuntu 24.04 x64 VM, as a normal user with
`sudo docker` access. Host Python 3 (3.10+) uses only its standard library; the
existing generator image supplies Python and `confluent-kafka`. There is no
installer, dependency installation, image pull, or rebuild in the launcher.

| Environment override | Default |
| --- | --- |
| `KAFKA_BENCH_IMAGE` | `kafka-syslog-generator:86c927622` |
| `KAFKA_BENCH_NETWORK` | `kafka-cloud-bench` |
| `KAFKA_BENCH_BROKER_CONTAINER` | `kafka-broker` |
| `KAFKA_BENCH_CONSUMER_CONTAINER` | `kafka-consumer` |
| `KAFKA_BENCH_BROKERS` | `kafka-broker:9092` (inside the Docker network) |
| `KAFKA_BENCH_TOPIC` | `otel-syslog` |
| `KAFKA_BENCH_ARTIFACTS` | `$HOME/kafka-bench-artifacts` |

Empty/unset overrides use these defaults. Broker addresses and topic must point
to the same pipeline as the inspected containers; no discovery or
reconfiguration is performed. The topic must already exist with one available
partition. The image must already be local and include the `python` entrypoint
and `confluent-kafka`; the supplied default uses Python 3.14.

The target pipeline must already parse RFC5424 and export to a table with
`TimeGenerated` (`datetime`), `SyslogAppName`, `SyslogMsgId`, and
`SyslogMessage` (`string`) columns. The query defaults to the generic
`SyslogKafkaBench_CL` table. If yours differs, change only that reference in
the generated `latency.kql` before executing it. Null ingestion timestamps
invalidate results.
Existing Azure Monitor JSON+gzip and managed-identity configuration remains
unchanged. No credentials, Azure resource IDs, or query permissions are added.

Keep the deployed DFE checkout and receiver-only checkout separate from this
scripts checkout. In particular, do not rebuild or replace a running
`otel-dfe:kafka-azure-3a0cd13a4` consumer from this branch. Its separate source
revision is `3a0cd13a4a3b9aaabeb819963b5089082baec434`; this scripts branch is
based on receiver-only revision `86c927622d1ac782e9082aa89042329c715d3996`.

## Safe preflight: no probe records

From the root of the new scripts checkout:

```bash
bash tools/pipeline_perf_test/azure_kafka_benchmark/run-probes.sh --help
bash tools/pipeline_perf_test/azure_kafka_benchmark/run-probes.sh --check
```

With no arguments, the launcher only displays help. `--check` checks host NTP
synchronization, image presence, running broker/consumer, and Kafka metadata.
It creates an automatically removed, read-only-mounted metadata-check container
on the existing network. It makes Kafka metadata requests but **produces no
records**, creates no topics, sends no Azure requests, and changes no existing
containers. A failed check exits nonzero. It does not prove end-to-end delivery.

The script locates `probe.py` and the KQL template relative to itself, so an
absolute script path works from any directory. Example overrides:

```bash
KAFKA_BENCH_TOPIC=otel-syslog \
KAFKA_BENCH_ARTIFACTS="$HOME/kafka-bench-artifacts" \
  bash tools/pipeline_perf_test/azure_kafka_benchmark/run-probes.sh --check
```

## Probe run: only when ready

`--run` is explicit consent to send new records through the existing pipeline,
including Azure ingestion and its associated costs:

```bash
bash tools/pipeline_perf_test/azure_kafka_benchmark/run-probes.sh --run
```

Optional shorter wiring check (not sufficient for a stable p99 estimate):

```bash
bash tools/pipeline_perf_test/azure_kafka_benchmark/run-probes.sh --run 20 2
```

Count must be an integer from 1 to 100000; rate must be positive and at most
10 probes/s. The launcher repeats preflight, then starts a **new temporary**
container from the generator image. It never commands an existing generator.
There is no continuous background load, catch-up burst, or retry of a probe
whose enqueue fails. The producer uses idempotent all-replica acknowledgments,
a bounded queue, 30-second message timeout, and a 35-second final flush.

Do not run concurrent probe campaigns unless explicitly accounted for. This
utility does not start, stop, synchronize, or validate a high-rate main
benchmark. Independently resolve exporter batch/API limits, resource limits,
and cost bounds before any high-load campaign.

## Evidence and query

Each run writes `$KAFKA_BENCH_ARTIFACTS/latency-<UUID>/` (the default is outside
the repository). Keep evidence private and outside source control:

| File | Contents |
| --- | --- |
| `events.jsonl` | Per-sequence enqueue and broker-delivery callback evidence |
| `producer-summary.json` | Planned/enqueued/acknowledged counts, errors, clock drift |
| `producer.log` | Producer stdout/stderr |
| `producer-exit-code.txt` | Docker/logging pipeline exit status; nonzero invalidates the query |
| `clock-before.txt`, `clock-after.txt` | Host synchronization checks |
| `host-started-at.txt`, `host-finished-at.txt` | Host UTC run boundaries |
| `pipeline-images.txt` | Running broker and DFE image IDs |
| `probe-image.txt` | Generator/probe image ID |
| `latency.kql` | Query with actual UUID, times, counts, and validity |

Failure exits nonzero and preserves available evidence. Missing summaries,
clock checks, or exit status are not treated as successful runs. Interrupted
runs may lack a query or final evidence and must not be used as valid results.
Do not hand-edit validity flags to obtain percentiles.

Open the target Log Analytics workspace's **Logs** view in KQL mode and paste
the generated query. Use its time range or a portal range covering the entire
run. The query is not submitted automatically; no additional query permission
is granted to the VM identity. Manually save results as CSV alongside private
evidence if needed. **Dashboard CSV import is not implemented here.**

The query deduplicates on sequence using the earliest ingestion time. It counts
missing, duplicate, malformed, negative-latency, missing-ingestion-time, and
conflicting-timestamp samples rather than claiming exactly-once delivery.
Only rows identifiable by the probe kind/run ID belong to the run; destroyed
identity metadata manifests as missing probes, not attributable malformed rows.
It suppresses `p50_ms`, `p95_ms`, and `p99_ms` while the run is incomplete or
producer/clock/data evidence is invalid. These columns are real-valued
millisecond estimates when present.

| Status | Interpretation |
| --- | --- |
| `INVALID_PRODUCER_OR_CLOCK` | Producer, launcher, acknowledgments, or clock checks failed |
| `INVALID_SAMPLE_DATA` | Malformed, negative, conflicting, or null-ingestion-time samples |
| `INCOMPLETE_WAIT_AND_REQUERY` | Not all expected sequences have been observed yet |
| `COMPLETE_SMALL_SAMPLE` | Complete, but fewer than 1000 valid probes; exploratory estimates |
| `COMPLETE` | Complete sampled run; still subject to measurement limitations |

Requery after ingestion catches up; do not resend the same run. Incomplete
coverage is not proof of permanent loss. The 1000-sample threshold is a
practical caution, **not a statistical confidence guarantee**.

Host synchronization is checked before/after, and clock steps exceeding 100 ms
relative to monotonic time abort production. This neither certifies UTC offset
nor removes cross-clock skew. Negative latency invalidates the result.
Broker acknowledgments establish neither Azure acceptance nor query visibility.
Percentiles combine all probes in an invocation; do not combine multiple
target-rate phases without a phase-specific query.

## Offline tests

From the repository root:

```bash
python3 -m unittest discover \
  -s tools/pipeline_perf_test/azure_kafka_benchmark -p 'test_*.py' -v
bash -n tools/pipeline_perf_test/azure_kafka_benchmark/run-probes.sh
```

Probe tests use a fake producer and clock; no Kafka package is required.
Launcher tests use Bash with mocked Docker/clock commands, not real services.
Set `BASH_EXE` to a Bash executable if it is not on `PATH` (for example, Git
Bash on Windows). Run launcher tests as a non-root user, like the launcher.
Tests cannot establish live ingestion, Azure query behavior, or actual latency.
