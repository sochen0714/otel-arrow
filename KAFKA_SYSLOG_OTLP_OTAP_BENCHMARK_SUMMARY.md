# Kafka Syslog to OTLP and OTAP: benchmark methodology and results

## Purpose and measured paths

Compare two downstream export protocols with the same raw Syslog Kafka input,
DFE Kafka receiver, downstream batch configuration, and consumer core allocation:

```text
Python/librdkafka generator -> Kafka [raw RFC 5424]
  -> Kafka receiver [Syslog parsing + Arrow construction]
  -> batch processor [up to 1000 logs / 200ms]
  -> OTLP/gRPC exporter [Protobuf]
  -> backend OTLP receiver -> Perf

Python/librdkafka generator -> Kafka [raw RFC 5424]
  -> Kafka receiver [Syslog parsing + Arrow construction]
  -> batch processor [up to 1000 logs / 200ms]
  -> OTAP exporter [Arrow]
  -> backend OTAP receiver -> Perf
```

These are backend-delivery benchmarks, not isolated parser or receiver tests.
The measured successful log counter is at the **backend Perf input**. This is
different from the receiver-only suite's local Perf endpoint. Neither path
uses the TCP/UDP Syslog receiver, and neither wraps the Kafka Syslog input in
OTLP.

The benchmark reuses the comparison dashboard wrapper and the existing
performance orchestrator for Docker deployment, monitoring, and SQL reporting.
No Rust receiver optimization was implemented for these measurements.
The results below are preserved historical measurements, not a rebenchmark of
the current main branch or this PR's checkout.

## Benchmark method

Each Kafka record value is one complete, uncompressed, 1024-byte RFC 5424
message with a plain non-CEF body. The Python producer has one thread and a
100-record scheduling batch; those records remain separate one-log values.
The broker has one topic, `otel-syslog`, one partition, and replication factor 1.

Both variants allocate one engine pipeline core to the consumer (index 1) and
one to the backend (index 2). The receiver, batch processor, and exporter share
the consumer pipeline core. The batch processor combines already-built small
Arrow payloads; it does not eliminate the receiver's per-record construction.

Runs were sequential, with 10 seconds of warmup, 20 seconds of observation,
then producer stop/flush and a 10-second bounded drain. Consumer shutdown has a
15-second timeout. Final backend counts include progress through consumer
shutdown, unlike the receiver-only suite's pre-shutdown local-sink cutoff.

There is a small timing difference in the current reproduction workflow:
historical runs sampled a Kafka record with the Kafka CLI after producer
stop/flush but before the 10-second drain. That CLI startup/read time allowed
additional consumer progress before the final cutoff. The current workflow
samples the record after final backend counter capture, avoiding that extra
delay before the cutoff. The final counts below retain the original historical
cutoff; the changed ordering has not been rebenchmarked.

Kafka and outbound protocol compression are disabled. OTAP uses five streams
per signal; the OTLP exporter uses its default five in-flight requests.
The matching backend receiver changes with the outbound protocol.

The two suites default to the six measured targets: 1k, 2k, 5k, 10k, 20k, and
100k logs/s. There are no matched-suite results for 200k through 1M logs/s.

## Metric definitions

**Actual input logs/s** counts broker-confirmed raw Kafka records.
**Backend logs/s** measures successful log items entering backend Perf during
observation. CPU and memory below belong to the **consumer container**,
including its receiver, batch processor, and exporter; they exclude the
backend, broker, and producer containers. CPU near 100% is approximately one
core, not 100% of the entire host.

Rate windows are independently sampled. A backend rate slightly above input
in a short low-rate observation does not demonstrate duplicates. Final
whole-workload counts are reported separately.

The generic dashboard's historical `dropped_logs_percentage` is an
observation-window counter-difference metric, not proof of permanent loss.
The red warning also uses a configured-target rate-shortfall heuristic.
Neither is an exact measurement of Kafka lag, blocking time, or network
saturation. Unrun target rates are unavailable data, not zero throughput.

## Recorded results

Measurements were recorded on 2026-09-29 UTC. The original run notes describe
native WSL Ubuntu 24.04, Docker Desktop's Linux engine 29.8.1, and Python 3.12.
The host exposed 16 logical CPUs / 8 physical cores of an AMD EPYC 7763 and
approximately 31 GiB RAM.

### Observation-window throughput and consumer resources

| Output | Target logs/s | Actual input logs/s | Backend logs/s | Avg CPU % | Peak memory MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| OTLP | 1,000 | 988.4 | 1,016.8 | 9.33 | 98.61 |
| OTAP | 1,000 | 994.3 | 965.3 | 8.26 | 174.52 |
| OTLP | 2,000 | 1,968.9 | 1,917.9 | 14.66 | 141.03 |
| OTAP | 2,000 | 1,971.1 | 2,027.3 | 14.52 | 168.29 |
| OTLP | 5,000 | 4,846.4 | 4,749.9 | 35.47 | 208.60 |
| OTAP | 5,000 | 4,825.5 | 4,980.5 | 37.47 | 202.80 |
| OTLP | 10,000 | 9,439.8 | 9,138.3 | 66.23 | 195.54 |
| OTAP | 10,000 | 9,374.2 | 9,193.6 | 72.42 | 203.20 |
| OTLP | 20,000 | 17,617.3 | 10,806.5 | 99.45 | 442.55 |
| OTAP | 20,000 | 17,564.3 | 3,546.1 | 99.91 | 628.89 |
| OTLP | 100,000 | 59,236.0 | 9,708.7 | 100.52 | 465.61 |
| OTAP | 100,000 | 63,035.1 | 2,712.2 | 100.17 | 555.40 |

At 10k targets both variants retained consumer CPU headroom. At 20k both used
approximately one core, and observed input exceeded backend throughput.
The tested points bracket saturation but do not locate an exact sustainable
operating rate. The producer did not reach the 100k target in either variant.
Higher targets were not run for these matched backend suites.

### Whole-workload final counts

| Output | Target | Broker-confirmed logs | Final backend Perf logs | Not observed by final cutoff |
| --- | --- | ---: | ---: | ---: |
| OTLP | 1k | 29,500 | 29,500 | 0 |
| OTAP | 1k | 29,750 | 29,750 | 0 |
| OTLP | 2k | 59,000 | 59,000 | 0 |
| OTAP | 2k | 59,100 | 59,100 | 0 |
| OTLP | 5k | 145,000 | 145,000 | 0 |
| OTAP | 5k | 144,800 | 144,800 | 0 |
| OTLP | 10k | 282,100 | 282,100 | 0 |
| OTAP | 10k | 280,800 | 280,800 | 0 |
| OTLP | 20k | 534,300 | 534,300 | 0 |
| OTAP | 20k | 530,200 | 530,200 | 0 |
| OTLP | 100k | 1,823,500 | 543,902 | 1,279,598 |
| OTAP | 100k | 1,879,400 | 160,501 | 1,718,899 |

The 1k smoke gates require complete aggregate delivery; all smoke totals
matched. The intermediate cases also matched after drain/shutdown, including
20k cases with observation-window backlog. Aggregate equality is not an
identity-based deduplication or field-fidelity guarantee.

The 100k deficits are records not observed by the final cutoff, not proven
receiver-side loss. The experiment later destroys the ephemeral Kafka broker,
so any retained backlog is not delivered afterward.

### Error and backpressure evidence

These are counters from the retained `producer-final.prom` (after producer
stop/flush) and `consumer-before-shutdown.prom` snapshots, not error-log counts
or observation-window rates. Consumer snapshots precede the final backend
capture and cannot establish what happened during shutdown.

| Evidence | Observed value | Interpretation |
| --- | --- | --- |
| `failed`, `kafka_delivery_failed`, `kafka_enqueue_failed` | Each 0 in all 12 cases | No producer failures reported by these counters; not an end-to-end delivery guarantee. |
| `kafka_flush_timeouts`, `kafka_pending` | Each 0 in all 12 cases | No reported producer flush timeout or remaining producer delivery acknowledgments at capture. |
| `kafka_queue_full` | 40 for OTLP 100k; 0 in the other 11 cases | Producer queue pressure, not evidence of 40 dropped logs; delivery/enqueue failure counters remained 0. |
| Consumer `batching_errors_total` | 0 in all 12 cases | Explicitly emitted zero batch-error counters before shutdown. |
| Consumer exporter `messages_total`, scope `exporter.exports`, outcome `success` | Positive in all 12 cases | Successful export messages, not individual log items or final backend counts. |
| Consumer exporter non-success outcome series | Absent in all 12 snapshots | Unavailable evidence, not measured zero failures. |

Consumer logs and post-shutdown monitoring logs are not included in the
selected evidence snapshot. Conditional failure series that were not emitted
are not treated as zeros. Parse/export error logs and shutdown/admin-connection
errors therefore cannot be independently quantified from this snapshot. No
absence-of-errors claim is inferred from the final delivery totals.
Original run notes report admin connection errors during monitoring teardown,
after endpoint shutdown and outside observation/final capture periods; those
are not observation failures.

## Batching and interpretation

Without downstream batching, one-log Arrow payloads result in one-log OTLP
requests. Reusing a gRPC connection does not remove encoding setup, request
framing, scheduling, backend dispatch, or completion handling. Finite exporter
request concurrency can backpressure the receiver even without saturated
network bandwidth.

Batching amortizes those costs across many logs but adds Arrow merge work.
The consumer CPU budget is shared by parsing, Arrow construction, batching,
and export, rather than independently provisioned for each stage. These
matched-suite counters do not attribute CPU time to individual stages or
explain all of OTAP's additional throughput deficit. Separate earlier batching
and profiling experiments are not included in these result tables.

Off-CPU queue, scheduling, and acknowledgment delays need further controlled
experiments. Do not interpret this local result as a general performance
ranking of OTLP and OTAP. Batch-aware receiver construction is a candidate
optimization, not a measured change in these suites.

## Reproduce and inspect

Follow
[Matched Syslog Kafka OTLP and OTAP benchmarks](tools/comparison_dashboard/README.md#matched-syslog-kafka-otlp-and-otap-benchmarks)
in the comparison dashboard README for the shared Kafka Syslog setup: Linux or
native WSL storage, Python 3.11+, Git, curl/coreutils, and Docker BuildKit.
Initialize submodules, install dashboard and orchestrator requirements into
the active venv, build the engine with `FEATURES=kafka`, and build the Python
Kafka Syslog generator. At least three logical CPUs are needed for indices
0, 1, and 2; leave additional resources for Kafka and the producer.

Run from `tools/comparison_dashboard` with the repository venv active:

```bash
python dashboard.py validate
python dashboard.py run \
  suites/dfe/dfe-logs-kafka-syslog-otlp-recv-baseline.yaml \
  --tests 1k,2k,5k,10k,20k,100k --observation-interval 20
python dashboard.py run \
  suites/dfe/dfe-logs-kafka-syslog-otap-recv-baseline.yaml \
  --tests 1k,2k,5k,10k,20k,100k --observation-interval 20
python dashboard.py build
python dashboard.py serve --port 3000
```

Open <http://localhost:3000/compare/kafka_receiver_syslog_otlp_otap/>.
Do not use `--clean` when preserving prior results. These commands combine the
previous smoke, intermediate, and overload invocations into one per protocol;
the original results retain their run IDs below. No benchmark workloads were
run while preparing this PR.

Network: `kafka-syslog-benchmark`. Shared container names and loopback ports:

| Container | Host -> container port | Role |
| --- | --- | --- |
| `load-generator` | 18085 -> 5001 | Start/stop/status/metrics API |
| `kafka-broker` | 19094 -> 19094 | External Kafka listener |
| `kafka-consumer` | 18088 -> 8080 | Engine admin/metrics |
| `backend-service` | 18087 -> 8080 | Engine admin/metrics |

Inside Docker, Kafka clients use port 9092 and the controller uses 9093.
Backend OTLP or OTAP data arrives on internal port 1235. Run suites
sequentially; names and ports overlap the receiver-only suite.

| Output | Targets | Original run ID |
| --- | --- | --- |
| OTLP | 1k | `20260929_210750` |
| OTAP | 1k | `20260929_210927` |
| OTLP | 2k, 5k, 10k, 20k | `20260929_221258` |
| OTAP | 2k, 5k, 10k, 20k | `20260929_221930` |
| OTLP | 100k | `20260929_212317` |
| OTAP | 100k | `20260929_212559` |

Run artifacts are written under
`tools/comparison_dashboard/.data/dfe_logs_kafka_syslog_{otlp,otap}_recv/<run-id>/tests/<rate>/`.
The selected local evidence snapshot retains `sql_report-*.json`,
`verified-delivery.json`, final producer/backend Prometheus snapshots,
consumer pre-shutdown metrics, `images.txt`, and `kafka-record.txt`. Full run
directories and the selected snapshot also retain both rendered configurations;
`timeseries.json` remains in the full run directories. Published metrics are in
`tools/comparison_dashboard/.site/data/suite/<suite-slug>/<rate>/`.
Raw outputs, the evidence manifest, and local profile files are not committed
by this PR.

All 12 SQL reports and 12 delivery-verification files were SHA256-checked
against the retained `syslog-protocol-pr-summary-evidence.json`. Their metric
values and final counts match the tables: rates are rounded to one decimal,
CPU and peak memory to two decimals, and final counts are exact integers.
The final producer/backend Prometheus counters also match all 12 count rows.

## Provenance and limitations

The engine image was reused unchanged for the consumer and backend:

| Image | Local Docker image ID |
| --- | --- |
| `df_engine:latest` | `sha256:be3432dd08060d6eb15299066f4993f7824a33b8f570b33491f58be7ca95af4d` |
| `load_generator:kafka-syslog` | `sha256:92ed2053f3f0c366fdd0e0210b081cd0930ab9a32946c7c197420e90e1c8f9a1` |
| `apache/kafka:latest` | `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` |

The retained engine reports v0.55.0; its exact build-source commit is
unverified. The recorded measurement checkout HEAD was
`3a0cd13a4a3b9aaabeb819963b5089082baec434` with local benchmark additions.
That SHA is not a verified image-build SHA. A clean build of a later revision
reproduces the workflow, not necessarily these numbers. Local image IDs
are not automatically pullable registry digests.

These are short local observations, not repeated-run medians, restart or
delivery guarantees, or production certification. The protocol difference,
shared CPU work, and off-CPU behavior need further controlled experiments
before attributing the entire performance gap to a single component.
