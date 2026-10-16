# Task 5750616 validation evidence

## Result and scope

**PASS: all ten real-broker runtime cases, twice.** Both supported username sources
delivered all three exact expected OpenTelemetry log bodies through the Kafka
receiver and console exporter. All six acquisition failures exited nonzero;
both broker-rejected credential pairs produced safe SASL authentication errors
and zero decoded output. No known synthetic credential contents appeared in
receiver logs. No production source or dependency changes were required.

Task: [5750616](https://dev.azure.com/msdata/91079a30-3e43-4a67-8253-5ca0050e01e1/_workitems/edit/5750616),
parent story 5647117, implementation task 5712126.
Only initial acquisition is assessed: no AKV, rotation, live consumer
recreation/rebalancing validation, or benchmarks.
Azure DevOps state was not changed by this validation session.

## Source and runtime provenance

The explicitly selected fork base, not the fork's older `main`, was used:

| Item | Value |
| --- | --- |
| Repository | `sochen0714/otel-arrow` |
| Selected base branch | `sochen0714-kafka-consumer-group-e2e-validation` |
| Engine source SHA | `cd29e57b470bb7555fe7cf13c5e91ebaf3a40e24` |
| Password-file implementation | `e65c92fe736f09f495bb715774fe830b857fd0ea`, merged in [open-telemetry/otel-arrow#4321](https://github.com/open-telemetry/otel-arrow/pull/4321) |
| Username-file implementation | `3366e0b2abdfa34855a703df1b0c1eeda600cb94`, merged in [open-telemetry/otel-arrow#4393](https://github.com/open-telemetry/otel-arrow/pull/4393) |
| Initial engine image | `otap-flat-file-sasl:5750616-cd29e57b4` |
| Initial image ID | `sha256:7c422335b3fe1100024c1326dcb8ac8d94c5511c0b688d11d1dc512b222616a4` |
| Repeat engine image | `otap-flat-file-sasl:cd29e57b470b`, built by the harness's default Compose path |
| Repeat image ID | `sha256:8f037068b4a992c152ab7519ed446beba346373400dba8cba828eb8ba44a1884` |
| Executable SHA-256, identical in both builds | `3fe737f04b051726f5961b31aed07c6573fd93c114fb1dd7437c212f49efb444` |
| Target | `x86_64-unknown-linux-gnu`, Linux containers on Docker Desktop |
| Build | Shared `Dockerfile.dataflow`, locked release build, `kafka,flat-file-sasl-auth` plus default features |
| CPU flags | `RUSTFLAGS=-C target-cpu=x86-64` |
| Broker | `confluentinc/cp-kafka:8.3.1` |
| Broker image ID/digest | `sha256:0ad069035863aa1b090f4d9af47bfd2c08dc32864f3575d7d8579e3155c2586d` |
| Authentication | `PLAIN` on `SASL_SSL`, `kafka:9093`, verified local CA and hostname |
| Payload | Three unique records, supported `plaintext` logs decoding, console `record_json` |
| Tool versions | PowerShell 7.4.21, Docker server 29.8.2, Compose 5.6.0 |

Both `git merge-base --is-ancestor <implementation-sha> HEAD` checks returned
exit **0**. No upstream merge, cherry-pick, history rewrite, or dependency
upgrade was necessary. Kafka capability binding was already present.
The pinned protobuf submodule was restored at
`be5d58470429d0255ffdd49491f0815a3a63d6ef`.

The live configuration is `auth.sasl.credential_source: capability`, with
node-level `capabilities.sasl_credential_provider: credentials`. The suggested
`credentials: capability` spelling is not the field in this source.
Both pipeline configs bind the real
`urn:otel:extension:flat_file_sasl_auth`; neither duplicates inline credentials
in the receiver.

## Executed commands

Commands below were run from this isolated worktree's repository root.
The image build and the initial ten-case run both exited **0**:

```powershell
git submodule update --init proto/opentelemetry-proto
$repo = (Get-Location).Path
$sha = git rev-parse HEAD
docker buildx build --load --progress plain `
  --build-context "otel-arrow=$repo" --platform linux/amd64 `
  --build-arg TARGETPLATFORM=linux/amd64 `
  --build-arg FEATURES=kafka,flat-file-sasl-auth `
  --build-arg "RUSTFLAGS=-C target-cpu=x86-64" `
  --label "org.opencontainers.image.revision=$sha" `
  -f rust\otap-dataflow\examples\kafka\Dockerfile.dataflow `
  -t otap-flat-file-sasl:5750616-cd29e57b4 rust\otap-dataflow

pwsh -NoProfile -File `
  rust\otap-dataflow\examples\kafka\flat-file-sasl\scripts\Test-FlatFileSasl.ps1 `
  -SkipBuild -Image otap-flat-file-sasl:5750616-cd29e57b4
```

Initial run project: `ff-sasl-5750616-4c571fe932`.
Engine provenance was captured at `2026-10-16T22:57:24Z`; its final negative
cases were observed at approximately `23:01:21Z`.

The final harness, including assertions repeated after controlled shutdown,
was then executed through its default Compose build path:

```powershell
pwsh -NoProfile -File `
  rust\otap-dataflow\examples\kafka\flat-file-sasl\scripts\Test-FlatFileSasl.ps1
```

That command exited **0**, built the repeat image above, and passed all ten
cases again. Repeat project: `ff-sasl-5750616-a56aaaf7d4`; engine provenance
was captured at `2026-10-16T23:04:21Z`, and the last authentication rejection
was observed at `23:08:19Z`.

The full Docker argument/exit-code audit, raw sanitized logs, build output,
expected bodies, binary hash, image provenance, results, and cleanup receipts
are retained locally under `.runs\<run-project>`. Credential-property stdin
is deliberately excluded from the command audit.

The committed [evidence snapshot](./evidence/2026-10-16/) contains
the [initial](./evidence/2026-10-16/initial-summary.json) and
[repeat](./evidence/2026-10-16/summary.json) run summaries,
the [final case results](./evidence/2026-10-16/results.json),
[source and binary provenance](./evidence/2026-10-16/provenance.json),
[decoded output and failure excerpts](./evidence/2026-10-16/runtime-excerpts.log),
the [scoped cleanup receipt](./evidence/2026-10-16/cleanup.json), and
[independent resource checks](./evidence/2026-10-16/resource-checks.json).
The [command audit](./evidence/2026-10-16/command-audit.json) groups 396
successful Docker invocations into 27 distinct command/outcome entries;
the receiver process exit codes for acquisition failures are recorded
separately in `results.json`. The [build log](./evidence/2026-10-16/build.log)
records the default Compose build.

## Case checklist

| Case | Outcome | Observable assertion |
| --- | --- | --- |
| Inline username + password file | PASS | Exactly three expected JSON `body` values |
| Username file + password file | PASS | Exactly the same three expected JSON `body` values, fresh group |
| Missing password file | PASS | `password_secret_file` read failure; engine exit 1; zero bodies |
| Invalid UTF-8 password file | PASS | Field-specific UTF-8 diagnostic; engine exit 1; zero bodies |
| CR/LF-only password file | PASS | Password cannot be empty; engine exit 1; zero bodies |
| Missing username file + valid inline username | PASS | `username_file` read failure; engine exit 1; no fallback output |
| Invalid UTF-8 username file + valid inline username | PASS | Field-specific UTF-8 diagnostic; engine exit 1; no fallback output |
| CR/LF-only username file + valid inline username | PASS | Username cannot be empty; engine exit 1; no fallback output |
| Wrong password | PASS | Explicit SASL rejection; zero bodies through controlled shutdown |
| Wrong file username + valid inline username | PASS | Explicit SASL rejection; zero bodies; no inline fallback |

The topic end offset was exactly `flat-file-sasl-logs:0:3`. The body oracle
compares decoded output against all three complete input strings, not a log
substring or partition assignment. Each positive case uses an independent
group with `auto_offset_reset: earliest`.

File failures are subject to the engine's existing recovery policy: a
three-second readiness timeout is retried five times, then the process exits
1, within the harness's 45-second case bound. A broker authentication failure
is asynchronous; the process can continue retrying until the harness stops it.
Its later graceful-shutdown footer is **not** treated as authentication or
delivery success. Passing those negatives requires explicit authentication
errors and no decoded telemetry.

## Additional checks and cleanup

- PowerShell AST parse and direct body-parser, redactor, leak-detector, and
  bounded-process helper checks: **PASS**.
- Deliberately supplying the old `otap-kafka-sasl-tls:latest` image: **rejected
  before broker startup**, with a failed-run summary and successful cleanup.
- Resolved Compose configuration: **PASS**; inherited scripts refer to this
  worktree's existing `sasl-tls` directory, and all certificate mounts use the
  new project's named volume.
- `npx --no-install markdownlint-cli2
  rust\otap-dataflow\examples\kafka\README.md
  rust\otap-dataflow\examples\kafka\flat-file-sasl\README.md
  rust\otap-dataflow\examples\kafka\flat-file-sasl\VALIDATION.md`: **PASS**.
- `py tools\sanitycheck.py`: **baseline failure**, 872 unrelated existing
  Windows CRLF failures after normalizing this change's files; **zero
  changed-file failures**. Unrelated files were left untouched.
- `git diff --cached --check`: **PASS**.

No Rust source changed, so the unrelated full Rust workspace suite and its
previously reported Windows librdkafka/Snappy failure were not rerun.
The actual affected runtime path was built and exercised instead.

Both projects' containers, networks, and certificate volumes were removed;
independent label-filtered Docker queries found none remaining.
Generated credential files and the extracted executable were removed.
The pre-existing `pipeline-operator-local-control-plane` container was left
running, and no shared services were stopped, pruned, or reconfigured.
Only the new validation image tags and normal build cache remain.
