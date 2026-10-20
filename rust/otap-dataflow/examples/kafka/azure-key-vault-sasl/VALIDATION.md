# AKV receiver validation status

**Status: harness preparation only. Live receiver E2E is pending.**

No Azure login was initiated, no secrets/tokens were retrieved, and no
Rust/Docker image was built for this preparation. No user Azure resources
or upstream pull requests were changed.

## Source boundaries

The shared broker/certificate/image recipe comes from the reproducible
flat-file harness at
`sochen0714/otel-arrow@c5cc48b3888785c84ded65ac2c272c5b3f7a781d`.
Receiver capability integration is already upstream in
[#4304](https://github.com/open-telemetry/otel-arrow/pull/4304), commit
`298846b57cb7e4d09479ef206572c92604665a4a`.

Final production source integration is deferred until the parent explicitly
hands off its exact commits. The expected official SDK revision is
`e1f93542648359690c640935f271f49a66dccc25`, not the removed vendored SDK
or a newer beta. The accepted temporary upstream default crypto/runtime
features belong to that parent's integration evaluation, not a claim of
crypto-provider neutrality here.

The parent reported affected/regression checks, but its initial full
workspace run exhausted the Windows paging file and was retrying with bounded
concurrency. That is **not** a green whole-workspace result. This harness
neither repeats nor expands those parent claims.

## Lightweight local checks

From the repository root, these checks require no Docker engine, Azure
identity, secret reads, or Rust compilation:

```powershell
$scripts = 'rust\otap-dataflow\examples\kafka\azure-key-vault-sasl\scripts'
py -3 -B -m unittest discover -s $scripts -p test_akv_runtime.py -v
$tokens = $null
$errors = $null
$null = [System.Management.Automation.Language.Parser]::ParseFile(
  (Join-Path (Get-Location) "$scripts\Test-AzureKeyVaultSasl.ps1"),
  [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw ($errors -join "`n") }
py -3 tools\sanitycheck.py
npx markdownlint-cli2 `
  rust\otap-dataflow\examples\kafka\azure-key-vault-sasl\README.md `
  rust\otap-dataflow\examples\kafka\azure-key-vault-sasl\VALIDATION.md
```

The driver must also pass `docker compose config --format json` with
placeholder inputs without contacting the daemon. Confirm no published ports,
unchanged verified TLS, external dedicated login volume, and no inherited
inline PLAIN JAAS override. These are structural checks, not a live Kafka
or Azure acceptance result. Unit tests of the renderer/scanner are not an SDK
mock and cannot substitute for E2E.

Preparation results: all 17 renderer/scanner tests, PowerShell parsing, and the
fail-closed execution gate passed. Daemon-free Compose resolution passed with
synthetic placeholder paths, no published ports, the dedicated external login
volume, inherited certificate tooling, no inline JAAS override, and no access
from the receiver to the broker/reference credential volumes. Markdown lint
and changed-file sanity (eight Markdown/YAML/Python files) passed.
The full `py -3 tools\sanitycheck.py` command
encountered pre-existing CRLF checkout failures across unrelated tracked
Markdown/YAML/Python files on Windows. Changed-file sanity is checked
separately after LF normalization; unrelated files are not rewritten and a
green repository-wide sanity result is not claimed.

## Live acceptance still required

| Acceptance | Current result |
| --- | --- |
| Final SDK/source handoff integrated and clean source committed | Pending parent handoff |
| User sign-in in dedicated local Docker volume | Pending user action |
| Existing vault access model and Docker network reachability | Unverified |
| Actual source build and executable/image provenance | NOT_RUN |
| Both real SDK reads, real capability binding, exact three output bodies | NOT_RUN |
| Username/password missing-secret failures, no partial output | NOT_RUN |
| Bounded startup timeout and safe diagnostic classifications | NOT_RUN |
| Authorization failure against an explicitly approved denied target | NOT_RUN; optional target not supplied |
| Broker-only wrong-password rejection, no output | NOT_RUN |
| Receiver/broker credential-disclosure scans and scoped cleanup | NOT_RUN |

The [README](README.md) contains exact future sign-in/run commands, runtime
input shape, fixture restrictions, safe negative-case strategy, evidence
handling, and cleanup scope. Do not label the feature E2E-validated or publish
local resource details until an actual run meets these oracles.
