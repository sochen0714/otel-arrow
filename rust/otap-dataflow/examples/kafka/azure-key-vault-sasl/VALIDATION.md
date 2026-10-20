# AKV receiver validation status

**Status: production source integrated; live receiver E2E is pending.**

No Azure login was initiated and no secrets/tokens were retrieved. Source
checks and image-build preparation are permitted after the parent's handoff;
they are not live acceptance. No user Azure resources or upstream pull
requests were changed by this validation session.

## Source boundaries

The shared broker/certificate/image recipe comes from the reproducible
flat-file harness at
`sochen0714/otel-arrow@c5cc48b3888785c84ded65ac2c272c5b3f7a781d`.
Receiver capability integration is already upstream in
[#4304](https://github.com/open-telemetry/otel-arrow/pull/4304), commit
`298846b57cb7e4d09479ef206572c92604665a4a`.

Production source `b455560661d4610e6d10237000e3c0f6628663f1` was handed off.
Only its net feature diff from `4242bc8ab80d0f49e33ce7ce3c1763eb8a7bab8d`
is integrated onto validation baseline
`8f315f5b9b8833e48378c7d93ddf17ca6ae07181` (newer main
`cf090f47d9145c44ffe1199fa168eb534fc7d750` plus the harness).
The integrated validation revision is recorded by Git and future build
provenance, not mislabeled as the production tree.
The net source/runtime integration commit is
`82083de42e92eb63a62518edfb6bfba80cb9c82d`; build provenance records the exact
final checkout SHA, including any subsequent documentation/provenance commit.

Integration retained all 1,150 existing package name/version pairs and all
55 newer-main component IDs, deduplicating the old OAuth2 inventory entry and
adding only AKV. It preserves the existing engine lifecycle-readiness helper
and adds the production tests' raw-probe helpers alongside it; no receiver
implementation was changed. Component inventory verification and targeted
Rust formatting passed. Synthetic secret-name fixtures use unrelated names
in this validation branch; no private runtime identifiers are embedded.

SDK Core/Identity/Secrets and matching typespec packages use official revision
`e1f93542648359690c640935f271f49a66dccc25`, not vendored files or a newer beta.
The accepted temporary upstream default crypto/runtime features belong to
the parent's integration evaluation, not a claim of provider neutrality here.

The parent's full retry resolved paging exhaustion but stopped on the same
pre-existing Windows native Snappy failure (1,590 passed, one failed, six
ignored), after structure/inventory/format/workspace Clippy/no_std passed.
That is **not** a green whole-workspace result, nor a test result for this
newer-main integration.

The bounded integration check
`cargo check --locked -p otel-arrow-dfe-contrib-extensions --lib --tests
--no-default-features --features azure-key-vault-sasl-auth,crypto-ring`
compiled the newer-main engine/test helpers but failed in the Windows Cargo
Git checkout of the official SDK: its tracked authorizer symlink was
materialized as a literal relative-path file. No SDK/cache patch or vendoring
was applied. This is a Windows checkout prerequisite failure, not evidence of
an API regression or a successful integrated crate check. Linux image build
verification remains separate.

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
unchanged verified TLS, no login/cache mount in managed mode, an external
dedicated login volume only in development mode, and no inherited inline
PLAIN JAAS override. These are structural checks, not a live Kafka
or Azure acceptance result. Unit tests of the renderer/scanner are not an SDK
mock and cannot substitute for E2E.

Preparation results: all 23 renderer/scanner/identity-selection tests,
PowerShell parsing, and the fail-closed local managed-identity execution gate
passed. Daemon-free Compose resolution passed for both modes: no published
ports, no managed-mode login/cache mount, optional development external
login only, and no receiver access to broker/reference credential volumes.
Markdown lint and changed-file sanity are checked after each update.
The full `py -3 tools\sanitycheck.py` command
encountered pre-existing CRLF checkout failures across unrelated tracked
Markdown/YAML/Python files on Windows. Changed-file sanity is checked
separately after LF normalization; unrelated files are not rewritten and a
green repository-wide sanity result is not claimed.

## Live acceptance still required

| Acceptance | Current result |
| --- | --- |
| Final SDK/source handoff integrated and clean source committed | Integrated net diff; see Git revision and build provenance |
| Approved VM access and existing identity selection | Pending private target/access/identity handoff; no remote execution authorized |
| Optional local development sign-in | Prerequisite-blocked by access policy; not a feature/data-plane failure |
| Existing vault access model and Docker network reachability | Unverified |
| Actual source build and executable/image provenance | Separate build preparation; requires successful build artifact |
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
