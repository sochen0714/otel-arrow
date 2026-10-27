# Azure Key Vault SASL validation

## Recorded published-SDK run

**Five required live cases passed on 2026-10-27 UTC**, on an approved Azure
Linux/amd64 VM using its existing system-assigned managed identity.
The receiver used real SDK reads and Kafka PLAIN over verified SASL_SSL.

This result belongs to production source
`da8412df9721ecf3c4a365c464ba120131ddb2d8` and frozen validation source
`ced089ecbd8d8977ede6d9b0b170a5db5013d955`, not every later commit.
The [immutable validation package][package] contains the exact source and
images. Its build-time `NOT_RUN` metadata predates the subsequent live run.
The selected [sanitized run evidence](evidence/published-sdk-run.json)
records only source/image identity, bounded results, and cleanup outcomes.

| Check | Recorded result |
| --- | --- |
| Configured credentials | PASS; exactly three expected bodies |
| Missing username | PASS; `username/not_found`, 15 s readiness failure, exit 1, zero records |
| Missing password | PASS; `password/not_found`, 15 s readiness failure, exit 1, zero records |
| 1 ms startup timeout | PASS; exact credentials readiness failure, exit 1, zero records |
| Broker rejection | PASS; explicit SASL rejection, zero records |
| Receiver lifecycle | All stopped; configured and broker-rejection final exits 0 |
| Disclosure and OOM scans | No detected receiver/broker disclosure or receiver OOM |
| Scoped cleanup | Containers, network, and volumes verified removed |
| Command audit | All 198 recorded operations exited 0 |
| Outer wrapper and SSH exits | Both 0 |
| Optional denied-vault authorization | NOT_RUN; no approved denied target supplied |

The packages were registry Secrets `1.1.0-beta.1`, Core `1.2.0-beta.1`, and
Identity `1.1.0-beta.1`. The development image ID was
`sha256:8e4e912b8413d8a87c531bfbe6c6b691374365c53793489fc2808f5456d788c7`;
the extracted executable SHA-256 was
`4f06174009b0dd9d6e17d907cfa9184e83b89ba989a65b60e4c5fe3b9e83dafc`.
Those values matched the built/exported image provenance; tags alone were not
accepted. Sanitized evidence validation passed independently on Linux and
Windows, including the actual outer exit and optional coverage boundary.

## This reusable fork branch

The branch starts from production
`09ffcae624c5e5fa5bc50efb6788ec8b0e0d3697`, a later merge of main into the
same AKV feature. It imports the validated harness, updates the process
documentation, and retains exact-source/SDK/identity approval gates.
It does not change production Rust source, dependencies, or the feature PR.

The recorded live run above has **not** been repeated for this branch's new
source SHA. Following the [README](README.md) builds new, exact-source images
and produces a separately attributable run. Do not retag an old image or cite
the earlier five-case result as new-head acceptance. This task publishes the
process only; it performs no new Azure login, Key Vault reads, image build,
cloud mutation, or live test.

## Reproducible offline checks

The following checks passed for this process change:

| Check | Result |
| --- | --- |
| Python runtime/build-context fixtures, Windows | 31 passed; 2 Linux-only skips |
| Python runtime/build-context/wrapper fixtures, Linux | 33 passed |
| PowerShell provenance and case oracles | 47 passed on Windows and Linux |
| Managed and development Compose structure | Both passed; no services started |
| BuildKit context exclusion, synthetic canaries | PASS; root/nested private files excluded and baseline rules retained |
| Repository sanity and Markdown lint | PASS |

The BuildKit check used a network-free scratch export, not an engine build.
It verified the actual Dockerfile-specific exclusion behavior without real
runtime inputs or an Azure identity.

From this example directory:

```powershell
python -B -m unittest discover -s scripts -p test_akv_runtime.py
pwsh -NoProfile -File scripts/Test-CaseOracles.ps1
```

These fixtures cover input/identity validation, safe scanning, fresh receiver
containers, actual terminal exits, readiness timer races, post-exit log
recapture, and scoped wrapper cleanup. The Python wrapper cases require Linux;
Windows skips those explicitly. None is a mock-SDK live acceptance claim.

From the repository root:

```powershell
python tools/sanitycheck.py
npx markdownlint-cli2 `
  rust/otap-dataflow/examples/kafka/azure-key-vault-sasl/README.md `
  rust/otap-dataflow/examples/kafka/azure-key-vault-sasl/VALIDATION.md
```

Compose configuration can also be resolved with synthetic placeholder input
and no daemon connection. Check both managed and development overrides: no
published ports, no managed-mode login-cache mount, no inherited inline JAAS
override, and no receiver access to broker credential/reference volumes.
Offline success does not imply a green whole-workspace Rust CI run.

[package]: https://github.com/sochen0714/otel-arrow/releases/tag/akv-e2e-validation-ced089e
