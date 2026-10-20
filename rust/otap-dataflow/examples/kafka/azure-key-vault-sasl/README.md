# Azure Key Vault SASL: receiver E2E harness

**Integrated, not live-validated.** The parent handed off the final production
source and released its heavy-build hold. Source/image builds may proceed;
live AKV cases are prerequisite-blocked pending an approved identity route.
This branch contains the net feature integration for validation, not
production receiver implementation changes.
See [VALIDATION.md](VALIDATION.md) for the current evidence boundary.

This reuses the flat-file SASL harness at
`sochen0714/otel-arrow@c5cc48b3888785c84ded65ac2c272c5b3f7a781d`:
the shared [`Dockerfile.dataflow`](../Dockerfile.dataflow), SASL/TLS broker
Compose service, and certificate script. Only those shared files were imported,
not the fork's unrelated examples or historical evidence. The shared build
now has a two-job default to limit compiler concurrency.

## Flow and scope

The real `df_engine` is built from the exact integrated checkout with
`kafka,azure-key-vault-sasl-auth`. A development-only Debian image adds Azure
CLI to that unchanged executable for private broker provisioning. Debian
Trixie matches the shared engine image's glibc generation; copying this
executable into an Azure Linux 3 image
would risk a glibc mismatch. The official Azure CLI image is used separately
for interactive login. CLI is never added to a production image.

The primary runtime path is approved VM managed identity ->
real Key Vault SDK reads ->
`urn:otel:extension:azure_key_vault_sasl_auth` -> receiver capability binding ->
real Kafka PLAIN over verified SASL_SSL -> plaintext log decoding -> console
`record_json`. Both secrets must be acquired before provider readiness. There
is no inline/file credential fallback, polling, rotation, or mocked SDK.

`receiver.yaml` is a template. The helper renders an ignored runtime file;
secret values are never substituted into that pipeline configuration.
The actual receiver field and binding are:

```yaml
capabilities:
  sasl_credential_provider: credentials
config:
  auth:
    sasl:
      mechanism: PLAIN
      credential_source: capability
```

The spelling `credentials: capability` is not valid for this receiver.

## Execution prerequisites

The handed-off production commit is
`b455560661d4610e6d10237000e3c0f6628663f1`, with unmodified official Azure SDK
revision `e1f93542648359690c640935f271f49a66dccc25`. Its net diff from production
base `4242bc8ab80d0f49e33ce7ce3c1763eb8a7bab8d` is integrated onto this branch's
newer-main baseline, retaining its package versions and component IDs.
The resulting validation source SHA is intentionally different from the
production commit. Do not use the old vendored draft, beta2, patched/fake
binaries, or a wholesale merge of the fork.

Required tools: PowerShell 7+, Git, Docker Linux with Compose v2/buildx, and the
pinned protobuf submodule. The prepared image targets Linux/amd64:

```powershell
git submodule update --init proto/opentelemetry-proto
```

The parent's bounded workspace check has ended.
The first actual engine build can take tens of minutes. Build jobs default
to two, but adequate Docker disk, memory, and paging space are still required.
No whole-workspace success is implied by harness preparation.

The selected runtime identity must already have read access to both selected
secrets and network reachability from Docker. Use **Key Vault Secrets User**
for an existing RBAC vault, or **Secrets Get** for an existing legacy access
policy vault. Do not switch permission models or alter resources, RBAC,
policies, secrets, firewall, subscriptions, or app registrations for this test.
The vault's actual permission and network model has not been verified.
The attempted local sign-in was blocked by an identity access policy, not a
Key Vault SDK/data-plane failure. Do not retry alternate login flows, export
token caches, or weaken policy. An identity-admin-approved runtime/identity
route is required before any token/secret preflight or live test. The user
selected an existing Azure VM with managed identity; target/access and its
existing identity selection must be handed off privately before execution.

### Approved VM managed identity

The VM recipe requires an approved Linux/amd64 VM with Docker and PowerShell
7. Do not connect, install tools remotely, or probe IMDS until its target,
access channel, and execution scope are explicitly released by the parent.
Keep VM, subscription, identity, account, and vault identifiers in ignored
runtime inputs/private handoff, never committed documentation or evidence.

Confirm the VM's **existing** identity: explicitly select
`{"method":"managed_identity"}` for its system-assigned identity, or also set
`"client_id":"<existing-user-assigned-client-id>"` for the selected existing
user-assigned identity. Do not guess which exists or enable/change an identity.
The runtime file must supply the identity block; the harness does not silently
default it from a developer environment.

Base Compose mounts no CLI/user login cache. On that approved VM only, the
private bootstrap logs into CLI with `--identity --allow-no-subscriptions`
and output disabled, then provisions the private broker. Its cache is
container-local and deleted with that container. The receiver independently
uses the real SDK `ManagedIdentityCredential`, not CLI tokens, a forwarded
user cache, or an identity proxy. Both use the same explicit client-ID selection.
IMDS is excluded from proxy routing; actual container reachability must be
approved. Do not modify VM identity, firewall, routing, or IAM to make it work.

### Optional development mode: prerequisite-blocked

Host Windows `az login` does **not** authenticate the collector container.
The parent has prepared the dedicated login volume, but local access is
policy-blocked. Do not recreate/delete it. The commands below document the
local recipe only; use them only if that exact flow is subsequently approved.
The user, not the harness, performs interactive sign-in:

```powershell
docker volume inspect otap-akv-e2e-login --format '{{.Name}}'
docker run --rm -it `
  --mount type=volume,source=otap-akv-e2e-login,target=/root/.azure `
  --env AZURE_CONFIG_DIR=/root/.azure `
  --env AZURE_CORE_COLLECT_TELEMETRY=no `
  mcr.microsoft.com/azure-cli@sha256:933eb8dcb81aecb6f77e03c5b8660f0a1dfa9e1d16525763a27f86ff3b83a044 `
  az login --use-device-code
```

The user follows the CLI's interactive instructions privately. Do not paste
codes or tokens into chat, logs, configuration, or evidence. Do not mount/copy
the host's general `.azure` directory, bake tokens into images, or run
`az account get-access-token` as a preflight. Development mode never initiates
interactive login.
It preserves this explicitly external login volume during cleanup. The Azure
CLI cache is sensitive and shared read/write only with the test containers
that need token refresh; use a trusted local Docker environment.

That bootstrap digest was inspected as Azure CLI 2.91.0. The development
image independently installs CLI 2.91.0, avoiding assumptions about the mutable
`azurelinux3.0` tag or cross-version CLI cache compatibility.

### Ignored runtime inputs

In this example's `.local` directory, create `runtime.json` using the
privately supplied vault root and secret names. Keep actual resource details,
tenant/client IDs, and any local configuration out of public git and evidence.
There are no secret values in this file:

```json
{
  "vault_url": "https://<vault-name>.vault.azure.net",
  "username_secret": {
    "name": "<username-secret-name>"
  },
  "password_secret": {
    "name": "<password-secret-name>"
  },
  "identity": {
    "method": "managed_identity"
  }
}
```

Replace every placeholder. This harness accepts public Azure vault roots
only, not `/secrets/...` URLs. Each reference independently accepts an
optional `"version": "<exact-32-hex-version>"`; omitted versions mean latest
at startup. The broker bootstrap and receiver use the same references.
Do not run while another actor changes the selected latest versions: an
immutable version pin is the reproducible alternative.
Use the VM identity block confirmed by its owner, including an existing
user-assigned client ID only when selected. Optional approved local development
instead requires `"identity":{"method":"development"}` and the separate
`compose.development.yaml` login-volume override selected by the driver.

For authorization coverage only, an owner can optionally supply
`"authorization_vault_url": "https://<known-denied-vault>.vault.azure.net"`.
Use an existing target they explicitly authorize testing and know this
identity cannot read. No resource discovery, permission mutation, alternate
identity guessing, or secret writes are performed. Without that input,
authorization is explicitly `NOT_RUN`, not passed. A 404 or network error
does not satisfy the required `authorization` category.

The local broker fixture requires an alphanumeric/underscore username other
than its synthetic admin `plain`, and a password without control characters.
Spaces, quotes, and backslashes in supported passwords are preserved and
JAAS-escaped, never trimmed. An unsupported value fails safely; do not change
the user's secrets to accommodate this fixture. Report the fixture limitation
and obtain a material test-strategy decision instead.

### Future execution command

Only on the approved VM, after its access/identity handoff, integration commit,
and explicit execution release:

```powershell
Set-Location (Join-Path 'rust' 'otap-dataflow' 'examples' 'kafka' 'azure-key-vault-sasl')
$source = (git rev-parse HEAD).Trim()
$production = 'b455560661d4610e6d10237000e3c0f6628663f1'
git fetch --no-tags https://github.com/sochen0714/otel-arrow.git $production
pwsh -NoProfile -File (Join-Path 'scripts' 'Test-AzureKeyVaultSasl.ps1') `
  -SourceCommit $source -ProductionCommit $production `
  -RuntimeInput (Join-Path '.local' 'runtime.json') `
  -IdentityMode managed_identity -ManagedIdentityVmApproved -ExecutionApproved
```

`-ExecutionApproved` explicitly acknowledges the execution gates; it is not
permission to change Azure. The driver requires clean tracked source and the
exact `HEAD`, verifies that the supplied production commit is an ancestor,
or is identified by a `Production-Source-Commit` integration-commit trailer,
checks the SDK revision, and records source/image/executable provenance.
There is no execution permission implied by the existence of this script.
The fetch retains the exact production object for provenance without merging
its history into the validation source.
The managed-identity driver refuses to run on the local Windows host or without
the explicit approved-VM flag, without probing IMDS. If local development is
later explicitly approved, use `-IdentityMode development` without the VM flag;
do not copy its login volume to the VM.

`-SkipBuild -Image <previously-built-development-image>` reuses only an image
with matching source, production revision, and feature labels. This is a
provenance check, not protection against malicious image relabeling: only use
images built from this recipe. The extracted executable SHA-256 and actual
image ID are recorded; a tag alone is not evidence.

## Oracles and safe negative cases

The producer uses the broker's public synthetic admin to publish exactly three
unique records to one partition. Authenticated TLS offset inspection confirms
the input count, but **only the exact three console-exported bodies** pass
delivery. Each receiver case uses a new consumer group with earliest offsets.

| Case | Required observable result |
| --- | --- |
| Configured latest/pinned references | Exactly the three input bodies at the real console exporter |
| Random absent username reference | Nonzero startup exit; stage `username`, category `not_found`; no records |
| Random absent password reference | Nonzero startup exit; stage `password`, category `not_found`; no records |
| 1 ms startup timeout | Nonzero startup exit; category `timeout`; no records |
| Explicitly supplied denied vault | Nonzero startup exit; category `authorization`; no records, or `NOT_RUN` without a target |
| Broker-only wrong password | Explicit Kafka SASL rejection and a further no-delivery observation window |

Missing-reference tests only read a run-unique, overwhelmingly unlikely name;
they never create/delete a secret. Password failure preserves the configured
valid username reference, exercising failure after a possible first read
without accepting partial telemetry. No fabricated readiness log is used:
production readiness is an internal signal, not a public AKV success marker.
Nonzero exit plus the precise bounded acquisition error and zero telemetry
are the accessible startup-failure oracle, not direct instrumentation of that
internal signal.

The provider's diagnostic is `Azure Key Vault {stage} acquisition failed
({kind})`. The scanner recognizes only the documented stage/category
allowlists, not a generic process failure or HTTP status alone.

For broker setup, a private helper container reads the same pair using Azure
CLI, configures only the disposable broker's private JAAS volume, and holds a
private reference for leak detection. The receiver cannot mount either that
reference or the broker-auth volume: its only credential source is the real
AKV extension. Wrong-password coverage changes only the local broker's expected
password to a random unequal value, then restarts that broker. It never
modifies the vault or changes production code.

Raw receiver/broker output stays transient in trusted Docker/host memory.
A private scanner checks exact credential substrings and JSON/URL-escaped
forms, returning only bounded categories, counts, and booleans. Unknown bodies,
raw SDK bodies, resource identifiers, CLI errors, and tokens are never saved or
echoed as evidence. Conservative substring scanning can fail on coincidental
matches for short/common credentials; investigate privately rather than
weakening the oracle or publishing raw logs. This is not proof against every
possible transformed disclosure. Docker retains bounded raw container logs
until scoped cleanup; `.local`, transient `.runs` files, and the login cache
also require a trusted local host.

## Bounds, evidence, and cleanup

Every run uses a unique `akv-sasl-*` Compose project, no published host ports,
private network, and project-scoped certificate, broker-auth, credential
reference, and Kafka data volumes. Kafka is capped at 1.5 GiB with a 512 MiB
heap; receiver/helper/certgen are capped at 512 MiB each. Services use one CPU.
This test starts no web UI and does not stop any other stack.

Case observation defaults to 90 seconds (`-CaseTimeoutSeconds`, 30-180).
Setup, every Docker subprocess, stdin writes, image builds, broker health,
shutdown, and cleanup have separate explicit bounds. Final log scans and
shutdown can extend a case past its observation deadline. Provider startup
normally has a 15-second bound, or 1 ms for the intentional timeout case.
No case is silently skipped or treated as success on a process/OOM failure.

Ignored `.runs\<project>` contains sanitized JSON only: exact source and
SDK revisions, actual image IDs, extracted executable SHA-256, CLI version,
expected fixture bodies, per-case observations, optional-coverage status,
operation exit codes, and the final test/cleanup result. No raw logs, build
logs, secret values, real vault URLs/names, or rendered runtime configs are
retained as evidence. Failed/partial runs have `TestsSucceeded: false`.

`finally` removes only this project's containers/network/volumes, deletes the
two known generated runtime files, and restores the driver's environment.
It verifies that no project-labeled containers/networks/volumes remain and
that the optional development login volume, when selected, still exists before
marking cleanup successful. Managed mode neither mounts nor inspects that
external volume. `.local/runtime.json`, reusable images, normal build cache,
and any separate development login volume remain. Never use global reset/prune
or stop shared services.
If cleanup fails or the host is interrupted, treat the run as failed and remove
only the exact project reported in its `summary.json`; its private Docker
volumes and container logs may still contain credentials.

This is development-only validation with no upstream PR. The imported feature
retains its production changelog entry; the harness itself needs no release
note. Live tests and full workspace checks remain deferred, not inferred green
from bounded source checks or image builds.
