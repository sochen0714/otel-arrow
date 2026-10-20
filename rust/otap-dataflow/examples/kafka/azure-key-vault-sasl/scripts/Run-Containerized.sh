#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 4 || "$1" != "--execution-approved" || "$2" != "--managed-identity-vm-approved" ]]; then
    printf '%s\n' 'Execution is gated: both parent runtime/identity approvals are required.' >&2
    printf '%s\n' 'Usage: bash Run-Containerized.sh --execution-approved --managed-identity-vm-approved <production-sha> <ignored-runtime-json>' >&2
    exit 2
fi
[[ "$(uname -s)" == "Linux" ]] || { printf '%s\n' 'Run only on the approved Linux Azure VM.' >&2; exit 2; }
[[ "$3" =~ ^[0-9a-f]{40}$ ]] || { printf '%s\n' 'Invalid production revision.' >&2; exit 2; }

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
example="$(cd -- "$script_dir/.." && pwd -P)"
repo="$(git -C "$example" rev-parse --show-toplevel)"
repo="$(cd -- "$repo" && pwd -P)"
source="$(git -C "$repo" rev-parse HEAD)"
[[ "$source" =~ ^[0-9a-f]{40}$ ]] || { printf '%s\n' 'Cannot resolve validation revision.' >&2; exit 2; }
input="$(realpath -e -- "$4")"
[[ "$input" == "$example/.local/"* && -f "$input" ]] || {
    printf '%s\n' 'Runtime input must be a file inside this example''s ignored .local directory.' >&2
    exit 2
}
git -C "$repo" check-ignore --quiet -- "$input" || {
    printf '%s\n' 'Runtime input is not ignored by Git.' >&2
    exit 2
}
runner="otap-akv-e2e-runner:${source:0:12}"
collector="otap-akv-e2e-development:${source:0:12}"
nonce="$(tr -d '-' </proc/sys/kernel/random/uuid)"
project="akv-sasl-${nonce:0:12}"
container="$project-driver"

docker_bounded() {
    timeout --signal=TERM --kill-after=5s 30s sudo -n docker "$@"
}

project_ids() {
    if [[ "$1" == "container" ]]; then
        docker_bounded container ls --all --quiet --filter "label=com.docker.compose.project=$project"
    else
        docker_bounded "$1" ls --quiet --filter "label=com.docker.compose.project=$project"
    fi
}

cleanup() {
    local status=$? failed=0 ids kind id owner
    trap - EXIT INT TERM
    set +e
    if docker_bounded container inspect "$container" >/dev/null 2>&1; then
        owner="$(docker_bounded inspect --format '{{index .Config.Labels "org.otel.akv.driver"}}' "$container")"
        if [[ "$owner" == "$project" ]]; then
            docker_bounded rm -f "$container" >/dev/null || failed=1
        else
            printf '%s\n' 'Refusing to remove a driver container without this run''s ownership label.' >&2
            failed=1
        fi
    fi
    for kind in container network volume; do
        if ! ids="$(project_ids "$kind")"; then
            failed=1
            continue
        fi
        if [[ -n "$ids" ]]; then
            # Only IDs returned by this unique project's label filter may be removed.
            while IFS= read -r id; do
                if [[ "$kind" == "container" ]]; then
                    docker_bounded rm -f "$id" >/dev/null || failed=1
                else
                    docker_bounded "$kind" rm "$id" >/dev/null || failed=1
                fi
            done <<<"$ids"
        fi
        if ! ids="$(project_ids "$kind")" || [[ -n "$ids" ]]; then
            failed=1
        fi
    done
    if (( failed )); then
        printf '%s\n' 'Scoped cleanup failed; do not report this run as successful.' >&2
        status=1
    fi
    exit "$status"
}
for kind in container network volume; do
    if ! ids="$(project_ids "$kind")"; then
        printf '%s\n' 'Cannot verify an unused project namespace; refusing execution.' >&2
        exit 1
    fi
    if [[ -n "$ids" ]]; then
        printf '%s\n' 'Project namespace already exists; refusing to use or clean it.' >&2
        exit 2
    fi
done
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

runner_source="$(docker_bounded image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$runner")"
[[ "$runner_source" == "$source" ]] || { printf '%s\n' 'Runner image source mismatch.' >&2; exit 2; }
runner_id="$(docker_bounded image inspect --format '{{.Id}}' "$runner")"
printf 'ROOT-EQUIVALENT Docker socket access; project=%s source=%s runner=%s\n' "$project" "$source" "$runner_id"

# The same absolute path is essential: bind sources are resolved by the host daemon.
# No Azure cache, home directory, IMDS network, or host binaries enter this driver.
timeout --signal=INT --kill-after=45s 30m sudo -n docker run --rm --pull never \
    --name "$container" --label "org.otel.akv.driver=$project" \
    --network none --memory 768m --cpus 1 --pids-limit 256 \
    --mount "type=bind,source=$repo,target=$repo" \
    --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock \
    --workdir "$repo" \
    --env GIT_CONFIG_COUNT=1 \
    --env GIT_CONFIG_KEY_0=safe.directory \
    --env "GIT_CONFIG_VALUE_0=$repo" \
    --env "AKV_DRIVER_REPO=$repo" --env "AKV_DRIVER_INPUT=$input" \
    --env "AKV_DRIVER_SOURCE=$source" --env "AKV_DRIVER_PRODUCTION=$3" \
    --env "AKV_DRIVER_PROJECT=$project" --env "AKV_DRIVER_IMAGE=$collector" \
    "$runner" -Command '
        $ErrorActionPreference = "Stop"
        & (Join-Path $env:AKV_DRIVER_REPO "rust" "otap-dataflow" "examples" "kafka" "azure-key-vault-sasl" "scripts" "Test-AzureKeyVaultSasl.ps1") `
            -SourceCommit $env:AKV_DRIVER_SOURCE -ProductionCommit $env:AKV_DRIVER_PRODUCTION `
            -RuntimeInput $env:AKV_DRIVER_INPUT -Image $env:AKV_DRIVER_IMAGE `
            -ProjectName $env:AKV_DRIVER_PROJECT -ExecutionApproved -ManagedIdentityVmApproved -SkipBuild
    '
