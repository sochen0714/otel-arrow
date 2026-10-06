#!/usr/bin/env bash
# Run on the Azure Ubuntu VM. Uses the existing image; never restarts the pipeline.
set -euo pipefail

here="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

case "${1:---help}" in
  --help)
    printf '%s\n' \
      'bash run-probes.sh --check            # metadata/clock checks; NO PROBE RECORDS' \
      'bash run-probes.sh --run [count rate] # default: 1200 probes at 2/s (~10 min)' \
      'Run --run only when the measured benchmark window is ready.' \
      'Use the same VM and topic as the benchmark. No Azure role changes needed.' \
      'Optional environment overrides (defaults shown):' \
      '  KAFKA_BENCH_IMAGE=kafka-syslog-generator:86c927622' \
      '  KAFKA_BENCH_NETWORK=kafka-cloud-bench' \
      '  KAFKA_BENCH_BROKER_CONTAINER=kafka-broker' \
      '  KAFKA_BENCH_CONSUMER_CONTAINER=kafka-consumer' \
      '  KAFKA_BENCH_BROKERS=kafka-broker:9092' \
      '  KAFKA_BENCH_TOPIC=otel-syslog' \
      '  KAFKA_BENCH_ARTIFACTS=$HOME/kafka-bench-artifacts' \
      'See the adjacent README.md for prerequisites and measurement limits.'
    exit 0
    ;;
  --check)
    if (( $# != 1 )); then
      echo '--check does not accept arguments; use --help.' >&2
      exit 2
    fi
    ;;
  --run)
    if (( $# > 3 )); then
      echo '--run accepts only optional count and rate; use --help.' >&2
      exit 2
    fi
    ;;
  *) echo 'Expected --check or --run; use --help.' >&2; exit 2 ;;
esac
if [[ "$EUID" == 0 ]]; then
  echo 'Run as your normal VM user, not sudo bash; individual Docker commands use sudo.' >&2
  exit 2
fi

image="${KAFKA_BENCH_IMAGE:-kafka-syslog-generator:86c927622}"
network="${KAFKA_BENCH_NETWORK:-kafka-cloud-bench}"
broker_container="${KAFKA_BENCH_BROKER_CONTAINER:-kafka-broker}"
consumer_container="${KAFKA_BENCH_CONSUMER_CONTAINER:-kafka-consumer}"
brokers="${KAFKA_BENCH_BROKERS:-kafka-broker:9092}"
topic="${KAFKA_BENCH_TOPIC:-otel-syslog}"
artifacts="${KAFKA_BENCH_ARTIFACTS:-$HOME/kafka-bench-artifacts}"
count="${2:-1200}"
rate="${3:-2}"
if [[ "$1" == "--run" ]]; then
  python3 -c '
import sys
try:
    count, rate = int(sys.argv[1]), float(sys.argv[2])
except ValueError:
    sys.exit("Count must be an integer and rate must be a number")
if not (0 < count <= 100000 and 0 < rate <= 10):
    sys.exit("Use a positive rate <= 10 probes/s and count <= 100000")
' "$count" "$rate"
fi

clock="$(timedatectl show --property=NTPSynchronized --value)"
if [[ "$clock" != "yes" ]]; then
  echo 'VM clock is not reported synchronized; do not measure cross-system latency yet.' >&2
  exit 1
fi
sudo docker image inspect "$image" --format '{{.Id}}'
for container in "$broker_container" "$consumer_container"; do
  state="$(sudo docker inspect --format '{{.State.Running}}' "$container")"
  if [[ "$state" != "true" ]]; then
    echo "$container is not running; refusing to start probes." >&2
    exit 1
  fi
done
sudo docker run --rm --pull never --network "$network" \
  --mount "type=bind,source=$here,target=/probe,readonly" \
  --entrypoint python "$image" /probe/probe.py check \
  --brokers "$brokers" --topic "$topic"
if [[ "$1" == "--check" ]]; then
  echo 'Probe preflight passed. No records sent and no existing containers changed.'
  exit 0
fi

run_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
output="$artifacts/latency-$run_id"
mkdir -p "$output"
output="$(cd -- "$output" && pwd)"
printf '%s\n' "$clock" > "$output/clock-before.txt"
date -u --iso-8601=ns > "$output/host-started-at.txt"
sudo docker image inspect "$image" --format '{{.Id}}' > "$output/probe-image.txt"
sudo docker inspect --format '{{.Name}} {{.Image}}' "$broker_container" "$consumer_container" \
  > "$output/pipeline-images.txt"
printf 'Probe run ID: %s\nEvidence: %s\n' "$run_id" "$output"

code=0
sudo docker run --rm --pull never --name "kafka-la-probe-$run_id" \
  --network "$network" --stop-timeout 45 --user "$(id -u):$(id -g)" \
  --mount "type=bind,source=$here,target=/probe,readonly" \
  --mount "type=bind,source=$output,target=/evidence" \
  --entrypoint python "$image" /probe/probe.py run \
  --brokers "$brokers" --topic "$topic" \
  --run-id "$run_id" --output /evidence --count "$count" --rate "$rate" \
  2>&1 | tee "$output/producer.log" || code=$?
printf '%s\n' "$code" > "$output/producer-exit-code.txt"

timedatectl show --property=NTPSynchronized --value > "$output/clock-after.txt"
date -u --iso-8601=ns > "$output/host-finished-at.txt"
if [[ ! -s "$output/producer-summary.json" ]]; then
  echo "Producer did not write its summary. Inspect $output/producer.log. No valid result." >&2
  exit 1
fi
report_code=0
python3 "$here/probe.py" report --output "$output" || report_code=$?
if [[ "$code" != 0 || "$report_code" != 0 ]]; then
  echo "Probe run invalid; preserved evidence in $output." >&2
  exit 1
fi
printf '\nRun this file in your Log Analytics workspace -> Logs, using the query time range:\n%s/latency.kql\n' "$output"
echo 'Do not interpret HTTP/broker success as LA ingestion. Requery until coverage is complete.'
