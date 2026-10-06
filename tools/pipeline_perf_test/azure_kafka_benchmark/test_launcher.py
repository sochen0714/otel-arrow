"""Exercise the actual Bash launcher with Docker and clock access mocked out."""

import os
from pathlib import Path
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest


SCRIPT = Path(__file__).with_name("run-probes.sh").resolve()
HARNESS = r"""
set -euo pipefail
script="$1"
shift
if command -v cygpath >/dev/null 2>&1; then
  script="$(cygpath -u "$script")"
  TEST_PYTHON="$(cygpath -u "$TEST_PYTHON")"
  KAFKA_BENCH_ARTIFACTS="$(cygpath -u "$KAFKA_BENCH_ARTIFACTS")"
fi
timedatectl() {
  printf 'MOCK clock\n' >&2
  printf '%s\n' "${MOCK_CLOCK:-yes}"
}
python3() {
  "$TEST_PYTHON" "$@"
}
sudo() {
  printf 'MOCK sudo' >&2
  printf ' <%s>' "$@" >&2
  printf '\n' >&2
  case "$*" in
    'docker image inspect '*) printf 'mock-probe-image\n' ;;
    'docker inspect --format {{.State.Running}} '*)
      printf '%s\n' "${MOCK_RUNNING:-true}"
      ;;
    'docker inspect --format {{.Name}} {{.Image}} '*)
      printf 'mock-pipeline-image\n'
      ;;
    'docker run '*)
      if [[ "$*" == *' /probe/probe.py check '* ]]; then
        return "${MOCK_CHECK_EXIT:-0}"
      elif [[ "$*" == *' /probe/probe.py run '* ]]; then
        echo 'MOCK producer failure; no records sent' >&2
        return 42
      else
        echo 'Unexpected Docker invocation blocked by test harness' >&2
        return 97
      fi
      ;;
    *) echo 'Unexpected sudo invocation blocked by test harness' >&2; return 98 ;;
  esac
}
# Also block accidental direct Docker use instead of the expected sudo wrapper.
docker() { echo 'Direct Docker invocation blocked by test harness' >&2; return 99; }
source "$script" "$@"
"""


class LauncherTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bash = os.environ.get("BASH_EXE") or shutil.which("bash")
        if not cls.bash:
            raise unittest.SkipTest("Bash is required; set BASH_EXE if it is not on PATH")
        result = subprocess.run(
            [cls.bash, "--noprofile", "--norc", "-c", 'printf "%s" "$EUID"'],
            capture_output=True, text=True, check=True, timeout=10,
        )
        if result.stdout == "0":
            raise unittest.SkipTest("Launcher tests require a non-root user")

    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.output = Path(temporary.name) / "evidence"

    def launch(self, *arguments, **overrides):
        env = {
            key: value for key, value in os.environ.items()
            if not key.startswith(("KAFKA_BENCH_", "MOCK_", "BASH_FUNC_"))
            and key not in ("BASH_ENV", "ENV")
        }
        env.update(
            TEST_PYTHON=sys.executable,
            KAFKA_BENCH_ARTIFACTS=str(self.output),
        )
        env.update(overrides)
        return subprocess.run(
            [self.bash, "--noprofile", "--norc", "-c", HARNESS,
             "launcher-test", str(SCRIPT), *arguments],
            env=env, cwd=self.output.parent,
            capture_output=True, text=True, timeout=30,
        )

    # Scenario: The launcher is invoked with no arguments or explicit help.
    # Guarantees: Neither mode accesses Docker, checks the clock, or creates run evidence.
    def test_default_and_help_are_inert(self):
        for arguments in ((), ("--help",)):
            with self.subTest(arguments=arguments):
                result = self.launch(*arguments)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("--run [count rate]", result.stdout)
                self.assertNotIn("MOCK", result.stderr)
                self.assertFalse(self.output.exists())

    # Scenario: A typo or extra arguments might otherwise hide an unintended run request.
    # Guarantees: Invalid invocations fail before contacting Docker or the host clock.
    def test_invalid_invocations_are_rejected(self):
        for arguments in (("--typo",), ("--check", "--run"), ("--run", "20", "2", "extra")):
            with self.subTest(arguments=arguments):
                result = self.launch(*arguments)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertNotIn("MOCK", result.stderr)
                self.assertFalse(self.output.exists())

    # Scenario: Count or rate is invalid, unbounded, or non-finite.
    # Guarantees: Input validation rejects it before any metadata access or producer start.
    def test_invalid_count_and_rate_are_rejected(self):
        for count, rate in (
            ("0", "2"), ("100001", "2"), ("1.5", "2"), ("x", "2"),
            ("20", "0"), ("20", "11"), ("20", "nan"), ("20", "inf"),
        ):
            with self.subTest(count=count, rate=rate):
                result = self.launch("--run", count, rate)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("MOCK", result.stderr)
                self.assertFalse(self.output.exists())

    # Scenario: Preflight runs from an unrelated directory with custom pipeline settings.
    # Guarantees: Overrides reach only a metadata check, without a producer or evidence writes.
    def test_preflight_is_metadata_only_with_overrides(self):
        result = self.launch(
            "--check",
            KAFKA_BENCH_IMAGE="test-generator:local",
            KAFKA_BENCH_NETWORK="test-network",
            KAFKA_BENCH_BROKER_CONTAINER="test-broker",
            KAFKA_BENCH_CONSUMER_CONTAINER="test-consumer",
            KAFKA_BENCH_BROKERS="test-broker:9092",
            KAFKA_BENCH_TOPIC="test-topic",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("No records sent", result.stdout)
        for value in (
            "<test-generator:local>", "<test-network>", "<test-broker>",
            "<test-consumer>", "<--brokers> <test-broker:9092>",
            "<--topic> <test-topic>", "<--rm>", "<--pull> <never>",
            "target=/probe,readonly>", "</probe/probe.py> <check>",
        ):
            self.assertIn(value, result.stderr)
        self.assertNotIn("</probe/probe.py> <run>", result.stderr)
        self.assertNotIn("<--name>", result.stderr)
        self.assertFalse(self.output.exists())

    # Scenario: Clock synchronization, container state, or topic metadata preflight fails.
    # Guarantees: --run aborts without starting the producer or creating run evidence.
    def test_failed_preflight_prevents_production(self):
        for override in (
            {"MOCK_CLOCK": "no"}, {"MOCK_RUNNING": "false"}, {"MOCK_CHECK_EXIT": "3"},
        ):
            with self.subTest(override=override):
                result = self.launch("--run", **override)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("</probe/probe.py> <run>", result.stderr)
                self.assertFalse(self.output.exists())

    # Scenario: Explicit --run reaches the mocked producer, which exits without a summary.
    # Guarantees: Bounded defaults are forwarded and failure evidence remains without a valid query.
    def test_run_defaults_preserve_failed_launcher_evidence(self):
        result = self.launch("--run")
        self.assertNotEqual(result.returncode, 0)
        log = result.stdout + result.stderr
        for value in (
            "<kafka-syslog-generator:86c927622>", "<kafka-cloud-bench>",
            "<--brokers> <kafka-broker:9092>", "<--topic> <otel-syslog>",
            "<--count> <1200> <--rate> <2>", "<--stop-timeout> <45>",
            "<--name> <kafka-la-probe-", "</probe/probe.py> <run>",
        ):
            self.assertIn(value, log)
        self.assertIn("No valid result", result.stderr)
        runs = list(self.output.glob("latency-*"))
        self.assertEqual(len(runs), 1)
        self.assertEqual((runs[0] / "producer-exit-code.txt").read_text().strip(), "42")
        self.assertTrue((runs[0] / "producer.log").is_file())
        self.assertFalse((runs[0] / "latency.kql").exists())

    # Scenario: Explicit count/rate and pipeline overrides are used for a probe run.
    # Guarantees: Metadata and producer commands use the same supplied topic and broker.
    def test_run_forwards_explicit_configuration(self):
        result = self.launch(
            "--run", "20", "3",
            KAFKA_BENCH_BROKERS="test-broker:9092",
            KAFKA_BENCH_TOPIC="test-topic",
        )
        self.assertNotEqual(result.returncode, 0)
        log = result.stdout + result.stderr
        self.assertEqual(log.count("<--brokers> <test-broker:9092>"), 2)
        self.assertEqual(log.count("<--topic> <test-topic>"), 2)
        self.assertIn("<--count> <20> <--rate> <3>", log)


if __name__ == "__main__":
    unittest.main()
