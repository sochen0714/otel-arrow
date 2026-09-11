#!/usr/bin/env python3
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Run an isolated local SASL/TLS Kafka schema-translation demonstration."""

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import sys
import time
import uuid

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))
from translate import ConfigError, load_yaml, runtime_environment, translate, write_config

TOPIC = "schema-demo-logs"
GROUP = "schema-demo-group"
PARTITIONS = {0, 1, 2}
CLIENTS = {name: f"kafka-local-{name}" for name in ("consumer-a", "consumer-b")}


class DemoError(RuntimeError):
    """The demo could not verify its required broker-side invariants."""


def parse_group_description(output):
    """Read Kafka's offset table, retaining missing commits as None."""
    rows = {}
    for line in output.splitlines():
        fields = line.split()
        if len(fields) < 9 or fields[:2] != [GROUP, TOPIC]:
            continue
        try:
            partition = int(fields[2])
            offset = None if fields[3] == "-" else int(fields[3])
            end = None if fields[4] == "-" else int(fields[4])
        except ValueError as exc:
            raise DemoError("Malformed broker consumer-group offset table") from exc
        if partition in rows:
            raise DemoError(f"Partition {partition} appears more than once in the group")
        rows[partition] = {
            "offset": offset, "end": end, "member": fields[6], "client": fields[8],
        }
    return rows


def assignment_sets(rows, expected):
    """Return disjoint per-container assignments, or None while rebalancing."""
    if set(rows) != PARTITIONS:
        return None
    assigned = {name: [] for name in expected}
    for partition, row in rows.items():
        if row["member"] == "-" or row["client"] == "-":
            return None
        matches = [
            name for name in expected
            if row["client"] == CLIENTS[name]
            or row["client"].startswith(CLIENTS[name] + "-")
        ]
        if len(matches) != 1:
            return None
        assigned[matches[0]].append(partition)
    if any(not partitions for partitions in assigned.values()):
        return None
    return {name: sorted(partitions) for name, partitions in assigned.items()}


def generate_configs(directory, membership):
    """Translate the local fixture for either the host or container launcher."""
    document = load_yaml(HERE / "customer.yaml")
    bindings = load_yaml(HERE / "bindings.yaml")
    if not isinstance(document, dict) or document.get("name") != "kafka-local":
        raise DemoError("The demo requires the checked-in kafka-local customer fixture")
    for instance in CLIENTS:
        config = translate(
            document, bindings, instance_id=instance, membership=membership,
            rebalance_strategy="cooperative_sticky", auth_policy="sasl-only",
        )
        native = config["groups"]["default"]["pipelines"]["main"]["nodes"]["kafka-local"]["config"]
        expected_auth = {
            "mechanism": "PLAIN",
            "username": "${env:KAFKA_DEMO_USERNAME__JSON}",
            "password": "${env:KAFKA_DEMO_PASSWORD__JSON}",
        }
        if not (
            native["brokers"] == "kafka:9093" and native["group_id"] == GROUP
            and native["logs"] == {"topics": [TOPIC], "encoding": "otlp_proto"}
            and native.get("tls") == {"ca_file": "/home/nonroot/certs/ca.crt"}
            and native.get("auth", {}).get("sasl") == expected_auth
            and native["auto_offset_reset"] == "latest"
            and not any(signal in native for signal in ("metrics", "traces"))
        ):
            raise DemoError("The demo requires the checked-in broker, group, log topic, and SASL/TLS bindings")
        write_config(directory / f"{instance}.yaml", config)
    shutil.copyfile(HERE / "producer.yaml.template", directory / "producer.yaml")
    print(f"Generated receiver configurations: {directory}", flush=True)
    return bindings


class Demo:
    def __init__(self, args):
        self.args = args
        self.project = args.cleanup or "kafka-schema-" + uuid.uuid4().hex[:12]
        self.generated = HERE / "generated" / self.project
        self.env = os.environ.copy()
        # Do not reuse credentials from a developer's shell or any real system.
        self.env.update({
            "KAFKA_DEMO_USERNAME": "demo_" + secrets.token_hex(4),
            "KAFKA_DEMO_PASSWORD": secrets.token_hex(32),
            "KAFKA_DEMO_STORE_PASSWORD": secrets.token_hex(32),
            "KAFKA_DEMO_IMAGE": args.image,
            "KAFKA_DEMO_GENERATED_DIR": str(self.generated),
            "KAFKA_DEMO_CORES_A": str(args.consumer_a_cores or args.num_cores),
            "KAFKA_DEMO_CORES_B": str(args.consumer_b_cores or args.num_cores),
        })
        for name in ("KAFKA_DEMO_USERNAME", "KAFKA_DEMO_PASSWORD"):
            self.env[name + "__JSON"] = json.dumps(self.env[name])
        self.prefix = ["docker", "compose", "--project-name", self.project,
                       "--file", str(HERE / "compose.yaml")]
        self.evidence = {"project": self.project, "image": args.image, "phases": []}
        self.last_group = ""

    def redact(self, output):
        for name in ("KAFKA_DEMO_USERNAME", "KAFKA_DEMO_PASSWORD", "KAFKA_DEMO_STORE_PASSWORD"):
            raw = self.env[name]
            for value in (json.dumps(raw), json.dumps(raw)[1:-1], raw):
                output = output.replace(value, "[redacted]")
        return output

    def command(self, arguments, *, timeout=60, check=True, show=False):
        try:
            result = subprocess.run(
                arguments, cwd=HERE, env=self.env, capture_output=True,
                text=True, encoding="utf-8", errors="replace", timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            raise DemoError(f"Command timed out after {timeout:.0f}s: {' '.join(arguments)}") from None
        result.stdout = self.redact(result.stdout)
        result.stderr = self.redact(result.stderr)
        if show or (check and result.returncode):
            print(result.stdout + result.stderr, end="", flush=True)
        if check and result.returncode:
            raise DemoError(f"Command exited {result.returncode}: {' '.join(arguments)}")
        return result

    def compose(self, *arguments, **kwargs):
        return self.command(self.prefix + list(arguments), **kwargs)

    def broker(self, executable, *arguments, timeout=45):
        return self.compose("exec", "-T", "kafka", executable,
                            "--bootstrap-server", "kafka:29092", *arguments, timeout=timeout)

    def generate(self):
        bindings = generate_configs(self.generated, self.args.membership)
        self.env = runtime_environment(bindings, self.env)

    def wait_until(self, description, callback):
        deadline = time.monotonic() + self.args.timeout
        while time.monotonic() < deadline:
            value = callback(max(1, min(45, deadline - time.monotonic())))
            if value is not None:
                return value
            time.sleep(min(2, max(0, deadline - time.monotonic())))
        raise DemoError(f"Timed out waiting for {description}")

    def describe(self, timeout=45):
        result = self.broker("kafka-consumer-groups", "--group", GROUP, "--describe", timeout=timeout)
        self.last_group = result.stdout + result.stderr
        return parse_group_description(result.stdout)

    def wait_assignment(self, expected):
        last = None
        since = time.monotonic()

        def check(timeout):
            nonlocal last, since
            rows = self.describe(timeout)
            assigned = assignment_sets(rows, expected)
            signature = (assigned, {p: row["member"] for p, row in rows.items()})
            if assigned is None or signature != last:
                last, since = signature, time.monotonic()
                return None
            if time.monotonic() - since < 4:
                return None
            return rows

        rows = self.wait_until(f"stable disjoint assignments for {', '.join(expected)}", check)
        assigned = assignment_sets(rows, expected)
        print(f"Verified partition coverage: {json.dumps(assigned, sort_keys=True)}", flush=True)
        return rows

    def end_offsets(self, timeout=45):
        result = self.broker("kafka-get-offsets", "--topic", TOPIC, "--time", "-1", timeout=timeout)
        offsets = {}
        for line in result.stdout.splitlines():
            fields = line.strip().split(":")
            if len(fields) == 3 and fields[0] == TOPIC:
                try:
                    partition, offset = int(fields[1]), int(fields[2])
                except ValueError as exc:
                    raise DemoError("Malformed broker topic-end offset table") from exc
                if partition in offsets:
                    raise DemoError("Duplicate partition in broker topic-end offset table")
                offsets[partition] = offset
        if set(offsets) != PARTITIONS:
            raise DemoError("Topic end-offset snapshot did not cover exactly three partitions")
        return offsets

    def produce_and_verify(self, phase, expected):
        initial_group = self.wait_assignment(expected)
        before = self.end_offsets()
        print(f"{phase}: producing 600 bounded synthetic logs AFTER assignment; ends before={before}", flush=True)
        # Reaching max_signal_count stops generation, not the engine process.
        # One-record batches let broker offsets prove completion without waiting
        # for a process exit that requires an explicit shutdown request.
        self.compose("up", "-d", "--no-deps", "--force-recreate", "producer",
                     timeout=self.args.timeout)

        def produced(timeout):
            offsets = self.end_offsets(timeout)
            count = sum(offsets[p] - before[p] for p in PARTITIONS)
            if count > 600:
                raise DemoError(f"Producer exceeded its 600-message bound: {count}")
            return offsets if count == 600 else None

        after = self.wait_until("600 new single-record Kafka messages", produced)
        if any(after[p] <= before[p] for p in PARTITIONS):
            raise DemoError(f"Bounded traffic did not advance every partition: before={before}, after={after}")

        def check(timeout):
            rows = self.describe(timeout)
            if assignment_sets(rows, expected) is None:
                return None
            if any(rows[p]["offset"] is None or rows[p]["offset"] < after[p] for p in PARTITIONS):
                return None
            return rows

        consumed = self.wait_until(f"{phase} committed offsets reaching the NEW topic ends", check)
        self.compose("stop", "--timeout", "20", "producer")
        # This comparison cannot pass merely because a newly created topic had
        # zero lag: every new end offset is strictly greater than the old end.
        phase_result = {
            "phase": phase,
            "assignments": assignment_sets(consumed, expected),
            "clients": sorted({row["client"] for row in consumed.values()}),
            "members": {p: row["member"] for p, row in consumed.items()},
            "produced_messages": sum(after[p] - before[p] for p in PARTITIONS),
            "ends_before": before, "ends_after": after,
            "commits_before": {p: row["offset"] for p, row in initial_group.items()},
            "commits_after": {p: row["offset"] for p, row in consumed.items()},
        }
        self.evidence["phases"].append(phase_result)
        self.save_evidence()
        print(f"{phase}: verified commits reached new ends {after}", flush=True)

    def save_evidence(self):
        self.generated.mkdir(parents=True, exist_ok=True)
        output = json.dumps(self.evidence, indent=2, sort_keys=True) + "\n"
        (self.generated / "evidence.json").write_text(self.redact(output), encoding="utf-8")

    def diagnostics(self):
        print("Collecting redacted project diagnostics...", file=sys.stderr, flush=True)
        for args in (("ps", "--all"), ("logs", "--no-color", "--tail", "100")):
            try:
                result = self.compose(*args, check=False, timeout=30)
                print(result.stdout + result.stderr, file=sys.stderr, end="")
            except (DemoError, OSError) as exc:
                print(f"Diagnostic command failed: {exc}", file=sys.stderr)
        if self.last_group:
            print(self.last_group, file=sys.stderr)

    def cleanup(self):
        self.compose("--profile", "producer", "down", "--volumes", "--remove-orphans", "--timeout", "15",
                     timeout=120, show=True)
        for kind in ("container", "network", "volume"):
            options = ["--all"] if kind == "container" else []
            remaining = self.command([
                "docker", kind, "ls", *options, "--quiet", "--filter",
                f"label=com.docker.compose.project={self.project}",
            ]).stdout.strip()
            if remaining:
                raise DemoError(f"Cleanup left project {kind} resources: {remaining}; retry --cleanup {self.project}")
        print(f"Removed only Compose project {self.project}, including private certificate volumes.", flush=True)

    def run(self):
        print(f"Unique local Compose project: {self.project}", flush=True)
        self.generate()
        self.command(["docker", "info", "--format", "{{.ServerVersion}}"])
        self.compose("config", "--quiet")
        started = False
        try:
            if self.args.skip_build:
                print("Using an existing image; this is NOT evidence of a build from the current worktree.", flush=True)
            else:
                print("Building the Kafka-enabled engine from this worktree (Docker layer/cache reuse allowed).", flush=True)
                self.compose("build", "consumer-a", timeout=self.args.build_timeout, show=True)
            image = self.command(["docker", "image", "inspect", self.args.image,
                                  "--format", "{{.Id}}"]).stdout.strip()
            self.evidence.update(image_id=image, built_from_worktree=not self.args.skip_build,
                                 membership=self.args.membership,
                                 cores={"consumer-a": self.env["KAFKA_DEMO_CORES_A"],
                                        "consumer-b": self.env["KAFKA_DEMO_CORES_B"]})
            print(f"Engine image: {self.args.image} ({image})", flush=True)
            started = True
            self.compose("up", "-d", "kafka-init", timeout=self.args.timeout, show=True)
            init_id = self.compose("ps", "--all", "--quiet", "kafka-init").stdout.strip()
            if not init_id:
                raise DemoError("Topic initializer container was not created")

            def initialized(timeout):
                state = json.loads(self.command(
                    ["docker", "inspect", init_id, "--format", "{{json .State}}"], timeout=timeout,
                ).stdout)
                if state["Status"] == "exited":
                    if state["ExitCode"] != 0:
                        raise DemoError("Kafka topic initialization failed")
                    return True
                return None

            self.wait_until("successful topic initialization", initialized)
            for instance in CLIENTS:
                self.compose(
                    "run", "--rm", "--no-deps", "--name", f"{self.project}-validate-{instance}",
                    instance, "--config", "file:/home/nonroot/consumer.yaml", "--validate-and-exit",
                    timeout=self.args.timeout,
                )
            self.compose(
                "run", "--rm", "--no-deps", "--name", f"{self.project}-validate-producer",
                "producer", "--config", "file:/home/nonroot/producer.yaml", "--validate-and-exit",
                timeout=self.args.timeout,
            )
            print("Native validation passed; broker-side consumption evidence is still required.", flush=True)
            self.compose("up", "-d", "--no-deps", "consumer-a", "consumer-b", show=True)
            self.produce_and_verify("both-consumers", ("consumer-a", "consumer-b"))
            if not self.args.no_restart:
                self.compose("stop", "--timeout", "20", "consumer-b", show=True)
                self.produce_and_verify("consumer-b-stopped", ("consumer-a",))
                self.compose("up", "-d", "--no-deps", "consumer-b", show=True)
                self.produce_and_verify("consumer-b-restarted", ("consumer-a", "consumer-b"))
            self.evidence["success"] = True
            self.save_evidence()
            print(f"PASS: schema translation, SASL/TLS, assignments, and NEW committed offsets verified.\n"
                  f"Evidence: {self.generated / 'evidence.json'}", flush=True)
        except (DemoError, ConfigError, OSError, KeyboardInterrupt):
            if started:
                self.diagnostics()
            raise
        finally:
            if started:
                if self.args.keep_running:
                    print(f"Project left running. Remove it with:\n"
                          f"  python scripts/run_demo.py --cleanup {self.project}", flush=True)
                else:
                    self.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build", action="store_true",
                        help="Reuse the default demo image without rebuilding; --image also skips the build")
    parser.add_argument("--image",
                        help="Reuse this existing Kafka-enabled engine image without building or pulling it")
    parser.add_argument("--num-cores", type=int, choices=(1, 2), default=1,
                        help="Cores per consumer (1 or 2 for this three-partition fixture)")
    parser.add_argument("--consumer-a-cores", type=int, choices=(1, 2))
    parser.add_argument("--consumer-b-cores", type=int, choices=(1, 2))
    parser.add_argument("--membership", choices=("dynamic", "static"), default="dynamic")
    parser.add_argument("--timeout", type=int, default=240, help="Maximum seconds per startup/verification phase")
    parser.add_argument("--build-timeout", type=int, default=3600)
    parser.add_argument("--no-restart", action="store_true", help="Skip the stop/restart portion")
    parser.add_argument("--keep-running", action="store_true", help="Retain only this demo project for local inspection")
    parser.add_argument("--cleanup", metavar="PROJECT", help="Remove a retained kafka-schema-* project and its volumes")
    args = parser.parse_args()
    args.skip_build = args.skip_build or args.image is not None
    args.image = args.image or "otap-kafka-schema-translation:local"
    if args.timeout < 30 or args.build_timeout < 30:
        parser.error("Timeouts must be at least 30 seconds")
    if args.cleanup and not re.fullmatch(r"kafka-schema-[0-9a-f]{12}", args.cleanup):
        parser.error("--cleanup requires the exact project name printed by this launcher")
    demo = Demo(args)
    try:
        if args.cleanup:
            demo.cleanup()
        else:
            demo.run()
    except (DemoError, ConfigError, OSError, KeyboardInterrupt) as exc:
        print(f"Demo failed: {demo.redact(str(exc)) or type(exc).__name__}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
