#!/usr/bin/env python3
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Socket-free container setup, engine startup and bounded Kafka demonstration."""

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.run_demo import CLIENTS, GROUP, HERE, PARTITIONS, TOPIC, DemoError, assignment_sets, generate_configs
from translate import ConfigError, load_yaml, runtime_environment

OUTPUT = Path("/output")
PRIVATE = Path("/runtime")
ENGINE = "/engine/df_engine"
SECRET_NAMES = ("KAFKA_DEMO_USERNAME", "KAFKA_DEMO_PASSWORD", "KAFKA_DEMO_STORE_PASSWORD")


def read_credentials(directory):
    values = json.loads((directory / "credentials.json").read_text(encoding="utf-8"))
    if not isinstance(values, dict) or set(values) != set(SECRET_NAMES):
        raise DemoError("Invalid local credential file; recreate the demo's volumes")
    if any(not isinstance(v, str) or not re.fullmatch(r"[A-Za-z0-9_]{1,128}", v)
           for v in values.values()):
        raise DemoError("Invalid local credential value; recreate the demo's volumes")
    return values


def prepare(output=OUTPUT, private=PRIVATE):
    output.mkdir(parents=True, exist_ok=True)
    private.mkdir(parents=True, exist_ok=True)
    private.chmod(0o700)
    if (private / "credentials.json").exists():
        values = read_credentials(private)
    else:
        values = {
            "KAFKA_DEMO_USERNAME": "demo_" + secrets.token_hex(4),
            "KAFKA_DEMO_PASSWORD": secrets.token_hex(32),
            "KAFKA_DEMO_STORE_PASSWORD": secrets.token_hex(32),
        }
        path = private / "credentials.json"
        with path.open("x", encoding="utf-8") as stream:
            path.chmod(0o600)
            json.dump(values, stream)
    shell_path = private / "credentials.env"
    with shell_path.open("w", encoding="utf-8", newline="\n") as stream:
        shell_path.chmod(0o600)
        stream.write("".join(f"export {k}={shlex.quote(v)}\n" for k, v in values.items()))
    generate_configs(output, "dynamic")
    (output / "evidence.json").unlink(missing_ok=True)
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        for path in (output, private, private / "credentials.json", shell_path):
            os.chown(path, 65532, 65532)
    print("Local secrets are in a private Docker volume, not generated YAML.", flush=True)


def environment():
    return runtime_environment(load_yaml(HERE / "bindings.yaml"),
                               {**os.environ, **read_credentials(PRIVATE)})


def redact(text, env):
    for key in SECRET_NAMES:
        value = env.get(key)
        if value:
            text = text.replace(value, "[redacted]")
    return text


def engine_command(name):
    return [ENGINE, "--config", f"file:{OUTPUT / (name + '.yaml')}",
            "--num-cores", "1", "--http-admin-bind", "127.0.0.1:8080"]


def validate(name, env):
    result = subprocess.run(engine_command(name) + ["--validate-and-exit"],
                            env=env, capture_output=True, text=True, timeout=60)
    if result.returncode:
        raise DemoError("Native validation failed:\n" + redact(result.stdout + result.stderr, env))


def wait_until(description, callback, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = callback()
        if value is not None:
            return value
        time.sleep(min(1, max(0, deadline - time.monotonic())))
    raise DemoError(f"Timed out waiting for {description}")


class Broker:
    """Use the project's private admin listener without Docker socket access."""

    def __init__(self):
        from confluent_kafka.admin import AdminClient
        self.admin = AdminClient({"bootstrap.servers": "kafka:29092", "socket.timeout.ms": 10000})

    def rows(self):
        from confluent_kafka import ConsumerGroupTopicPartitions, KafkaError, KafkaException
        try:
            group = self.admin.describe_consumer_groups([GROUP], request_timeout=10)[GROUP].result(15)
            if group.state.name != "STABLE":
                return {}
            offsets = self.admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(GROUP)], request_timeout=10,
            )[GROUP].result(15)
        except KafkaException as exc:
            if exc.args[0].code() in (
                KafkaError.GROUP_ID_NOT_FOUND, KafkaError.NOT_COORDINATOR,
                KafkaError.COORDINATOR_NOT_AVAILABLE, KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
            ):
                print(f"Waiting for the group coordinator: {exc.args[0].name()}", flush=True)
                return {}
            raise
        commits = {(p.topic, p.partition): p.offset for p in offsets.topic_partitions}
        rows = {}
        for member in group.members:
            for partition in member.assignment.topic_partitions:
                if partition.topic != TOPIC:
                    continue
                p = partition.partition
                if p in rows:
                    raise DemoError("A partition has multiple owners")
                offset = commits.get((TOPIC, p), -1)
                rows[p] = {"member": member.member_id, "client": member.client_id,
                           "offset": offset if offset >= 0 else None}
        return rows

    def ends(self):
        from confluent_kafka import TopicPartition
        from confluent_kafka.admin import OffsetSpec
        futures = self.admin.list_offsets(
            {TopicPartition(TOPIC, p): OffsetSpec.latest() for p in PARTITIONS}, request_timeout=10,
        )
        return {p.partition: future.result(15).offset for p, future in futures.items()}


def produced_offsets(before, after, count):
    if set(before) != PARTITIONS or set(after) != PARTITIONS:
        raise DemoError("Topic offsets must cover all three partitions")
    deltas = [after[p] - before[p] for p in PARTITIONS]
    if any(delta < 0 for delta in deltas) or sum(deltas) > count:
        raise DemoError("Unexpected topic offset change; another producer may be running")
    if sum(deltas) != count:
        return None
    if any(delta == 0 for delta in deltas):
        raise DemoError("The batch did not advance every partition")
    return after


def committed_rows(rows, after):
    if assignment_sets(rows, tuple(CLIENTS)) is None:
        return None
    if any(rows[p]["offset"] is None or rows[p]["offset"] < after[p] for p in PARTITIONS):
        return None
    return rows


def verify(timeout=240):
    (OUTPUT / "evidence.json").unlink(missing_ok=True)
    env = environment()
    validate("producer", env)
    broker = Broker()
    last_signature = None
    stable_since = time.monotonic()

    def ready():
        nonlocal last_signature, stable_since
        rows = broker.rows()
        assigned = assignment_sets(rows, tuple(CLIENTS))
        signature = {p: row["member"] for p, row in rows.items()} if assigned else None
        if signature is None or signature != last_signature:
            last_signature, stable_since = signature, time.monotonic()
            return None
        return rows if time.monotonic() - stable_since >= 4 else None

    wait_until("both consumers to own disjoint partitions", ready, timeout)
    before = broker.ends()
    # Keep this in sync with the bounded generator, not a sleep or a zero-lag check.
    producer_config = load_yaml(OUTPUT / "producer.yaml")
    traffic = producer_config["groups"]["default"]["pipelines"]["main"]["nodes"][
        "traffic-generator"]["config"]["traffic_config"]
    if traffic["max_batch_size"] != 1 or traffic["max_signal_count"] != 600:
        raise DemoError("The UI fixture requires 600 single-record batches")
    print("Both consumers assigned; producing 600 logs over SASL/TLS.", flush=True)
    with (OUTPUT / "producer.log").open("w", encoding="utf-8") as log:
        process = subprocess.Popen(engine_command("producer"), env=env, stdout=log, stderr=log)
        try:
            def produced():
                if process.poll() is not None and process.returncode != 0:
                    raise DemoError("Producer failed; see generated/ui/producer.log")
                return produced_offsets(before, broker.ends(), 600)

            after = wait_until("600 new Kafka messages", produced, timeout)
            rows = wait_until("consumer commits to reach the new offsets",
                              lambda: committed_rows(broker.rows(), after), timeout)
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
                    raise DemoError("Producer did not shut down gracefully; it was terminated") from None
    version = subprocess.run([ENGINE, "--version"], capture_output=True, text=True, check=True, timeout=15)
    evidence = {
        "success": True, "membership": "dynamic", "engine_version": version.stdout.strip(),
        "engine_image": os.environ.get("KAFKA_DEMO_IMAGE"),
        "produced_messages": 600, "ends_before": before, "ends_after": after,
        "commits_after": {p: row["offset"] for p, row in rows.items()},
        "assignments": assignment_sets(rows, tuple(CLIENTS)),
    }
    (OUTPUT / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print("PASS: 600 new messages committed across all three partitions. "
          "Consumers and Redpanda Console remain running.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "engine", "verify"))
    parser.add_argument("name", nargs="?", choices=tuple(CLIENTS))
    args = parser.parse_args()
    if args.action == "engine" and args.name is None:
        parser.error("engine requires consumer-a or consumer-b")
    try:
        if args.action == "prepare":
            prepare()
        elif args.action == "engine":
            env = environment()
            validate(args.name, env)
            os.execve(ENGINE, engine_command(args.name), env)
        else:
            verify()
    except (ConfigError, DemoError, OSError, ValueError, subprocess.SubprocessError) as exc:
        print(f"Container demo failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
