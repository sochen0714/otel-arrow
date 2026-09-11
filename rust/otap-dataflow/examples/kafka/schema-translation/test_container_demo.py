# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import io
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import yaml

from scripts.container_demo import Broker, committed_rows, prepare, produced_offsets, read_credentials
from scripts.run_demo import CLIENTS, DemoError, GROUP, PARTITIONS
from translate import load_yaml


class ContainerDemoTests(unittest.TestCase):
    # Scenario: Kafka's group coordinator is not ready during initial startup.
    # Guarantees: Only recognized coordinator-startup errors enter the bounded retry loop.
    @unittest.skipUnless(importlib.util.find_spec("confluent_kafka"), "Kafka SDK is in the Docker image")
    def test_coordinator_startup_retries(self):
        from confluent_kafka import KafkaError, KafkaException
        broker = object.__new__(Broker)
        broker.admin = mock.Mock()
        for code in (KafkaError.NOT_COORDINATOR, KafkaError.COORDINATOR_NOT_AVAILABLE,
                     KafkaError.COORDINATOR_LOAD_IN_PROGRESS, KafkaError.GROUP_ID_NOT_FOUND):
            future = mock.Mock()
            future.result.side_effect = KafkaException(KafkaError(code))
            broker.admin.describe_consumer_groups.return_value = {GROUP: future}
            with self.subTest(code=code), mock.patch("sys.stdout", new=io.StringIO()):
                self.assertEqual(broker.rows(), {})

    # Scenario: A group inspection fails because Kafka denies access.
    # Guarantees: Authorization errors surface immediately rather than looking like readiness retries.
    @unittest.skipUnless(importlib.util.find_spec("confluent_kafka"), "Kafka SDK is in the Docker image")
    def test_nontransient_broker_errors_propagate(self):
        from confluent_kafka import KafkaError, KafkaException
        broker = object.__new__(Broker)
        broker.admin = mock.Mock()
        future = mock.Mock()
        future.result.side_effect = KafkaException(KafkaError(KafkaError.GROUP_AUTHORIZATION_FAILED))
        broker.admin.describe_consumer_groups.return_value = {GROUP: future}
        with self.assertRaises(KafkaException):
            broker.rows()

    # Scenario: Container setup creates customer-derived configs and local credentials.
    # Guarantees: Both consumers use dynamic membership and no raw secrets enter public output.
    def test_prepare_separates_secrets_from_configs(self):
        with tempfile.TemporaryDirectory() as temporary, mock.patch("sys.stdout", new=io.StringIO()):
            root = Path(temporary)
            output, private = root / "output", root / "private"
            prepare(output, private)
            values = read_credentials(private)
            for name in CLIENTS:
                path = output / f"{name}.yaml"
                config = load_yaml(path)["groups"]["default"]["pipelines"]["main"]["nodes"]["kafka-local"]["config"]
                self.assertNotIn("group_instance_id", config)
                self.assertEqual(config["client_id"], CLIENTS[name])
                for secret in values.values():
                    self.assertNotIn(secret, path.read_text())
            self.assertTrue((output / "producer.yaml").exists())
            self.assertTrue((private / "credentials.env").exists())

    # Scenario: The user repeats compose up against persistent project volumes.
    # Guarantees: Broker credentials remain stable and previous success evidence is removed.
    def test_prepare_is_repeatable(self):
        with tempfile.TemporaryDirectory() as temporary, mock.patch("sys.stdout", new=io.StringIO()):
            root = Path(temporary)
            output, private = root / "output", root / "private"
            prepare(output, private)
            values = read_credentials(private)
            (output / "evidence.json").write_text('{"success": true}')
            prepare(output, private)
            self.assertEqual(read_credentials(private), values)
            self.assertFalse((output / "evidence.json").exists())

    # Scenario: A persistent credential file has unknown fields or shell metacharacters.
    # Guarantees: Setup refuses corruption instead of projecting it into a shell environment.
    def test_invalid_persistent_credentials_fail(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for values in ({"password": "unexpected"}, {
                "KAFKA_DEMO_USERNAME": "demo",
                "KAFKA_DEMO_PASSWORD": "$(command)",
                "KAFKA_DEMO_STORE_PASSWORD": "test",
            }):
                (directory / "credentials.json").write_text(json.dumps(values))
                with self.subTest(values=values), self.assertRaises(DemoError):
                    read_credentials(directory)

    # Scenario: A newly created topic has zero lag or an incomplete batch.
    # Guarantees: Neither case can count as completion of the required 600 new messages.
    def test_production_requires_new_complete_batch(self):
        before = {p: 10 for p in PARTITIONS}
        self.assertIsNone(produced_offsets(before, before, 600))
        self.assertIsNone(produced_offsets(before, {p: 11 for p in PARTITIONS}, 600))
        after = {p: 210 for p in PARTITIONS}
        self.assertEqual(produced_offsets(before, after, 600), after)

    # Scenario: Offsets regress, omit partitions, exceed the bound or miss one partition.
    # Guarantees: Unexpected broker state is surfaced as a failure.
    def test_invalid_production_evidence_fails(self):
        before = {p: 10 for p in PARTITIONS}
        for after in ({0: 210}, {0: 9, 1: 10, 2: 10},
                      {p: 211 for p in PARTITIONS}, {0: 10, 1: 310, 2: 310}):
            with self.subTest(after=after), self.assertRaises(DemoError):
                produced_offsets(before, after, 600)

    # Scenario: A produced batch has commits missing or behind, or has lost a consumer.
    # Guarantees: Success requires new committed offsets and participation by both consumers.
    def test_consumption_requires_commits_and_both_consumers(self):
        after = {p: 200 for p in PARTITIONS}
        rows = {
            p: {"client": CLIENTS["consumer-b" if p == 1 else "consumer-a"],
                "member": f"member-{p}", "offset": 200}
            for p in PARTITIONS
        }
        self.assertEqual(committed_rows(rows, after), rows)
        for offset in (None, 199):
            rows[0]["offset"] = offset
            self.assertIsNone(committed_rows(rows, after))
        rows[0]["offset"] = 200
        rows[1]["client"] = CLIENTS["consumer-a"]
        self.assertIsNone(committed_rows(rows, after))

    # Scenario: Docker-only startup exposes a Console and mounts runtime inputs.
    # Guarantees: Only the UI is host-published, no Docker socket is mounted, and UI awaits progress.
    def test_compose_is_socket_free_and_loopback_only(self):
        path = Path(__file__).parent / "compose.ui.yaml"
        config = yaml.safe_load(path.read_text())
        services = config["services"]
        self.assertEqual(services["console"]["depends_on"]["verify"]["condition"],
                         "service_completed_successfully")
        self.assertEqual(services["console"]["ports"], ["127.0.0.1:${KAFKA_DEMO_UI_PORT:-8085}:8080"])
        self.assertEqual(services["console"]["networks"], ["isolated", "ui"])
        for name, service in services.items():
            self.assertNotIn("privileged", service)
            for volume in service.get("volumes", []):
                self.assertNotIn("docker.sock", volume)
            if name != "console":
                self.assertNotIn("ports", service)
                self.assertNotIn("ui", service.get("networks", []))

    # Scenario: Redpanda Console is exposed for a local UI demonstration.
    # Guarantees: Vendor analytics and Redpanda-only broker administration stay disabled.
    def test_console_disables_vendor_analytics(self):
        config = yaml.safe_load((Path(__file__).parent / "console.yaml").read_text())
        self.assertFalse(config["analytics"]["enabled"])
        self.assertFalse(config["redpanda"]["adminApi"]["enabled"])


if __name__ == "__main__":
    unittest.main()
