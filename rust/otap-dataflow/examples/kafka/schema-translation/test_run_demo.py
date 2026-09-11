# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import copy
import io
from types import SimpleNamespace
import unittest
from unittest import mock

from scripts.run_demo import (
    CLIENTS, Demo, DemoError, GROUP, PARTITIONS, TOPIC,
    assignment_sets, parse_group_description,
)


class DemoTests(unittest.TestCase):
    def setUp(self):
        self.demo = Demo(SimpleNamespace(
            cleanup=None, image="local-test-image", consumer_a_cores=None,
            consumer_b_cores=None, num_cores=1, timeout=30,
        ))
        self.rows = {
            p: {"offset": 0, "end": 0, "member": f"member-{p}",
                "client": CLIENTS["consumer-b" if p == 1 else "consumer-a"]}
            for p in PARTITIONS
        }
        self.before = {p: 0 for p in PARTITIONS}
        self.after = {0: 201, 1: 199, 2: 200}
        self.expected = ("consumer-a", "consumer-b")

    def phase_mocks(self, offsets):
        self.demo.wait_assignment = mock.Mock(return_value=self.rows)
        self.demo.end_offsets = mock.Mock(side_effect=offsets)
        self.demo.compose = mock.Mock()
        self.demo.save_evidence = mock.Mock()
        self.demo.describe = mock.Mock(return_value={
            p: {**row, "offset": self.after[p], "end": self.after[p]}
            for p, row in self.rows.items()
        })

    # Scenario: Kafka reports warning text, a header and uncommitted partitions.
    # Guarantees: The parser ignores non-data lines without turning absent commits into zero.
    def test_group_table_preserves_missing_commits(self):
        output = (
            "Warning: Consumer group is rebalancing.\nGROUP TOPIC PARTITION CURRENT-OFFSET\n"
            f"{GROUP} {TOPIC} 0 - 10 - member-a /172.20.0.2 {CLIENTS['consumer-a']}\n"
        )
        rows = parse_group_description(output)
        self.assertEqual(set(rows), {0})
        self.assertIsNone(rows[0]["offset"])
        self.assertEqual(rows[0]["end"], 10)
        self.assertIsNone(assignment_sets(rows, self.expected))

    # Scenario: The broker table contains duplicate partitions or malformed offsets.
    # Guarantees: Invalid evidence is rejected rather than silently overwritten.
    def test_invalid_group_rows_fail(self):
        line = f"{GROUP} {TOPIC} 0 0 0 0 member /host {CLIENTS['consumer-a']}\n"
        for output in (line + line, line.replace(" 0 0 0 0 ", " 0 bad 0 0 ")):
            with self.subTest(output=output), self.assertRaises(DemoError):
                parse_group_description(output)

    # Scenario: Consumers have assigned partitions, missing owners or an unexpected client.
    # Guarantees: Readiness requires all three partitions and participation by both containers.
    def test_assignment_coverage(self):
        self.assertEqual(assignment_sets(self.rows, self.expected),
                         {"consumer-a": [0, 2], "consumer-b": [1]})
        for change in ("missing-partition", "no-member", "unexpected-client", "one-container"):
            rows = copy.deepcopy(self.rows)
            if change == "missing-partition":
                del rows[2]
            elif change == "no-member":
                rows[1]["member"] = "-"
            elif change == "unexpected-client":
                rows[1]["client"] = "unrelated-client"
            else:
                rows[1]["client"] = CLIENTS["consumer-a"]
            with self.subTest(change=change):
                self.assertIsNone(assignment_sets(rows, self.expected))

    # Scenario: A multi-core engine uses core-suffixed client labels.
    # Guarantees: Per-core members are attributed to their containing process.
    def test_core_labels_are_grouped(self):
        self.rows[0]["client"] += "-0"
        self.rows[2]["client"] += "-1"
        self.assertEqual(assignment_sets(self.rows, self.expected),
                         {"consumer-a": [0, 2], "consumer-b": [1]})

    # Scenario: A bounded producer delivers data but keeps its engine process alive.
    # Guarantees: Completion uses exactly 600 new messages, waits for commits, then stops the producer.
    def test_bounded_production_does_not_wait_for_process_exit(self):
        self.phase_mocks([self.before, self.before, self.after])
        with mock.patch("scripts.run_demo.time.sleep"), mock.patch("sys.stdout", new=io.StringIO()):
            self.demo.produce_and_verify("test", self.expected)
        self.assertEqual(self.demo.end_offsets.call_count, 3)
        self.assertEqual(self.demo.compose.call_args_list, [
            mock.call("up", "-d", "--no-deps", "--force-recreate", "producer", timeout=30),
            mock.call("stop", "--timeout", "20", "producer"),
        ])
        phase = self.demo.evidence["phases"][0]
        self.assertEqual(phase["produced_messages"], 600)
        self.assertEqual(phase["commits_after"], self.after)

    # Scenario: Broker offsets remain at zero or show only part of the bounded batch.
    # Guarantees: Initial zero lag and partial production cannot be reported as success.
    def test_zero_lag_or_partial_batch_is_not_completion(self):
        for partial in (self.before, {0: 100, 1: 100, 2: 100}):
            self.phase_mocks([self.before, partial])

            def wait_once(_description, callback):
                self.assertIsNone(callback(30))
                raise DemoError("verification timeout")

            with self.subTest(partial=partial), mock.patch.object(self.demo, "wait_until", wait_once):
                with mock.patch("sys.stdout", new=io.StringIO()), self.assertRaisesRegex(DemoError, "timeout"):
                    self.demo.produce_and_verify("test", self.expected)
                self.demo.save_evidence.assert_not_called()
                self.demo.describe.assert_not_called()

    # Scenario: Bounded traffic exceeds its count or fails to reach one partition.
    # Guarantees: Both count and coverage are necessary for a successful phase.
    def test_message_bound_and_partition_progress(self):
        for offsets in ({0: 201, 1: 200, 2: 200}, {0: 300, 1: 300, 2: 0}):
            self.phase_mocks([self.before, offsets])
            with self.subTest(offsets=offsets), mock.patch("sys.stdout", new=io.StringIO()):
                with self.assertRaises(DemoError):
                    self.demo.produce_and_verify("test", self.expected)
                self.demo.save_evidence.assert_not_called()

    # Scenario: Production completes but commits are absent or one partition trails.
    # Guarantees: End-offset advancement alone cannot masquerade as consumption.
    def test_commits_must_reach_new_ends(self):
        for commit in (None, 199):
            self.phase_mocks([self.before, self.after])
            self.demo.describe.return_value[2]["offset"] = commit
            calls = 0

            def wait_once(_description, callback):
                nonlocal calls
                calls += 1
                value = callback(30)
                if calls == 1:
                    return value
                self.assertIsNone(value)
                raise DemoError("commit timeout")

            with self.subTest(commit=commit), mock.patch.object(self.demo, "wait_until", wait_once):
                with mock.patch("sys.stdout", new=io.StringIO()), self.assertRaisesRegex(DemoError, "timeout"):
                    self.demo.produce_and_verify("test", self.expected)
                self.demo.save_evidence.assert_not_called()

    # Scenario: The Kafka topic-end command omits or duplicates a partition.
    # Guarantees: Incomplete offset snapshots fail before progress is evaluated.
    def test_end_offset_coverage(self):
        valid = "".join(f"{TOPIC}:{p}:0\n" for p in PARTITIONS)
        for output in (valid.replace(f"{TOPIC}:2:0\n", ""), valid + f"{TOPIC}:0:0\n"):
            with self.subTest(output=output), mock.patch.object(
                self.demo, "broker", return_value=SimpleNamespace(stdout=output),
            ), self.assertRaises(DemoError):
                self.demo.end_offsets()

    # Scenario: A run finishes with a stopped producer in an optional Compose profile.
    # Guarantees: Cleanup includes that profile and checks only this project's resources.
    def test_cleanup_covers_producer_profile(self):
        self.demo.compose = mock.Mock()
        self.demo.command = mock.Mock(return_value=SimpleNamespace(stdout=""))
        with mock.patch("sys.stdout", new=io.StringIO()):
            self.demo.cleanup()
        self.assertEqual(self.demo.compose.call_args.args[:3], ("--profile", "producer", "down"))
        self.assertEqual(self.demo.command.call_count, 3)
        for call in self.demo.command.call_args_list:
            self.assertIn(f"label=com.docker.compose.project={self.demo.project}", call.args[0])

    # Scenario: Docker reports that a project-owned volume survived compose down.
    # Guarantees: Cleanup does not print a success-shaped claim when resources remain.
    def test_cleanup_reports_remaining_resources(self):
        self.demo.compose = mock.Mock()
        self.demo.command = mock.Mock(side_effect=[
            SimpleNamespace(stdout=""), SimpleNamespace(stdout=""),
            SimpleNamespace(stdout="remaining-volume"),
        ])
        with self.assertRaisesRegex(DemoError, "remaining-volume"):
            self.demo.cleanup()


if __name__ == "__main__":
    unittest.main()
