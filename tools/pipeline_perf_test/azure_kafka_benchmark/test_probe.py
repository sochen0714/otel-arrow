"""Offline regression tests: no Docker, Kafka, Azure, or network access."""

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

import probe


RUN_ID = "de1400b2-a2b2-4853-8068-f59c6ec7d24b"
START = datetime(2026, 10, 6, 21, 0, 0, 123456, tzinfo=timezone.utc)


class FakeClock:
    def __init__(self):
        self.elapsed = 0.0
        self.wall_offset = 0.0

    def monotonic(self):
        return self.elapsed

    def now(self):
        return START + timedelta(seconds=self.elapsed + self.wall_offset)

    def sleep(self, seconds):
        self.elapsed += seconds


class FakeProducer:
    def __init__(self, clock=None, error=None, pending=0, stall_first=0):
        self.clock = clock
        self.error = error
        self.pending = pending
        self.stall_first = stall_first
        self.records = []
        self.callbacks = []

    def produce(self, topic, value, on_delivery):
        self.records.append((topic, value))
        self.callbacks.append(on_delivery)
        if len(self.records) == 1 and self.clock:
            self.clock.elapsed += self.stall_first

    def poll(self, _timeout):
        while self.callbacks:
            callback = self.callbacks.pop(0)
            callback(self.error, SimpleNamespace(partition=lambda: 0, offset=lambda: 10))

    def flush(self, _timeout):
        if not self.pending:
            self.poll(0)
        return self.pending

    def list_topics(self, timeout):
        partition = SimpleNamespace(error=None, leader=1)
        topic = SimpleNamespace(error=None, partitions={0: partition})
        return SimpleNamespace(topics={"otel-syslog": topic})


def args(count=3, rate=2, output=None):
    return SimpleNamespace(
        run_id=RUN_ID, topic="otel-syslog", brokers="kafka-broker:9092",
        count=count, rate=rate, output=output,
    )


def body(record):
    return json.loads(record.split(b" - LA-LATENCY - ", 1)[1])


class ProbeTests(unittest.TestCase):
    # Scenario: Sequence widths and subsecond timestamps vary across RFC5424 probes.
    # Guarantees: Each record is 1024 ASCII bytes, with intact parseable identity and timestamp.
    def test_record_size_and_identity(self):
        for seq in (0, 9, 10, 99999):
            record = probe.make_record(RUN_ID, seq, START)
            self.assertEqual(len(record), 1024)
            self.assertTrue(record.isascii())
            self.assertFalse(record.endswith(b"\n"))
            self.assertTrue(record.startswith(b"<134>1 2026-10-06T21:00:00.123456Z "))
            data = body(record)
            self.assertEqual(data["run_id"], RUN_ID)
            self.assertEqual(data["sequence"], seq)
            self.assertEqual(data["sent_at"], "2026-10-06T21:00:00.123456Z")
            self.assertEqual(data["kind"], probe.KIND)

    # Scenario: Three probes are sent at two probes per second using a controlled clock.
    # Guarantees: Timestamps are fresh per enqueue and acknowledgment evidence reconciles.
    def test_fresh_per_send_timestamps(self):
        clock = FakeClock()
        producer = FakeProducer(clock)
        evidence = probe.Evidence(requested=3)
        events = []
        probe.produce_samples(
            producer, args(), evidence, events.append,
            clock.now, clock.monotonic, clock.sleep,
        )
        producer.flush(35)
        times = [body(record)["sent_at"] for _, record in producer.records]
        self.assertEqual(times, [
            "2026-10-06T21:00:00.123456Z",
            "2026-10-06T21:00:00.623456Z",
            "2026-10-06T21:00:01.123456Z",
        ])
        self.assertTrue(evidence.complete(0))
        self.assertEqual(evidence.acknowledged_bytes, 3072)
        self.assertEqual(len(events), 6)

    # Scenario: The first enqueue stalls for two seconds.
    # Guarantees: Later probes are rate-spaced, not emitted as a catch-up burst.
    def test_slow_enqueue_does_not_burst(self):
        clock = FakeClock()
        producer = FakeProducer(clock, stall_first=2)
        probe.produce_samples(
            producer, args(), probe.Evidence(3), lambda _: None,
            clock.now, clock.monotonic, clock.sleep,
        )
        times = [datetime.fromisoformat(body(value)["sent_at"].replace("Z", "+00:00"))
                 for _, value in producer.records]
        self.assertGreaterEqual((times[1] - times[0]).total_seconds(), 2.5)
        self.assertGreaterEqual((times[2] - times[1]).total_seconds(), 0.5)

    # Scenario: The producer cannot enqueue a fresh probe because its bounded queue is full.
    # Guarantees: It fails without retrying the same stale payload or claiming an enqueue.
    def test_full_queue_aborts_without_retry(self):
        clock = FakeClock()
        producer = FakeProducer(clock)
        evidence = probe.Evidence(1)
        with patch.object(producer, "produce", side_effect=BufferError("full")) as send:
            with self.assertRaises(BufferError):
                probe.produce_samples(
                    producer, args(1), evidence, lambda _: None,
                    clock.now, clock.monotonic, clock.sleep,
                )
            self.assertEqual(send.call_count, 1)
        self.assertEqual(evidence.enqueued, 0)
        self.assertFalse(evidence.complete(0))

    # Scenario: A Kafka callback reports failure after the first probe is enqueued.
    # Guarantees: Remaining production stops and no successful run is reported.
    def test_delivery_failure_stops_production(self):
        clock = FakeClock()
        producer = FakeProducer(clock, error="delivery timeout")
        evidence = probe.Evidence(3)
        with self.assertRaisesRegex(RuntimeError, "delivery failed"):
            probe.produce_samples(
                producer, args(), evidence, lambda _: None,
                clock.now, clock.monotonic, clock.sleep,
            )
        self.assertEqual(len(producer.records), 1)
        self.assertEqual(evidence.delivery_failed, 1)
        self.assertFalse(evidence.complete(0))

    # Scenario: Source wall time steps 200 ms while monotonic time continues normally.
    # Guarantees: The run rejects the clock discontinuity before sending another probe.
    def test_clock_jump_invalidates_run(self):
        clock = FakeClock()
        producer = FakeProducer(clock)
        evidence = probe.Evidence(3)

        def jumping_sleep(seconds):
            clock.sleep(seconds)
            clock.wall_offset = 0.2

        with self.assertRaisesRegex(RuntimeError, "Wall clock moved"):
            probe.produce_samples(
                producer, args(), evidence, lambda _: None,
                clock.now, clock.monotonic, jumping_sleep,
            )
        self.assertEqual(len(producer.records), 1)
        self.assertGreater(evidence.max_clock_drift_ms, 100)

    # Scenario: Metadata identifies two partitions instead of the configured single partition.
    # Guarantees: Probes do not claim to represent a differently partitioned pipeline.
    def test_partition_change_is_rejected(self):
        producer = FakeProducer()
        metadata = producer.list_topics(15)
        metadata.topics["otel-syslog"].partitions[1] = SimpleNamespace(error=None, leader=1)
        with patch.object(producer, "list_topics", return_value=metadata):
            with self.assertRaisesRegex(RuntimeError, "one-partition"):
                probe.check_topic(producer, "otel-syslog")

    # Scenario: The Python check command sees a healthy one-partition topic.
    # Guarantees: The real command only checks metadata and never enqueues a probe.
    def test_check_command_never_produces(self):
        producer = FakeProducer()
        with patch.object(probe, "make_producer", return_value=producer):
            with patch("sys.argv", ["probe.py", "check"]):
                self.assertEqual(probe.main(), 0)
        self.assertEqual(producer.records, [])
        self.assertEqual(producer.callbacks, [])

    # Scenario: A bounded run completes, then attempts to reuse the same evidence directory.
    # Guarantees: Durable counts and a run-specific query exist; prior evidence is not overwritten.
    def test_run_and_query_evidence(self):
        module = ModuleType("confluent_kafka")
        module.KafkaException = RuntimeError
        with TemporaryDirectory() as directory:
            output = Path(directory)
            with patch.dict("sys.modules", {"confluent_kafka": module}):
                with patch.object(probe, "make_producer", return_value=FakeProducer()):
                    self.assertEqual(probe.run(args(count=1, output=output)), 0)
                    with self.assertRaises(FileExistsError):
                        probe.run(args(count=1, output=output))
            summary = json.loads((output / "producer-summary.json").read_text())
            self.assertTrue(summary["producer_ok"])
            self.assertEqual(summary["acknowledged"], 1)
            for phase in ("before", "after"):
                (output / f"clock-{phase}.txt").write_text("yes\n")
            (output / "producer-exit-code.txt").write_text("0\n")
            self.assertEqual(probe.report(SimpleNamespace(output=output)), 0)
            query = (output / "latency.kql").read_text()
            self.assertIn(RUN_ID, query)
            self.assertIn("let ProducerAndClockOK = true;", query)
            self.assertNotIn("__RUN_ID__", query)
            self.assertIn("percentile(LatencyMs, 99)", query)
            self.assertIn("iff(Complete, ObservedP99Ms, real(null))", query)
            with self.assertRaises(FileExistsError):
                probe.report(SimpleNamespace(output=output))

    # Scenario: Flush leaves a probe unresolved even though enqueue succeeded.
    # Guarantees: The persisted summary and exit code report failure, not successful delivery.
    def test_unresolved_flush_is_invalid(self):
        module = ModuleType("confluent_kafka")
        module.KafkaException = RuntimeError
        with TemporaryDirectory() as directory:
            output = Path(directory)
            with patch.dict("sys.modules", {"confluent_kafka": module}):
                with patch.object(probe, "make_producer", return_value=FakeProducer(pending=1)):
                    self.assertEqual(probe.run(args(count=1, output=output)), 1)
            summary = json.loads((output / "producer-summary.json").read_text())
            self.assertFalse(summary["producer_ok"])
            self.assertEqual(summary["pending_after_flush"], 1)
            self.assertTrue(summary["errors"])

    # Scenario: Host NTP synchronization fails after an otherwise successful producer run.
    # Guarantees: Query generation marks the result invalid and gates the percentile output.
    def test_unsynchronized_clock_gates_query(self):
        summary = {
            "run_id": RUN_ID, "requested": 1200, "acknowledged": 1200,
            "producer_ok": True,
            "source_started_at": probe.timestamp(START),
            "source_finished_at": probe.timestamp(START + timedelta(minutes=10)),
        }
        template = Path(probe.__file__).with_name("latency-template.kql").read_text()
        query, valid = probe.render_query(summary, "yes\n", "no\n", template)
        self.assertFalse(valid)
        self.assertIn("let ProducerAndClockOK = false;", query)
        self.assertNotIn("__", query)

    # Scenario: Docker or the log writer fails after the producer saves a successful summary.
    # Guarantees: The launcher exit status still invalidates the generated percentile query.
    def test_launcher_failure_gates_query(self):
        summary = {
            "run_id": RUN_ID, "requested": 1200, "acknowledged": 1200,
            "producer_ok": True,
            "source_started_at": probe.timestamp(START),
            "source_finished_at": probe.timestamp(START + timedelta(minutes=10)),
        }
        template = Path(probe.__file__).with_name("latency-template.kql").read_text()
        query, valid = probe.render_query(summary, "yes\n", "yes\n", template, 1)
        self.assertFalse(valid)
        self.assertIn("let ProducerAndClockOK = false;", query)

    # Scenario: Producer counts disagree despite a success flag in the input summary.
    # Guarantees: Query generation cannot report a valid run with missing acknowledgments.
    def test_acknowledgment_mismatch_gates_query(self):
        summary = {
            "run_id": RUN_ID, "requested": 1200, "acknowledged": 1199,
            "producer_ok": True,
            "source_started_at": probe.timestamp(START),
            "source_finished_at": probe.timestamp(START + timedelta(minutes=10)),
        }
        template = Path(probe.__file__).with_name("latency-template.kql").read_text()
        _, valid = probe.render_query(summary, "yes\n", "yes\n", template)
        self.assertFalse(valid)


if __name__ == "__main__":
    unittest.main()
