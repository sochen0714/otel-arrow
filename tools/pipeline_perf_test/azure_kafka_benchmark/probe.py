"""Bounded RFC5424 probes for sampled Kafka -> Log Analytics ingestion latency."""

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import signal
import sys
import time
from uuid import UUID


KIND = "kafka-la-probe-v1"
APP = "kbench-probe"
MSG_ID = "LA-LATENCY"
SIZE = 1024
CLOCK_TOLERANCE_SECONDS = 0.1


def utc_now():
    return datetime.now(timezone.utc)


def timestamp(value):
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def make_record(run_id, sequence, sent_at):
    """Stamp each record immediately before enqueue, never from a reused pool."""
    UUID(run_id)
    if sequence < 0 or sent_at.utcoffset() is None:
        raise ValueError("Sequence must be nonnegative and source time timezone-aware")
    sent = timestamp(sent_at)
    header = f"<134>1 {sent} azure-latency-probe {APP} - {MSG_ID} - "
    data = {
        "kind": KIND,
        "run_id": run_id,
        "sequence": sequence,
        "sent_at": sent,
        "padding": "",
    }
    body = json.dumps(data, separators=(",", ":"), ensure_ascii=True)
    padding = SIZE - len((header + body).encode("ascii"))
    if padding < 0:
        raise ValueError("Probe metadata does not fit the 1024-byte record")
    data["padding"] = "x" * padding
    return (header + json.dumps(data, separators=(",", ":"))).encode("ascii")


@dataclass
class Evidence:
    requested: int
    enqueued: int = 0
    acknowledged: int = 0
    acknowledged_bytes: int = 0
    delivery_failed: int = 0
    max_clock_drift_ms: float = 0
    errors: list[str] = field(default_factory=list)

    def require_healthy(self):
        if self.errors:
            raise RuntimeError(self.errors[0])

    def complete(self, pending):
        return (
            not self.errors
            and pending == 0
            and self.delivery_failed == 0
            and self.requested == self.enqueued == self.acknowledged
            and self.acknowledged_bytes == self.requested * SIZE
        )


def produce_samples(
    producer, args, evidence, journal, now=utc_now,
    monotonic=time.monotonic, sleep=time.sleep,
):
    anchor_monotonic = monotonic()
    anchor_utc = now()
    interval = 1.0 / args.rate
    deadline = anchor_monotonic

    for sequence in range(args.count):
        while monotonic() < deadline:
            producer.poll(0)
            evidence.require_healthy()
            sleep(min(0.05, max(0, deadline - monotonic())))
        producer.poll(0)
        evidence.require_healthy()
        enqueue_monotonic = monotonic()
        sent_at = now()
        drift = abs(
            (sent_at - anchor_utc).total_seconds()
            - (enqueue_monotonic - anchor_monotonic)
        )
        evidence.max_clock_drift_ms = max(evidence.max_clock_drift_ms, drift * 1000)
        if drift > CLOCK_TOLERANCE_SECONDS:
            raise RuntimeError("Wall clock moved by more than 100 ms relative to monotonic time")
        payload = make_record(args.run_id, sequence, sent_at)
        sent = timestamp(sent_at)

        def delivered(error, message, seq=sequence, source_sent_at=sent):
            event = {
                "event": "delivery", "sequence": seq, "sent_at": source_sent_at,
                "callback_at": timestamp(now()),
            }
            if error is not None:
                evidence.delivery_failed += 1
                detail = f"Probe {seq} delivery failed: {error}"
                evidence.errors.append(detail)
                event["error"] = str(error)
            else:
                evidence.acknowledged += 1
                evidence.acknowledged_bytes += SIZE
                event.update(partition=message.partition(), offset=message.offset())
            journal(event)

        # A full queue aborts the run rather than retrying with a stale timestamp.
        producer.produce(args.topic, value=payload, on_delivery=delivered)
        evidence.enqueued += 1
        journal({"event": "enqueued", "sequence": sequence, "sent_at": sent, "bytes": SIZE})
        # Do not catch up missed scheduling slots with a burst.
        deadline = monotonic() + interval


def make_producer(args):
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": args.brokers,
        "client.id": "kafka-la-latency-probe",
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "none",
        "linger.ms": 0,
        "queue.buffering.max.messages": 100,
        "queue.buffering.max.kbytes": 1024,
        "message.timeout.ms": 30000,
        "request.timeout.ms": 10000,
        "socket.timeout.ms": 10000,
        "socket.connection.setup.timeout.ms": 10000,
        "allow.auto.create.topics": False,
    })


def check_topic(producer, topic):
    metadata = producer.list_topics(timeout=15)
    description = metadata.topics.get(topic)
    if description is None:
        raise RuntimeError(f"Topic {topic!r} does not exist; no topic will be created")
    if description.error is not None:
        raise RuntimeError(f"Topic metadata error: {description.error}")
    if len(description.partitions) != 1:
        raise RuntimeError("This probe configuration expects the current one-partition topic")
    for partition in description.partitions.values():
        if partition.error is not None or partition.leader < 0:
            raise RuntimeError("Topic partition is unavailable")


def save_json(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")


def run(args):
    from confluent_kafka import KafkaException

    UUID(args.run_id)
    if not (0 < args.rate <= 10 and 0 < args.count <= 100000):
        raise ValueError("Use a positive rate <= 10 probes/s and count <= 100000")
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    # Refuse to mix evidence from multiple invocations.
    events_path = output / "events.jsonl"
    with events_path.open("x", encoding="utf-8") as events:
        evidence = Evidence(requested=args.count)
        started = utc_now()
        producer = None
        pending = None

        def journal(event):
            events.write(json.dumps(event, separators=(",", ":")) + "\n")
            events.flush()

        def interrupt(_signum, _frame):
            raise KeyboardInterrupt("Probe run interrupted")

        old_handler = signal.signal(signal.SIGTERM, interrupt)
        try:
            producer = make_producer(args)
            check_topic(producer, args.topic)
            produce_samples(producer, args, evidence, journal)
        except (KafkaException, RuntimeError, ValueError, OSError, BufferError, KeyboardInterrupt) as error:
            evidence.errors.append(f"{type(error).__name__}: {error}")
            print(evidence.errors[-1], file=sys.stderr, flush=True)
        finally:
            signal.signal(signal.SIGTERM, old_handler)
            if producer is not None:
                try:
                    pending = producer.flush(35)
                    if pending:
                        evidence.errors.append(f"{pending} probe deliveries unresolved after flush")
                except (KafkaException, RuntimeError, OSError, KeyboardInterrupt) as error:
                    evidence.errors.append(f"Flush failed: {type(error).__name__}: {error}")
            result = {
                "run_id": args.run_id, "kind": KIND,
                "source_started_at": timestamp(started),
                "source_finished_at": timestamp(utc_now()),
                "rate_probes_per_second": args.rate,
                "record_size_bytes": SIZE, "topic": args.topic,
                **vars(evidence),
                "pending_after_flush": pending,
                "producer_ok": evidence.complete(pending),
                "measurement": "sampled source enqueue attempt -> approximate LA ingestion time",
                "note": "Broker acknowledgment is NOT evidence of Log Analytics ingestion.",
            }
            save_json(output / "producer-summary.json", result)
            print(json.dumps(result, indent=2), flush=True)
    return 0 if result["producer_ok"] else 1


def render_query(summary, clock_before, clock_after, template, producer_exit_code=0):
    run_id = str(UUID(summary["run_id"]))
    expected = int(summary["requested"])
    acknowledged = int(summary["acknowledged"])
    if expected <= 0 or not 0 <= acknowledged <= expected:
        raise ValueError("Invalid producer counts")
    times = {}
    for name in ("source_started_at", "source_finished_at"):
        parsed = datetime.fromisoformat(summary[name].replace("Z", "+00:00"))
        if parsed.utcoffset() is None:
            raise ValueError("Source timestamps must be timezone-aware")
        times[name] = timestamp(parsed)
    clock_ok = clock_before.strip() == clock_after.strip() == "yes"
    verified = (
        summary["producer_ok"] is True
        and expected == acknowledged
        and clock_ok
        and producer_exit_code == 0
    )
    replacements = {
        "__RUN_ID__": run_id,
        "__EXPECTED__": str(expected),
        "__ACKNOWLEDGED__": str(acknowledged),
        "__PRODUCER_AND_CLOCK_OK__": str(verified).lower(),
        "__STARTED__": times["source_started_at"],
        "__FINISHED__": times["source_finished_at"],
    }
    for key, value in replacements.items():
        if key not in template:
            raise ValueError(f"Missing query placeholder {key}")
        template = template.replace(key, value)
    return template, verified


def report(args):
    summary = json.loads((args.output / "producer-summary.json").read_text(encoding="utf-8"))
    query, verified = render_query(
        summary,
        (args.output / "clock-before.txt").read_text(encoding="utf-8"),
        (args.output / "clock-after.txt").read_text(encoding="utf-8"),
        Path(__file__).with_name("latency-template.kql").read_text(encoding="utf-8"),
        int((args.output / "producer-exit-code.txt").read_text(encoding="utf-8")),
    )
    with (args.output / "latency.kql").open("x", encoding="utf-8") as stream:
        stream.write(query)
    print(f"LA query written: {args.output / 'latency.kql'}")
    if not verified:
        print("INVALID producer/clock evidence: query will suppress percentiles.", file=sys.stderr)
        return 1
    print("Producer/clock evidence ready. LA delivery and latency are NOT verified yet.")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("check", "run"):
        subparser = commands.add_parser(name)
        subparser.add_argument("--brokers", default="kafka-broker:9092")
        subparser.add_argument("--topic", default="otel-syslog")
        if name == "run":
            subparser.add_argument("--run-id", required=True)
            subparser.add_argument("--output", type=Path, required=True)
            subparser.add_argument("--count", type=int, default=1200)
            subparser.add_argument("--rate", type=float, default=2)
    query = commands.add_parser("report")
    query.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "run":
        return run(args)
    if args.command == "report":
        return report(args)
    check_topic(make_producer(args), args.topic)
    print("Kafka topic ready; metadata checked, no probes sent.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
