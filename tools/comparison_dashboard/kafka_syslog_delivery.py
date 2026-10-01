"""Check raw Syslog/Kafka final delivery to a matching OTLP or OTAP Perf backend."""

import argparse
from pathlib import Path
import re

if __package__:
    from .kafka_syslog_metrics import counter, read_prometheus
    from .kafka_syslog_receiver_only import count, PRODUCER_ERRORS, write_json
else:
    from kafka_syslog_metrics import counter, read_prometheus
    from kafka_syslog_receiver_only import count, PRODUCER_ERRORS, write_json


BATCH_ERRORS = (
    "dropped_conversion_total", "batching_errors_total",
    "nacked_inbound_slots_total", "nacked_outbound_slots_total",
)


def node_samples(samples, node, core):
    return [
        (name, tags, value) for name, tags, value in samples
        if tags.get("otel_scope_node_id") == node
        and tags.get("otel_scope_core_id") == core
        and tags.get("otel_scope_pipeline_group_id") == "default"
        and tags.get("otel_scope_pipeline_id") == "main"
    ]


def verify(output, protocol, expect_full_delivery=False):
    result_path = output / "verified-delivery.json"
    result_path.unlink(missing_ok=True)
    if protocol not in ("otlp", "otap"):
        raise ValueError(f"Unsupported output protocol: {protocol}")
    producer = read_prometheus(output / "producer-final.prom")
    backend = node_samples(
        read_prometheus(output / "backend-final.prom"), "perf", "2",
    )
    consumer = read_prometheus(output / "consumer-before-shutdown.prom")
    producer_health = {name: count(producer, name) for name in PRODUCER_ERRORS}
    for name, value in producer_health.items():
        if value:
            raise ValueError(f"Producer reported {name}: {value}")
    produced = count(producer, "logs_produced")
    if produced <= 0 or count(producer, "bytes_sent") != produced * 1024:
        raise ValueError("Producer record count/size does not match raw Syslog")
    received = count(
        backend, "items_total", otel_scope_name="node.input",
        signal="logs", outcome="success",
    )
    if not 0 < received <= produced:
        raise ValueError("Backend count is empty or exceeds broker-confirmed logs")
    if expect_full_delivery and received != produced:
        raise ValueError(f"Incomplete smoke delivery: sent={produced}, got={received}")

    batch = node_samples(consumer, "batch", "1")
    batch_health = {
        name: count(batch, name, otel_scope_name="otap.processor.batch")
        for name in BATCH_ERRORS
    }
    for name, value in batch_health.items():
        if value:
            raise ValueError(f"Batch processor reported {name}: {value}")
    pending_max = counter(
        batch, "flush_pending_requests_max", otel_scope_name="otap.processor.batch",
    )
    if not pending_max.is_integer() or pending_max > 1000:
        raise ValueError("Observed batch input count exceeds the configured bound")
    exporter = node_samples(consumer, "exporter", "1")
    exported = count(
        exporter, "messages_total", otel_scope_name="exporter.exports",
        signal="logs", outcome="success",
    )
    if exported <= 0:
        raise ValueError("No successful exporter requests before shutdown")
    failure_series = []
    for name, tags, value in batch + exporter + backend:
        if (name.endswith("_total")
                and tags.get("outcome") in ("failure", "refused", "error")):
            if count([(name, tags, value)], name):
                raise ValueError(f"Pipeline reported {name} {tags}: {value}")
            failure_series.append({"name": name, "labels": tags, "value": value})

    sample = (output / "kafka-record.txt").read_bytes()
    if (len(sample) != 1025 or not sample.endswith(b"\n")
            or re.match(rb"<\d{1,3}>1 ", sample) is None):
        raise ValueError("Kafka sample is not one 1024-byte RFC 5424 value")
    images = {}
    for line in (output / "images.txt").read_text().splitlines():
        match = re.fullmatch(
            r"/(load-generator|kafka-broker|kafka-consumer|backend-service) "
            r"(sha256:[0-9a-f]{64})", line,
        )
        if not match or match[1] in images:
            raise ValueError("Invalid or duplicate container image evidence")
        images[match[1]] = match[2]
    if len(images) != 4:
        raise ValueError("Expected image evidence for exactly four containers")
    if images["kafka-consumer"] != images["backend-service"]:
        raise ValueError("Consumer and backend must use the same engine image")
    for filename in ("kafka-consumer-config.rendered.yaml",
                     "backend-config.rendered.yaml"):
        if not (output / filename).read_text().strip():
            raise ValueError(f"Missing rendered config evidence: {filename}")
    result = {
        "output_protocol": protocol,
        "producer_logs": produced,
        "backend_logs": received,
        "not_observed_at_backend": produced - received,
        "full_delivery_required": expect_full_delivery,
        "producer_health": producer_health,
        "batch_health_before_shutdown": batch_health,
        "batch_pending_requests_max": pending_max,
        "exporter_successful_requests_before_shutdown": exported,
        "observed_failure_series": failure_series,
        "images": images,
        "note": (
            "Backend Perf success logs are captured AFTER consumer shutdown and "
            "BEFORE backend shutdown. Consumer health is pre-shutdown only. "
            "Unemitted outcome series are unavailable, not fabricated zeros. "
            "Counts are broker-confirmed input records and Perf-received logs, "
            "not an identity/deduplication check. A bounded deficit is not proven "
            "loss or a guarantee of complete Kafka drain."
        ),
    }
    write_json(result_path, result)
    print(f"Verified {protocol.upper()}: {received}/{produced} backend logs.")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--protocol", choices=("otlp", "otap"), required=True)
    parser.add_argument("--expect-full-delivery", action="store_true")
    args = parser.parse_args()
    verify(args.output_dir, args.protocol, args.expect_full_delivery)


if __name__ == "__main__":
    main()
