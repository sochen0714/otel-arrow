"""Offline matched protocol configuration, final delivery and SQL report checks."""

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

import duckdb
import yaml

from tools.comparison_dashboard import dashboard, kafka_syslog_delivery as delivery
from tools.comparison_dashboard.tests import test_kafka_syslog_receiver_only as local


DASHBOARD = local.DASHBOARD
PERF_TEST = local.PERF_TEST
REPORT = (PERF_TEST / "test_suites" / "comparison_dashboard" / "reports"
          / "report_logs.yaml")
RATES = [1000, 2000, 5000, 10000, 20000, 100000]


class ProtocolSuiteTests(unittest.TestCase):
    render = staticmethod(local.ReceiverOnlySuiteTests.render)
    steps = local.ReceiverOnlySuiteTests.steps

    def setUp(self):
        previous = Path.cwd()
        os.chdir(DASHBOARD)
        self.addCleanup(os.chdir, previous)
        self.manifest = dashboard.load_manifest(Path("manifest.yaml"))
        self.variants = {}
        for protocol in ("otlp", "otap"):
            suite = dashboard.load_suite(Path(
                f"suites/dfe/dfe-logs-kafka-syslog-{protocol}-recv-baseline.yaml"
            ))
            context = dashboard.build_template_context(
                suite, self.manifest, DASHBOARD / ".data/protocol-render-test", 20,
            )
            self.variants[protocol] = (
                suite, self.render(suite["orchestrator_template"], context),
            )

    def engines(self, steps):
        result = []
        for name in ("Deploy Backend Engine", "Deploy Kafka Consumer"):
            step = next(s for s in steps if s["name"] == name)
            spec = step["hooks"]["run"]["pre"][0]["render_template"]
            result.append(self.render(spec["template_path"], spec["variables"]))
        return result

    # Scenario: Both protocols render six rate cases with all real plugins loaded.
    # Guarantees: Nested actions, hooks, reports and template paths parse offline.
    def test_full_orchestrator_schema(self):
        sys.path.insert(0, str(PERF_TEST / "orchestrator"))
        self.addCleanup(sys.path.pop, 0)
        from lib.impl import actions, strategies  # noqa: F401
        from lib.runner.schema.loader import load_config_from_string
        from lib.impl.strategies.hooks.reporting.sql_report import SQLReportDetails

        for protocol, (_, rendered) in self.variants.items():
            with self.subTest(protocol=protocol):
                config = load_config_from_string(yaml.safe_dump(rendered))
                self.assertEqual([t.name for t in config.tests],
                                 [f"{rate // 1000}k" for rate in RATES])
                self.assertTrue(all(t.steps for t in config.tests))
                for test in rendered["tests"]:
                    self.engines(self.steps(test))
        SQLReportDetails.model_validate(yaml.safe_load(REPORT.read_text()))

    # Scenario: Matched suites differ only in exporter and matching backend input.
    # Guarantees: Images, raw Kafka input, cores, batching and timing stay matched.
    def test_only_output_protocol_differs(self):
        _, otlp = self.variants["otlp"]
        _, otap = self.variants["otap"]
        self.assertEqual(otlp["components"], otap["components"])
        self.assertEqual(set(otlp["components"]), {
            "load-generator", "kafka-broker", "kafka-consumer", "backend-service",
        })
        images = {k: v["deployment"]["docker"]["image"]
                  for k, v in otlp["components"].items()}
        self.assertEqual(images, {
            "load-generator": "load_generator:kafka-syslog",
            "kafka-broker": "apache/kafka:latest",
            "kafka-consumer": "df_engine:latest",
            "backend-service": "df_engine:latest",
        })
        for component in otlp["components"].values():
            docker = component["deployment"]["docker"]
            self.assertEqual(docker["network"], "kafka-syslog-benchmark")
            self.assertTrue(all(p["host_ip"] == "127.0.0.1"
                                for p in docker["ports"]))
        self.assertIn("refusing to replace", yaml.safe_dump(otlp["hooks"]))
        self.assertEqual([t["name"] for t in otlp["tests"]],
                         [t["name"] for t in otap["tests"]])
        for left, right, rate in zip(otlp["tests"], otap["tests"], RATES):
            consumers, backends, producers, timings = [], [], [], []
            for protocol, test in (("otlp", left), ("otap", right)):
                with self.subTest(protocol=protocol, rate=rate):
                    steps = self.steps(test)
                    backend, consumer = self.engines(steps)
                    for engine, core in ((consumer, 1), (backend, 2)):
                        self.assertEqual(
                            engine["policies"]["resources"]["core_allocation"]["set"],
                            [{"start": core, "end": core}],
                        )
                        self.assertEqual(engine["policies"]["telemetry"],
                                         {"runtime_metrics": "normal"})
                    pipeline = consumer["groups"]["default"]["pipelines"]["main"]
                    self.assertEqual(set(pipeline["nodes"]),
                                     {"receiver", "batch", "exporter"})
                    self.assertEqual(pipeline["connections"], [
                        {"from": "receiver", "to": "batch"},
                        {"from": "batch", "to": "exporter"},
                    ])
                    nodes = pipeline["nodes"]
                    self.assertEqual(nodes["receiver"]["config"]["logs"],
                                     {"topics": ["otel-syslog"], "encoding": "syslog"})
                    self.assertEqual(nodes["batch"]["config"], {
                        "format": "otap", "max_batch_duration": "200ms",
                        "otap": {"sizer": "items", "min_size": 1000, "max_size": 1000},
                    })
                    exporter = nodes.pop("exporter")
                    expected = "otlp_grpc" if protocol == "otlp" else "otap"
                    self.assertEqual(exporter["type"], f"urn:otel:exporter:{expected}")
                    config = {"grpc_endpoint": "http://backend-service:1235"}
                    if protocol == "otap":
                        config.update(compression_method="none", streams_per_signal=5,
                                      arrow={"payload_compression": "none"})
                    else:
                        config["compression_method"] = None
                    self.assertEqual(exporter["config"], config)
                    nodes = backend["groups"]["default"]["pipelines"]["main"]["nodes"]
                    self.assertEqual(nodes.pop("receiver")["type"],
                                     f"urn:otel:receiver:{protocol}")
                    self.assertEqual(nodes["perf"]["type"], "urn:otel:exporter:perf")
                    self.assertTrue(
                        nodes["perf"]["policies"]["telemetry"]["item_counts"],
                    )
                    consumers.append(consumer)
                    backends.append(backend)
                    start = next(s for s in steps
                                 if s["name"] == "Start Raw Syslog Producer")
                    payload = start["hooks"]["run"]["pre"][0]["send_http_request"]
                    self.assertEqual(payload["payload"], {
                        "load_type": "syslog", "syslog_transport": "kafka",
                        "syslog_format": "rfc5424", "syslog_content_type": "random",
                        "kafka_brokers": "kafka-broker:9092",
                        "kafka_topic": "otel-syslog", "target_rate": rate,
                        "threads": 1, "batch_size": 100,
                        "body_size": 1024, "message_size": 1024,
                    })
                    producers.append(payload)
                    timings.append([s["action"]["wait"] for s in steps
                                    if "wait" in s["action"]])
                    topic = next(s for s in steps if s["name"] == "Create Syslog Topic")
                    command = topic["hooks"]["run"]["pre"][0]["run_command"]["command"]
                    self.assertIn("--topic otel-syslog --partitions 1 "
                                  "--replication-factor 1", command)
            self.assertEqual(consumers[0], consumers[1])
            self.assertEqual(backends[0], backends[1])
            self.assertEqual(producers[0], producers[1])
            self.assertEqual(timings[0], timings[1])

    # Scenario: Producer stops and the consumer admin exits before backend shutdown.
    # Guarantees: Health is pre-shutdown, final totals post-shutdown, and 1k is gated.
    def test_capture_order_and_report_gate(self):
        for protocol, (_, config) in self.variants.items():
            for test in config["tests"]:
                steps = self.steps(test)
                names = [s["name"] for s in steps]
                ordered = [
                    "Warm Up", "Observe Load", "Stop and Flush Producer",
                    "Bounded Consumer Drain", "Stop Kafka Consumer",
                    "Capture Final Backend Counters",
                    "Capture Kafka Record and Verify Delivery", "Stop Backend",
                    "Stop Monitoring All", "Destroy All", "Run Report",
                ]
                self.assertEqual(sorted(ordered, key=names.index), ordered)
                for name, seconds in (("Warm Up", 10), ("Observe Load", 20),
                                      ("Bounded Consumer Drain", 10)):
                    self.assertEqual(steps[names.index(name)]["action"]["wait"],
                                     {"delay_seconds": seconds})
                stop = steps[names.index("Stop Kafka Consumer")]["hooks"]["run"]["pre"]
                self.assertIn("consumer-before-shutdown.prom",
                              stop[0]["run_command"]["command"])
                self.assertIn("timeout_secs=15", stop[1]["send_http_request"]["url"])
                capture = steps[names.index("Capture Final Backend Counters")]
                self.assertIn("backend-final.prom", yaml.safe_dump(capture))
                verify = steps[names.index("Capture Kafka Record and Verify Delivery")]
                command = verify["hooks"]["run"]["pre"][-1]["run_command"]["command"]
                self.assertIn(f"--protocol {protocol}", command)
                self.assertNotIn("\n", command)
                self.assertEqual("--expect-full-delivery" in command,
                                 test["name"] == "1k")
                report = steps[names.index("Run Report")]
                self.assertEqual(
                    report["hooks"]["run"]["pre"][0]["run_command"]["command"],
                    command,
                )

    # Scenario: A fresh dashboard registers only the two matched protocol suites.
    # Guarantees: Comparison cases match suite rates with no omitted experiments.
    def test_comparison_registration(self):
        path = Path("comparisons/kafka_receiver_syslog_otlp_otap.yaml")
        self.assertIn(path.resolve(), self.manifest.comparison_files)
        comparison = yaml.safe_load(path.read_text())
        self.assertEqual([t["loadgen_rate"] for t in comparison["tests"]], RATES)
        self.assertEqual({s["slug"] for s in comparison["suites"]},
                         {s["slug"] for s, _ in self.variants.values()})
        for protocol, (suite, config) in self.variants.items():
            self.assertEqual(suite["variables"]["rates"], RATES)
            self.assertEqual(suite["meta"]["protocols"], ["syslog", protocol])
            self.assertEqual([t["name"] for t in config["tests"]],
                             [t["name"] for t in comparison["tests"]])
            self.assertNotIn("topology", yaml.safe_dump(config))
            self.assertNotIn("otlp_http", yaml.safe_dump(config))


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.output = Path(directory.name)

    @staticmethod
    def sample(name, value, node, scope, core=1, extra=""):
        return (
            f'{name}{{otel_scope_node_id="{node}",otel_scope_core_id="{core}",'
            'otel_scope_pipeline_group_id="default",otel_scope_pipeline_id="main",'
            f'otel_scope_name="{scope}"{extra}}} {value}\n'
        )

    def fixture(self, received=1000):
        (self.output / "verified-delivery.json").unlink(missing_ok=True)
        (self.output / "producer-final.prom").write_text(
            "logs_produced 1000\nbytes_sent 1024000\nfailed 0\nkafka_pending 0\n"
            "kafka_delivery_failed 0\nkafka_enqueue_failed 0\nkafka_flush_timeouts 0\n"
        )
        (self.output / "backend-final.prom").write_text(self.sample(
            "items_total", received, "perf", "node.input", 2,
            ',signal="logs",outcome="success"',
        ))
        batch = "".join(self.sample(name, 0, "batch", "otap.processor.batch")
                        for name in delivery.BATCH_ERRORS)
        batch += self.sample("flush_pending_requests_max", 1000, "batch",
                             "otap.processor.batch")
        batch += self.sample("messages_total", 1, "exporter", "exporter.exports",
                             extra=',signal="logs",outcome="success"')
        (self.output / "consumer-before-shutdown.prom").write_text(batch)
        header = b"<134>1 2026-01-01T00:00:00Z host app - - - "
        (self.output / "kafka-record.txt").write_bytes(
            header + b"x" * (1024 - len(header)) + b"\n"
        )
        (self.output / "images.txt").write_text("\n".join(
            f"/{name} sha256:{'a' * 64}" for name in (
                "load-generator", "kafka-broker", "kafka-consumer", "backend-service",
            )
        ) + "\n")
        for name in ("kafka-consumer-config", "backend-config"):
            (self.output / f"{name}.rendered.yaml").write_text(
                "version: otel_dataflow/v1\n"
            )

    # Scenario: Both protocols deliver every broker-confirmed record at final cutoff.
    # Guarantees: Integral exact counts and observed health/image evidence persist.
    def test_complete_delivery(self):
        for protocol in ("otlp", "otap"):
            with self.subTest(protocol=protocol):
                self.fixture()
                result = delivery.verify(self.output, protocol, True)
                self.assertEqual(result["backend_logs"], 1000)
                self.assertIsInstance(result["backend_logs"], int)
                self.assertEqual(result["not_observed_at_backend"], 0)
                self.assertEqual(result["output_protocol"], protocol)
                self.assertEqual(result["observed_failure_series"], [])
                self.assertEqual(len(result["images"]), 4)
                self.assertEqual(json.loads(
                    (self.output / "verified-delivery.json").read_text()
                ), result)

    # Scenario: A bounded high-rate drain leaves logs unobserved at the backend.
    # Guarantees: A deficit is recorded for overload, but full-delivery smoke fails.
    def test_bounded_deficit_and_stale_success(self):
        self.fixture(900)
        result = delivery.verify(self.output, "otap")
        self.assertEqual(result["not_observed_at_backend"], 100)
        with self.assertRaisesRegex(ValueError, "Incomplete smoke delivery"):
            delivery.verify(self.output, "otap", True)
        self.assertFalse((self.output / "verified-delivery.json").exists())

    # Scenario: Required evidence files or individual health/count series are absent.
    # Guarantees: Missing evidence fails explicitly rather than becoming measured zero.
    def test_missing_evidence_fails(self):
        self.fixture()
        files = [p.name for p in self.output.iterdir()]
        for filename in files:
            with self.subTest(filename=filename):
                self.fixture()
                (self.output / filename).unlink()
                with self.assertRaises(FileNotFoundError):
                    delivery.verify(self.output, "otlp")
                self.assertFalse((self.output / "verified-delivery.json").exists())
        required = {
            "producer-final.prom": (*delivery.PRODUCER_ERRORS,
                                    "logs_produced", "bytes_sent"),
            "consumer-before-shutdown.prom": (
                *delivery.BATCH_ERRORS, "messages_total", "flush_pending_requests_max",
            ),
            "backend-final.prom": ("items_total",),
        }
        for filename, names in required.items():
            for name in names:
                with self.subTest(filename=filename, counter=name):
                    self.fixture()
                    path = self.output / filename
                    path.write_text("".join(
                        line for line in path.read_text().splitlines(keepends=True)
                        if not line.startswith(name)
                    ))
                    with self.assertRaises(ValueError):
                        delivery.verify(self.output, "otap")
                    self.assertFalse((self.output / "verified-delivery.json").exists())

    # Scenario: Counts, health, wire sample or provenance are malformed or invalid.
    # Guarantees: Fractional/nonfinite/negative totals and observed failures never pass.
    def test_invalid_evidence_fails(self):
        changes = [
            ("producer-final.prom", "kafka_pending 0", "kafka_pending 1"),
            ("producer-final.prom", "bytes_sent 1024000", "bytes_sent 1024"),
            ("producer-final.prom", "logs_produced 1000", "logs_produced 1000.5"),
            ("backend-final.prom", "} 1000", "} 1001"),
            ("backend-final.prom", "} 1000", "} 999.5"),
            ("backend-final.prom", "} 1000", "} 0"),
            ("backend-final.prom", "} 1000", "} -1"),
            ("backend-final.prom", "} 1000", "} nan"),
            ("backend-final.prom", "} 1000", "} inf"),
            ("backend-final.prom", 'outcome="success"', 'outcome="failure"'),
            ("backend-final.prom", 'otel_scope_core_id="2"', 'otel_scope_core_id="1"'),
            ("consumer-before-shutdown.prom", "} 0", "} 1"),
            ("consumer-before-shutdown.prom", "} 1000", "} 1001"),
            ("consumer-before-shutdown.prom", 'outcome="success"', 'outcome="refused"'),
            ("kafka-record.txt", "<134>1 ", "invalid "),
            ("kafka-record.txt", "<134>1 ", "<134>1  "),
            ("images.txt", "/backend-service", "/unknown"),
            ("images.txt", "sha256:", "missing:"),
            ("backend-config.rendered.yaml", "version: otel_dataflow/v1\n", ""),
        ]
        for filename, original, invalid in changes:
            with self.subTest(filename=filename, invalid=invalid):
                self.fixture()
                path = self.output / filename
                path.write_text(path.read_text().replace(original, invalid))
                with self.assertRaises(ValueError):
                    delivery.verify(self.output, "otlp")
                self.assertFalse((self.output / "verified-delivery.json").exists())

    # Scenario: Conditional failure series appear for exporter, batch or backend Perf.
    # Guarantees: Any nonzero or invalid failure/refused/error value fails verification.
    def test_failure_outcomes(self):
        for node in ("exporter", "batch", "perf"):
            filename = ("backend-final.prom" if node == "perf"
                        else "consumer-before-shutdown.prom")
            for outcome in ("failure", "refused", "error"):
                for value in (1, "nan", "inf", -1, 0.5):
                    with self.subTest(node=node, outcome=outcome, value=value):
                        self.fixture()
                        with (self.output / filename).open("a") as stream:
                            stream.write(self.sample(
                                "messages_total", value, node, "node.input",
                                2 if node == "perf" else 1,
                                f',signal="logs",outcome="{outcome}"',
                            ))
                        with self.assertRaises(ValueError):
                            delivery.verify(self.output, "otlp")

    # Scenario: Unrelated pipelines/signals expose large counts or failure outcomes.
    # Guarantees: Only core-2 default/main Perf log successes contribute to delivery.
    def test_endpoint_isolation(self):
        self.fixture()
        with (self.output / "backend-final.prom").open("a") as stream:
            stream.write(self.sample(
                "items_total", 999999, "perf", "node.input", 1,
                ',signal="logs",outcome="success"',
            ))
            stream.write(self.sample(
                "items_total", 999999, "perf", "node.input", 2,
                ',signal="metrics",outcome="success"',
            ))
        result = delivery.verify(self.output, "otlp", True)
        self.assertEqual(result["backend_logs"], 1000)

    # Scenario: An unsupported protocol is requested against otherwise valid evidence.
    # Guarantees: The verifier cannot label a non-OTLP/OTAP run as matched delivery.
    def test_unsupported_protocol(self):
        self.fixture()
        with self.assertRaisesRegex(ValueError, "Unsupported output protocol"):
            delivery.verify(self.output, "http")


class PerfReportTests(unittest.TestCase):
    def report(self, received=True):
        db = duckdb.connect()
        self.addCleanup(db.close)
        db.execute("CREATE TABLE metadata (Attribute VARCHAR)")
        db.execute('CREATE TABLE metadata_row ("test.name" VARCHAR, '
                   '"test.suite" VARCHAR, "test.start" VARCHAR)')
        db.execute("INSERT INTO metadata_row VALUES "
                   "('smoke','test-suite','2026-01-01T00:00:00Z')")
        db.execute('CREATE TABLE events ("attributes.test.name" VARCHAR, '
                   "name VARCHAR, timestamp BIGINT)")
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        for name, second in (("observation_start", 1), ("observation_stop", 11)):
            db.execute("INSERT INTO events VALUES (?,?,?)",
                       ["smoke", name, int((base.timestamp() + second) * 1e9)])
        labels = ("component_name", "otel_scope_core_id", "otel_scope_name",
                  "otel_scope_node_id", "signal", "outcome")
        db.execute("CREATE TABLE metrics (timestamp TIMESTAMPTZ, metric_name VARCHAR, "
                   "value DOUBLE, " + ", ".join(
                       f'"metric_attributes.{label}" VARCHAR' for label in labels
                   ) + ")")
        for second in range(12):
            rows = [
                ("logs_produced", 1000 * second, "load-generator",
                 None, None, None, None, None),
                ("container.cpu.usage", 0.5, "kafka-consumer",
                 None, None, None, None, None),
                ("container.cpu.allocated", 1, "kafka-consumer",
                 None, None, None, None, None),
                ("container.cpu.usage", 0.9, "backend-service",
                 None, None, None, None, None),
                ("items", 900 * second, "backend-service", "2",
                 "node.input", "perf", "logs", "success"),
                ("items", 999999 * second, "backend-service", "2",
                 "node.input", "perf", "logs", "failure"),
                ("items", 999999 * second, "backend-service", "2",
                 "node.input", "perf", "metrics", "success"),
                ("messages", 999999 * second, "backend-service", "2",
                 "node.input", "perf", "logs", "success"),
                ("items", 999999 * second, "kafka-consumer", "1",
                 "node.input", "batch", "logs", "success"),
            ]
            for row in rows:
                if not received and row[0] == "items" and row[1] == 900 * second:
                    continue
                db.execute("INSERT INTO metrics VALUES (?,?,?,?,?,?,?,?,?)",
                           [base + timedelta(seconds=second), *row])
        for query in yaml.safe_load(REPORT.read_text())["queries"]:
            db.execute(query["sql"])
        return db.execute("SELECT * FROM gh_actions_benchmark").fetchdf()

    # Scenario: Either matching backend exposes successful Perf log item counters.
    # Guarantees: SQL rates exclude batch/message/failure counters and use consumer CPU.
    def test_perf_counter_rate(self):
        frame = self.report()
        values = dict(zip(frame["name"], frame["value"]))
        self.assertAlmostEqual(values["logs_produced_rate"], 1000)
        self.assertAlmostEqual(values["logs_received_rate"], 900)
        self.assertAlmostEqual(values["cpu_percentage_normalized_avg"], 50)

    # Scenario: Prometheus observation contains no successful backend Perf log series.
    # Guarantees: The generic report gate rejects the missing rate instead of zero.
    def test_missing_perf_counter_fails_reporting(self):
        sys.path.insert(0, str(PERF_TEST / "orchestrator"))
        self.addCleanup(sys.path.pop, 0)
        from lib.impl.strategies.hooks.reporting.sql_report import (
            ResultTable, SQLReportHook,
        )

        report = yaml.safe_load(REPORT.read_text())
        table = ResultTable.model_validate(next(
            t for t in report["result_tables"] if t["name"] == "gh_actions_benchmark"
        ))
        frame = self.report(received=False)
        self.assertNotIn("logs_received_rate", set(frame["name"]))
        with self.assertRaisesRegex(ValueError, "logs_received_rate"):
            SQLReportHook._assert_required_values(table, frame)


if __name__ == "__main__":
    unittest.main()
