# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import copy
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import yaml

from translate import (
    ConfigError,
    duration_ms,
    load_yaml,
    runtime_environment,
    translate,
    validate_with_image,
    write_config,
)


HERE = Path(__file__).resolve().parent


class TranslationTests(unittest.TestCase):
    def setUp(self):
        self.customer = load_yaml(HERE / "customer.yaml")
        self.bindings = load_yaml(HERE / "bindings.yaml")

    def convert(self, **options):
        policy = dict(instance_id="consumer-a", membership="dynamic",
                      rebalance_strategy="cooperative_sticky", auth_policy="sasl-only")
        policy.update(options)
        return translate(self.customer, self.bindings, **policy)

    def native(self, **options):
        return self.convert(**options)["groups"]["default"]["pipelines"]["main"]["nodes"]["kafka-local"]["config"]

    # Scenario: The proposed document's authenticated example is translated.
    # Guarantees: Customer defaults, broker arrays, secrets and lag map to native fields.
    def test_example_mapping(self):
        self.customer["kafka"]["brokers"].append("[::1]:9093")
        n = self.native()
        self.assertEqual(n["brokers"], "kafka:9093,[::1]:9093")
        self.assertEqual(n["group_id"], "schema-demo-group")
        self.assertEqual(n["client_id"], "kafka-local-consumer-a")
        self.assertEqual(n["commit"], {"mode": "manual"})
        self.assertEqual(n["auto_offset_reset"], "latest")
        self.assertFalse(n["enable_idempotency"])
        self.assertEqual(n["isolation_level"], "read_uncommitted")
        self.assertEqual(n["lag_refresh_interval_ms"], 5000)
        self.assertEqual(n["logs"], {"topics": ["schema-demo-logs"], "encoding": "otlp_proto"})
        self.assertEqual(n["tls"], {"ca_file": "/home/nonroot/certs/ca.crt"})
        self.assertEqual(n["auth"]["sasl"]["username"], "${env:KAFKA_DEMO_USERNAME__JSON}")
        self.assertEqual(n["auth"]["sasl"]["password"], "${env:KAFKA_DEMO_PASSWORD__JSON}")
        self.assertNotIn("group_instance_id", n)

    # Scenario: Multiple processes translate the same logical source.
    # Guarantees: Group identity is shared; client and optional static identities differ.
    def test_member_identity_and_assignment_are_platform_inputs(self):
        a = self.native(membership="static")
        b = self.native(membership="static", instance_id="consumer-b")
        self.assertEqual(a["group_id"], b["group_id"])
        self.assertNotEqual(a["client_id"], b["client_id"])
        self.assertNotEqual(a["group_instance_id"], b["group_instance_id"])
        for strategy in ("range", "round_robin", "cooperative_sticky"):
            with self.subTest(strategy=strategy):
                self.assertEqual(self.native(rebalance_strategy=strategy)["rebalance_strategy"], strategy)
        self.assertNotIn("rebalance_strategy", self.native(rebalance_strategy="native-default"))

    # Scenario: All three signals use independent encoding, exclusions and header conversion.
    # Guarantees: Nested customer fields become exact native per-signal and header settings.
    def test_complete_signal_delivery_and_enrichment_mapping(self):
        k = self.customer["kafka"]
        k["subscription"]["signals"] = {
            "traces": {"topics": ["otlp-traces"]},
            "metrics": {"topics": ["otlp-metrics"], "encoding": "otapProto"},
            "logs": {"topics": ["^otlp-logs-.*"], "excludeTopics": ["^otlp-logs-test-.*"]},
        }
        k["delivery"]["readCommitted"] = True
        k["enrichment"] = {"resourceAttributesFromHeaders": [
            {"header": f"x-{kind}", "attribute": f"attr.{kind}", "type": kind}
            for kind in ("string", "bool", "int", "float")
        ]}
        n = self.native()
        self.assertEqual(n["traces"]["encoding"], "otlp_proto")
        self.assertEqual(n["metrics"]["encoding"], "otap_proto")
        self.assertEqual(n["logs"]["exclude_topics"], ["^otlp-logs-test-.*"])
        self.assertEqual(n["isolation_level"], "read_committed")
        self.assertEqual(n["resource_attrs_from_headers"]["x-int"], {"key": "attr.int", "value_type": "int"})

    # Scenario: A proposed minimal TLS-only configuration uses the system trust store.
    # Guarantees: TLS remains enabled and auth policy is explicitly decided by the harness.
    def test_no_sasl_modes_are_explicit_policy_choices(self):
        del self.customer["kafka"]["auth"]
        self.customer["kafka"]["tls"] = {}
        with self.assertRaises(ConfigError):
            self.convert()
        with self.assertRaises(ConfigError):
            self.convert(auth_policy="authenticated")
        n = self.native(auth_policy="allow-anonymous")
        self.assertEqual(n["tls"], {})
        self.assertNotIn("auth", n)
        self.customer["kafka"]["tls"]["mtlsClientCertificateReference"] = "client"
        self.bindings["references"]["client"] = {
            "certFile": "/certs/client.crt", "keyFile": "/certs/client.key",
            "keyPasswordEnv": "CLIENT_KEY_PASSWORD",
        }
        n = self.native(auth_policy="authenticated")
        self.assertEqual(n["tls"]["cert_file"], "/certs/client.crt")
        self.assertEqual(n["tls"]["key_file"], "/certs/client.key")
        self.assertEqual(n["tls"]["key_password"], "${env:CLIENT_KEY_PASSWORD__JSON}")

    # Scenario: Each proposed SASL mechanism is selected, with or without a client certificate.
    # Guarantees: PLAIN/SCRAM are preserved and SASL plus mTLS is representable.
    def test_sasl_mechanisms_and_mtls(self):
        self.customer["kafka"]["tls"]["mtlsClientCertificateReference"] = "client"
        self.bindings["references"]["client"] = {"certFile": "/certs/client.crt", "keyFile": "/certs/client.key"}
        for mechanism in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"):
            with self.subTest(mechanism=mechanism):
                self.customer["kafka"]["auth"]["sasl"]["mechanism"] = mechanism
                self.assertEqual(self.native()["auth"]["sasl"]["mechanism"], mechanism)

    # Scenario: Optional delivery, enrichment, observability and encoding are absent.
    # Guarantees: Fixed latest/manual behavior is retained and lag is not silently enabled.
    def test_optional_defaults(self):
        for key in ("delivery", "observability"):
            del self.customer["kafka"][key]
        del self.customer["kafka"]["subscription"]["signals"]["logs"]["encoding"]
        n = self.native()
        self.assertNotIn("lag_refresh_interval_ms", n)
        self.assertNotIn("resource_attrs_from_headers", n)
        self.assertEqual(n["logs"]["encoding"], "otlp_proto")
        self.assertEqual(n["isolation_level"], "read_uncommitted")

    # Scenario: Lag is disabled explicitly or enabled without a refresh interval.
    # Guarantees: Disabled lag omits the timer; enabled lag cannot lack a valid interval.
    def test_lag_conditional_interval(self):
        lag = self.customer["kafka"]["observability"]["consumerLag"]
        lag["enabled"] = False
        self.assertNotIn("lag_refresh_interval_ms", self.native())
        lag["enabled"] = True
        del lag["refreshInterval"]
        with self.assertRaises(ConfigError):
            self.convert()

    # Scenario: Human durations include fractional/compound values and boundary errors.
    # Guarantees: Milliseconds are exact, positive and bounded; values never silently round.
    def test_duration_conversion(self):
        for duration, expected in (("60s", 60000), ("1.5s", 1500), ("1m30s", 90000), ("0.001s", 1)):
            with self.subTest(duration=duration):
                self.assertEqual(duration_ms(duration), expected)
        for value in ("0s", "-1s", "1", "abc", "1.0001s", "18446744073709551616ms",
                      "0.99999999999999999999999999999999999ms", 60, True):
            with self.subTest(value=value), self.assertRaises(ConfigError):
                duration_ms(value)

    # Scenario: Customers supply removed fields, platform knobs or misspelled nested fields.
    # Guarantees: The translator rejects inputs instead of ignoring unsupported settings.
    def test_unknown_and_removed_fields(self):
        paths = [
            ((), "unknown"), (("kafka",), "group_instance_id"), (("kafka",), "rebalance_strategy"),
            (("kafka",), "consumer_config"), (("kafka",), "authentication"),
            (("kafka", "delivery"), "deduplicate"), (("kafka", "delivery"), "atMostOnce"),
            (("kafka", "delivery"), "guarantee"), (("kafka", "subscription"), "startAt"),
            (("kafka", "auth"), "type"), (("kafka", "auth", "sasl"), "password"),
            (("kafka", "tls"), "insecure"), (("kafka", "subscription", "signals"), "events"),
            (("kafka", "subscription", "signals", "logs"), "exclude_topics"),
            (("kafka", "observability", "consumerLag"), "interval"),
        ]
        original = copy.deepcopy(self.customer)
        for path, field in paths:
            with self.subTest(path=path, field=field):
                self.customer = copy.deepcopy(original)
                obj = self.customer
                for key in path:
                    obj = obj[key]
                obj[field] = "not-supported"
                with self.assertRaises(ConfigError):
                    self.convert()

    # Scenario: Required schema objects and string/boolean types are invalid.
    # Guarantees: Missing TLS, empty values, null objects and string booleans are rejected.
    def test_required_values_and_types(self):
        cases = [
            (("type",), "Other"), (("name",), ""), (("kafka", "brokers"), []),
            (("kafka", "tls"), None), (("kafka", "auth"), {}),
            (("kafka", "subscription", "consumerGroup"), ""),
            (("kafka", "subscription", "signals"), {}),
            (("kafka", "subscription", "signals", "logs", "topics"), []),
            (("kafka", "subscription", "signals", "logs", "encoding"), "otlp_proto"),
            (("kafka", "delivery", "readCommitted"), "false"),
            (("kafka", "observability", "consumerLag", "enabled"), 1),
            (("kafka", "auth", "sasl", "mechanism"), "AWS_MSK_IAM_OAUTHBEARER"),
        ]
        original = copy.deepcopy(self.customer)
        for path, value in cases:
            with self.subTest(path=path, value=value):
                self.customer = copy.deepcopy(original)
                obj = self.customer
                for key in path[:-1]:
                    obj = obj[key]
                obj[path[-1]] = value
                with self.assertRaises(ConfigError):
                    self.convert()
        self.customer = copy.deepcopy(original)
        del self.customer["kafka"]["tls"]
        with self.assertRaises(ConfigError):
            self.convert()

    # Scenario: Bootstrap endpoints contain malformed hosts, ports or credentials.
    # Guarantees: The native comma-delimited broker string cannot gain extra endpoints or credentials.
    def test_invalid_brokers(self):
        for address in ("kafka", "kafka:0", "kafka:65536", "kafka:9093,other:9093",
                        "user:pass@kafka:9093", "https://kafka:9093", "[invalid]:9093", "kafka :9093"):
            with self.subTest(address=address):
                self.customer["kafka"]["brokers"] = [address]
                with self.assertRaises(ConfigError):
                    self.convert()

    # Scenario: Signal topics overlap or exclusions cannot apply to a literal-only subscription.
    # Guarantees: Ambiguous exact ownership and unusable exclude lists fail before generation.
    def test_topics_and_exclusions(self):
        signals = self.customer["kafka"]["subscription"]["signals"]
        signals["traces"] = {"topics": ["schema-demo-logs"]}
        with self.assertRaises(ConfigError):
            self.convert()
        del signals["traces"]
        signals["logs"]["excludeTopics"] = ["^exclude"]
        with self.assertRaises(ConfigError):
            self.convert()
        del signals["logs"]["excludeTopics"]
        for topic in (".", "..", "bad topic", "a" * 250):
            signals["logs"]["topics"] = [topic]
            with self.subTest(topic=topic), self.assertRaises(ConfigError):
                self.convert()

    # Scenario: Enrichment contains the same source header twice.
    # Guarantees: List-to-map translation cannot silently drop an earlier extraction rule.
    def test_duplicate_header_rules(self):
        self.customer["kafka"]["enrichment"] = {"resourceAttributesFromHeaders": [
            {"header": "x-tenant", "attribute": "tenant.id", "type": "string"},
            {"header": "x-tenant", "attribute": "tenant.other", "type": "string"},
        ]}
        with self.assertRaises(ConfigError):
            self.convert()

    # Scenario: Secret references are missing, wrong-kind or contain inline credentials.
    # Guarantees: Local resolution is explicit and raw secrets are never accepted as bindings.
    def test_invalid_bindings(self):
        originals = copy.deepcopy(self.bindings)
        for binding in ({}, {"usernameEnv": "USER"}, {"username": "do-not-echo", "password": "secret"},
                        {"usernameEnv": "USER", "passwordEnv": "PASSWORD__JSON"},
                        {"usernameEnv": "USER", "passwordEnv": "bad name"},
                        {"caFile": "/certs/ca"}):
            with self.subTest(binding=binding):
                self.bindings = copy.deepcopy(originals)
                self.bindings["references"]["demo-credentials"] = binding
                with self.assertRaises(ConfigError):
                    self.convert()
        self.bindings = {"references": {}}
        with self.assertRaises(ConfigError):
            self.convert()

    # Scenario: Runtime environment bindings are unset or empty.
    # Guarantees: Missing credentials fail explicitly without exposing supplied values.
    def test_missing_runtime_credentials(self):
        for environment in ({}, {"KAFKA_DEMO_USERNAME": "user", "KAFKA_DEMO_PASSWORD": ""}):
            with self.subTest(environment=environment), self.assertRaises(ConfigError):
                runtime_environment(self.bindings, environment)

    # Scenario: Secret values contain quotes, escapes, newlines or placeholder-looking text.
    # Guarantees: Raw-text engine substitution preserves one scalar without YAML injection or disk secrets.
    def test_secret_round_trip_and_literal_dollar_escaping(self):
        password = "\"\nnew-field: injected\n'\\ ${env:NOT_READ} $$ \u00e9"
        self.customer["kafka"]["subscription"]["signals"]["logs"]["topics"] = ["^logs-${env:NOT_READ}$"]
        config = self.convert()
        env = runtime_environment(self.bindings, {
            "KAFKA_DEMO_USERNAME": "true", "KAFKA_DEMO_PASSWORD": password,
        })
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "native.yaml"
            write_config(path, config)
            raw = path.read_text(encoding="utf-8")
            self.assertNotIn(password, raw)
            self.assertIn("username: ${env:KAFKA_DEMO_USERNAME__JSON}", raw)
            # Mirror the native single-pass scanner, not recursive string replacement.
            expanded = re.sub(r"\$\$|\$\{env:([^}]+)\}",
                              lambda match: "$" if match[0] == "$$" else env[match[1]], raw)
            parsed = yaml.safe_load(expanded)
        n = parsed["groups"]["default"]["pipelines"]["main"]["nodes"]["kafka-local"]["config"]
        self.assertEqual(n["auth"]["sasl"]["password"], password)
        self.assertEqual(n["auth"]["sasl"]["username"], "true")
        self.assertEqual(n["logs"]["topics"], ["^logs-${env:NOT_READ}$"])
        self.assertNotIn("new-field", n)

    # Scenario: YAML contains duplicates, aliases, non-string keys or a malformed secret value.
    # Guarantees: Parsing fails without silent replacement or secret-bearing source lines in errors.
    def test_strict_yaml_loading_and_redacted_errors(self):
        for contents in ("name: a\nname: b\n", "a: &value [x]\nb: *value\n", "1: value\n",
                         "password: [do-not-echo\n"):
            with tempfile.TemporaryDirectory() as tmp, self.subTest(contents=contents):
                path = Path(tmp) / "input.yaml"
                path.write_text(contents, encoding="utf-8")
                with self.assertRaises(ConfigError) as raised:
                    load_yaml(path)
                self.assertNotIn("do-not-echo", str(raised.exception))

    # Scenario: A customer source is named like the harness sink.
    # Guarantees: Generated connections still connect distinct receiver and sink nodes.
    def test_sink_identity_never_overwrites_receiver(self):
        self.customer["name"] = "sink"
        pipeline = self.convert()["groups"]["default"]["pipelines"]["main"]
        self.assertEqual(set(pipeline["nodes"]), {"sink", "sink-sink"})
        self.assertEqual(pipeline["connections"], [{"from": "sink", "to": "sink-sink"}])

    # Scenario: Native validation fails and its error text contains a resolved credential.
    # Guarantees: The failure is surfaced, but credentials and encoded equivalents are redacted.
    def test_native_error_redaction(self):
        env = {"KAFKA_DEMO_USERNAME": "demo-user", "KAFKA_DEMO_PASSWORD": "must-not-leak"}
        failure = subprocess.CompletedProcess([], 1, "", "invalid password must-not-leak")
        with mock.patch.dict(os.environ, env), mock.patch("translate.subprocess.run", return_value=failure):
            with self.assertRaises(ConfigError) as raised:
                validate_with_image(HERE / "generated" / "consumer-a.yaml", self.bindings, "test-image")
        self.assertIn("Native validation failed", str(raised.exception))
        self.assertNotIn("must-not-leak", str(raised.exception))

    # Scenario: A native validator hangs after Docker has started its container.
    # Guarantees: The timeout removes only that uniquely named validation container.
    def test_native_timeout_cleanup(self):
        env = {"KAFKA_DEMO_USERNAME": "demo-user", "KAFKA_DEMO_PASSWORD": "local-password"}
        with mock.patch.dict(os.environ, env), mock.patch("translate.subprocess.run") as run:
            run.side_effect = [subprocess.TimeoutExpired("docker", 120),
                               subprocess.CompletedProcess([], 0)]
            with self.assertRaisesRegex(ConfigError, "timed out"):
                validate_with_image(HERE / "generated" / "consumer-a.yaml", self.bindings, "test-image")
            start = run.call_args_list[0].args[0]
            cleanup = run.call_args_list[1].args[0]
            self.assertEqual(cleanup[:3], ["docker", "rm", "-f"])
            self.assertEqual(cleanup[3], start[start.index("--name") + 1])

    # Scenario: Invalid customer YAML is translated into an existing output path.
    # Guarantees: The CLI exits unsuccessfully and does not replace the previous file.
    def test_cli_failure_preserves_existing_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "customer.yaml"
            output = Path(tmp) / "existing.yaml"
            source.write_text("name: incomplete\n", encoding="utf-8")
            output.write_text("previous output\n", encoding="utf-8")
            result = subprocess.run([
                sys.executable, str(HERE / "translate.py"), str(source),
                "--bindings", str(HERE / "bindings.yaml"), "--instance-id", "a",
                "--membership", "dynamic", "--rebalance-strategy", "cooperative_sticky",
                "--auth-policy", "sasl-only", "--output", str(output),
            ], capture_output=True, text=True, timeout=20)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Translation failed", result.stderr)
            self.assertEqual(output.read_text(encoding="utf-8"), "previous output\n")

    # Scenario: An installed engine image validates generated configs with real native validators.
    # Guarantees: The engine accepts mapped settings and rejects invalid native regex syntax.
    @unittest.skipUnless(os.environ.get("OTAP_SCHEMA_TEST_IMAGE"), "Set OTAP_SCHEMA_TEST_IMAGE for native validation")
    def test_native_engine_validation(self):
        image = os.environ["OTAP_SCHEMA_TEST_IMAGE"]
        self.customer["kafka"]["tls"]["mtlsClientCertificateReference"] = "client"
        self.bindings["references"]["client"] = {"certFile": "/certs/client.crt", "keyFile": "/certs/client.key"}
        self.customer["kafka"]["subscription"]["signals"].update(
            traces={"topics": ["traces"], "encoding": "otlpProto"},
            metrics={"topics": ["metrics"], "encoding": "otapProto"},
        )
        self.customer["kafka"]["enrichment"] = {"resourceAttributesFromHeaders": [
            {"header": "x-tenant", "attribute": "tenant.id", "type": "string"},
        ]}
        self.customer["kafka"]["delivery"]["readCommitted"] = True
        env = runtime_environment(self.bindings, {
            **os.environ, "KAFKA_DEMO_USERNAME": "test-user", "KAFKA_DEMO_PASSWORD": "test\"'\\password",
        })
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "native.yaml"
            for mechanism in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"):
                self.customer["kafka"]["auth"]["sasl"]["mechanism"] = mechanism
                write_config(path, self.convert(membership="static"))
                result = subprocess.run([
                    "docker", "run", "--rm", "--network", "none", "--mount",
                    f"type=bind,source={tmp},target=/input,readonly",
                    "-e", "KAFKA_DEMO_USERNAME__JSON", "-e", "KAFKA_DEMO_PASSWORD__JSON", image,
                    "--config", "file:/input/native.yaml", "--validate-and-exit",
                ], env=env, capture_output=True, text=True, timeout=60)
                self.assertEqual(result.returncode, 0, f"Native {mechanism} config validation failed")
            self.customer["kafka"]["subscription"]["signals"]["logs"]["topics"] = ["^["]
            write_config(path, self.convert())
            result = subprocess.run([
                "docker", "run", "--rm", "--network", "none", "--mount",
                f"type=bind,source={tmp},target=/input,readonly",
                "-e", "KAFKA_DEMO_USERNAME__JSON", "-e", "KAFKA_DEMO_PASSWORD__JSON", image,
                "--config", "file:/input/native.yaml", "--validate-and-exit",
            ], env=env, capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0, "Invalid regex unexpectedly passed native validation")


if __name__ == "__main__":
    unittest.main()
