"""Lightweight harness tests. No Docker, Azure, SDK, or network access."""

from copy import deepcopy
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.parse import quote

import akv_runtime as runtime


TEMPLATE = (Path(__file__).resolve().parent.parent / "receiver.yaml").read_text(encoding="utf-8")
CONFIG = {
    "vault_url": "https://example-e2e.vault.azure.net/",
    "username_secret": {"name": "example-username"},
    "password_secret": {"name": "example-password", "version": "1a" * 16},
}
VALUES = {"username": "fixture_login_794", "password": 'fixture-password-"\\-794'}
EXPECTED = ["fixture-record-1", "fixture-record-2", "fixture-record-3"]


def rendered_fields(case, config=None):
    rendered = runtime.render_config(TEMPLATE, config or deepcopy(CONFIG), case, "fixture-nonce")
    fields = {}
    for line in rendered.splitlines():
        key, _, value = line.strip().partition(": ")
        if key in ("vault_url", "username_secret", "password_secret", "startup_timeout"):
            fields[key] = json.loads(value)
    return fields


class RuntimeTests(unittest.TestCase):
    # Scenario: A configured run uses one latest-at-startup reference and one pinned version.
    # Guarantees: References and exact version text survive rendering without defaults or trimming.
    def test_configured_preserves_optional_pins(self):
        fields = rendered_fields("configured")
        self.assertEqual(fields["username_secret"], CONFIG["username_secret"])
        self.assertEqual(fields["password_secret"], CONFIG["password_secret"])
        self.assertEqual(fields["startup_timeout"], "15s")

    # Scenario: Public runtime input has absent, malformed, or unsupported identity/resource fields.
    # Guarantees: Invalid input cannot inject alternate credentials, fallback, or arbitrary CLI targets.
    def test_invalid_inputs_fail_closed(self):
        for changes in (
            {"vault_url": "http://example-e2e.vault.azure.net"},
            {"vault_url": "https://example-e2e.vault.azure.net/secrets/example"},
            {"vault_url": "https://example-e2e.vault.azure.net?query=1"},
            {"vault_url": "https://user@example-e2e.vault.azure.net"},
            {"vault_url": "https://example-e2e.vault.azure.net:443"},
            {"vault_url": "https://other.invalid"},
            {"username_secret": {"name": "../example"}},
            {"password_secret": {"name": "example", "version": "a" * 31}},
            {"username_secret": {"name": "example", "inline": "not-allowed"}},
            {"identity": {"method": "client_secret"}},
            {"vault_url": None},
        ):
            with self.subTest(fields=list(changes)):
                with self.assertRaises(runtime.SafeFailure):
                    runtime.validate_config({**deepcopy(CONFIG), **changes})

    # Scenario: Secret versions are optional, but supplied versions must have exactly 32 hex digits.
    # Guarantees: Both references accept valid independent pins and reject whitespace and non-hex text.
    def test_version_pins_are_exact(self):
        for version in ("Ab" * 16, "0" * 32):
            config = deepcopy(CONFIG)
            config["username_secret"]["version"] = version
            self.assertEqual(rendered_fields("configured", config)["username_secret"]["version"], version)
        for version in (" " + "a" * 32, "a" * 32 + "\n", "z" * 32, "", 123):
            config = deepcopy(CONFIG)
            config["username_secret"]["version"] = version
            with self.assertRaises(runtime.SafeFailure):
                runtime.validate_config(config)

    # Scenario: Broker setup selects a latest reference or an explicit immutable version through CLI.
    # Guarantees: Secret IDs have the selected version and returned whitespace is not normalized.
    def test_cli_reference_and_value_preservation(self):
        with patch.object(runtime, "run_az", return_value=" exact value \r\n") as run_az:
            self.assertEqual(runtime.read_secret(CONFIG, "username_secret"), " exact value \r\n")
            self.assertEqual(run_az.call_args.args[0], [
                "keyvault", "secret", "show", "--id",
                "https://example-e2e.vault.azure.net/secrets/example-username", "--query", "value",
            ])
            runtime.read_secret(CONFIG, "password_secret")
            self.assertEqual(run_az.call_args.args[0][4], (
                "https://example-e2e.vault.azure.net/secrets/example-password/" + "1a" * 16
            ))

    # Scenario: One selected secret does not exist while the other remains the configured valid reference.
    # Guarantees: Missing-password coverage retains the real username read and cannot become a file fallback.
    def test_missing_reference_changes_only_selected_field(self):
        for selected, other in (("username", "password"), ("password", "username")):
            with self.subTest(selected=selected):
                fields = rendered_fields(f"missing-{selected}")
                self.assertEqual(fields[f"{selected}_secret"], {"name": "otap-e2e-absent-fixture-nonce"})
                self.assertEqual(fields[f"{other}_secret"], CONFIG[f"{other}_secret"])

    # Scenario: Authorization coverage is requested with and without an explicitly provided denied vault.
    # Guarantees: The harness never guesses an inaccessible Azure resource or changes the original references.
    def test_authorization_requires_explicit_target(self):
        with self.assertRaises(runtime.SafeFailure):
            rendered_fields("authorization")
        config = {**deepcopy(CONFIG), "authorization_vault_url": "https://example-denied.vault.azure.net"}
        fields = rendered_fields("authorization", config)
        self.assertEqual(fields["vault_url"], config["authorization_vault_url"])
        self.assertEqual(fields["username_secret"], CONFIG["username_secret"])

    # Scenario: The startup deadline is intentionally shorter than a real remote credential acquisition.
    # Guarantees: Timeout setup changes only the extension's startup bound, not its identity or source.
    def test_timeout_changes_only_deadline(self):
        actual = rendered_fields("timeout")
        expected = rendered_fields("configured")
        expected["startup_timeout"] = "1ms"
        self.assertEqual(actual, expected)

    # Scenario: A template placeholder is missing or a case name is not in the bounded matrix.
    # Guarantees: Rendering fails rather than producing a success-shaped partial runtime configuration.
    def test_bad_template_or_case_fails(self):
        with self.assertRaises(runtime.SafeFailure):
            runtime.render_config(TEMPLATE.replace("__VAULT_URL_JSON__", ""), CONFIG, "configured", "nonce")
        with self.assertRaises(runtime.SafeFailure):
            rendered_fields("unsupported")

    # Scenario: A real password contains spaces, backslashes, quotes, or unsupported control characters.
    # Guarantees: Supported JAAS values are preserved exactly; unsupported values fail without normalization.
    def test_jaas_escaping_does_not_trim(self):
        self.assertEqual(runtime.jaas_string(' a\\"b '), ' a\\\\\\"b ')
        for value in ("value\n", "\tvalue", "value\x00", "value\x7f"):
            with self.assertRaises(runtime.SafeFailure):
                runtime.jaas_string(value)

    # Scenario: A wrong-credential test changes only the private local broker configuration.
    # Guarantees: It preserves the actual pair for leak scanning and chooses a genuinely unequal password.
    def test_broker_rejection_does_not_change_source_pair(self):
        values = deepcopy(VALUES)
        with patch.object(runtime, "write_private") as write_private:
            with patch.object(runtime.secrets, "token_urlsafe", side_effect=[VALUES["password"], "synthetic-unequal"]):
                runtime.configure_broker(values, reject=True)
        self.assertEqual(values["username"], VALUES["username"])
        self.assertEqual(values["password"], VALUES["password"])
        self.assertEqual(values["rejected_broker_password"], "synthetic-unequal")
        self.assertEqual(write_private.call_args_list[0].args[0], Path("/broker-auth/server.conf"))
        self.assertIn('user_fixture_login_794="synthetic-unequal"', write_private.call_args_list[0].args[1])
        self.assertEqual(write_private.call_args_list[1].args[0], Path("/private/reference.json"))

    # Scenario: A vault username cannot be represented unambiguously by this local JAAS fixture.
    # Guarantees: The helper fails before writing a broker file and never silently rewrites the username.
    def test_unsupported_broker_username_fails_before_write(self):
        for username in ("plain", "has-dash", "has space", "line\n", ""):
            with self.subTest(username=username):
                with patch.object(runtime, "write_private") as write_private:
                    with self.assertRaises(runtime.SafeFailure):
                        runtime.configure_broker({**VALUES, "username": username})
                    write_private.assert_not_called()

    # Scenario: Console output includes exactly the three expected string log bodies.
    # Guarantees: Only the complete exact multiset passes delivery, independent of incidental startup text.
    def test_exact_delivery_oracle(self):
        logs = "receiver started\n" + "\n".join(json.dumps({"body": body}) for body in EXPECTED)
        summary = runtime.summarize_logs(logs, EXPECTED, VALUES)
        self.assertTrue(summary["ExactBodies"])
        self.assertEqual(summary["OutputCount"], 3)
        self.assertFalse(summary["LeakDetected"])

    # Scenario: Startup-only output, duplicates, missing bodies, wrong bodies, or non-string bodies appear.
    # Guarantees: Counts, offsets, readiness messages, and malformed delivery cannot pass the E2E oracle.
    def test_non_exact_delivery_fails(self):
        for bodies in ([], EXPECTED[:2], EXPECTED + EXPECTED[:1], [*EXPECTED[:2], "wrong"], [1, 2, 3]):
            logs = "\n".join(json.dumps({"body": body}) for body in bodies)
            self.assertFalse(runtime.summarize_logs(logs, EXPECTED, VALUES)["ExactBodies"])
        with self.assertRaises(runtime.SafeFailure):
            runtime.summarize_logs('{"body":', EXPECTED, VALUES)

    # Scenario: The provider emits its documented bounded stage/category error Display.
    # Guarantees: Missing-password cannot pass on an unrelated username, configuration, or process failure.
    def test_safe_startup_classification(self):
        logs = "Azure Key Vault password acquisition failed (not_found)"
        summary = runtime.summarize_logs(logs, EXPECTED, VALUES)
        self.assertEqual(summary["StartupFailures"], [{"Stage": "password", "Category": "not_found"}])
        self.assertEqual(runtime.summarize_logs("404\nreceiver exited", EXPECTED, VALUES)["StartupFailures"], [])
        for category in ("authorization", "timeout", "authentication", "transient"):
            logs = f"Azure Key Vault acquisition acquisition failed ({category})"
            self.assertEqual(runtime.summarize_logs(logs, EXPECTED, VALUES)["StartupFailures"][0]["Category"], category)

    # Scenario: A live Kafka authentication error appears without decoded records.
    # Guarantees: Explicit SASL rejection is distinguishable from generic startup or network failure.
    def test_broker_rejection_is_explicit(self):
        for text in ("SASL authentication failed", "SASL authentication error", "SaslAuthenticationFailed"):
            self.assertTrue(runtime.summarize_logs(text, EXPECTED, VALUES)["BrokerAuthentication"])
        self.assertFalse(runtime.summarize_logs("connection refused", EXPECTED, VALUES)["BrokerAuthentication"])

    # Scenario: Credentials are emitted verbatim, JSON-escaped, or URL-escaped in captured process output.
    # Guarantees: Disclosure is flagged and serialized observations contain none of the raw log content.
    def test_disclosure_detection_and_safe_output(self):
        for value in VALUES.values():
            for encoded in (value, json.dumps(value)[1:-1], quote(value, safe="")):
                summary = runtime.summarize_logs(encoded + "\nprivate-resource-canary", EXPECTED, VALUES)
                self.assertTrue(summary["LeakDetected"])
                self.assertNotIn("private-resource-canary", json.dumps(summary))
                self.assertNotIn(value, json.dumps(summary))

    # Scenario: A body itself contains private material instead of an expected fixture record.
    # Guarantees: Evidence records only counts and booleans, never an unexpected body or resource path.
    def test_unexpected_body_never_leaves_scanner(self):
        body = "private-resource-canary"
        summary = runtime.summarize_logs(json.dumps({"body": body}), EXPECTED, VALUES)
        self.assertEqual(summary["OutputCount"], 1)
        self.assertFalse(summary["ExactBodies"])
        self.assertNotIn(body, json.dumps(summary))


if __name__ == "__main__":
    unittest.main()
