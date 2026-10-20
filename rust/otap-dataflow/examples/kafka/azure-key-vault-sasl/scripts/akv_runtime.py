#!/usr/bin/env python3
"""Private container-side provisioning, rendering, and fail-closed log oracles."""

import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
from urllib.parse import quote, urlsplit


class SafeFailure(Exception):
    """A diagnostic that contains no runtime resource or credential content."""


def validate_config(config):
    if not isinstance(config, dict):
        raise SafeFailure("Runtime input must be an object.")
    allowed = {
        "vault_url", "username_secret", "password_secret",
        "authorization_vault_url", "identity",
    }
    if set(config) - allowed:
        raise SafeFailure("Runtime input contains unsupported fields.")
    for field in ("vault_url", "authorization_vault_url"):
        if field not in config and field == "authorization_vault_url":
            continue
        value = config.get(field)
        if not isinstance(value, str):
            raise SafeFailure("A vault root URL is required.")
        url = urlsplit(value)
        if (
            url.scheme != "https"
            or not re.fullmatch(r"[A-Za-z0-9-]+\.vault\.azure\.net", url.netloc)
            or url.path not in ("", "/")
            or url.query
            or url.fragment
        ):
            raise SafeFailure("Use a public Azure HTTPS vault root, not a secret URL.")
    for field in ("username_secret", "password_secret"):
        ref = config.get(field)
        if not isinstance(ref, dict) or set(ref) - {"name", "version"}:
            raise SafeFailure("A secret reference requires name and optional version.")
        if not isinstance(ref.get("name"), str) or not re.fullmatch(
            r"[A-Za-z0-9-]{1,127}", ref["name"]
        ):
            raise SafeFailure("Invalid secret name in runtime input.")
        if "version" in ref and (
            not isinstance(ref["version"], str)
            or not re.fullmatch(r"[0-9a-fA-F]{32}", ref["version"])
        ):
            raise SafeFailure("A secret version must have exactly 32 hexadecimal digits.")
    identity = config.get("identity")
    if not isinstance(identity, dict) or set(identity) - {"method", "client_id"}:
        raise SafeFailure("Explicit identity method and optional managed client ID are required.")
    method = identity.get("method")
    if method not in ("managed_identity", "development"):
        raise SafeFailure("Use an explicitly approved managed_identity or development method.")
    if "client_id" in identity and (
        method != "managed_identity"
        or not isinstance(identity["client_id"], str)
        or not identity["client_id"]
        or identity["client_id"].strip() != identity["client_id"]
        or any(ord(char) < 32 for char in identity["client_id"])
    ):
        raise SafeFailure("Only managed identity accepts a nonempty existing client ID.")
    return config


def render_config(template, config, case, nonce):
    validate_config(config)
    username = dict(config["username_secret"])
    password = dict(config["password_secret"])
    vault = config["vault_url"]
    timeout = "15s"
    if case in ("missing-username", "missing-password"):
        selected = username if case == "missing-username" else password
        selected.clear()
        selected["name"] = f"otap-e2e-absent-{nonce}"
    elif case == "timeout":
        timeout = "1ms"
    elif case == "authorization":
        if "authorization_vault_url" not in config:
            raise SafeFailure("Authorization case needs an explicitly supplied denied vault.")
        vault = config["authorization_vault_url"]
    elif case not in ("configured", "broker-rejection"):
        raise SafeFailure("Unknown validation case.")
    replacements = {
        "__VAULT_URL_JSON__": vault,
        "__USERNAME_SECRET_JSON__": username,
        "__PASSWORD_SECRET_JSON__": password,
        "__IDENTITY_JSON__": config["identity"],
        "__STARTUP_TIMEOUT_JSON__": timeout,
    }
    for token, value in replacements.items():
        if template.count(token) != 1:
            raise SafeFailure("Receiver template has an invalid placeholder.")
        template = template.replace(token, json.dumps(value, ensure_ascii=True))
    return template


def run_az(arguments, output="json"):
    try:
        result = subprocess.run(
            ["az", *arguments, "--only-show-errors", "--output", output],
            capture_output=True, text=True, timeout=30, check=False,
        )
    except subprocess.TimeoutExpired:
        raise SafeFailure("Container Azure CLI operation timed out.") from None
    if result.returncode:
        raise SafeFailure("Container Azure CLI operation failed; check login, access, and network.")
    return json.loads(result.stdout) if output == "json" else None


def prepare_identity(config, mode, managed_approved):
    identity = config["identity"]
    if identity["method"] != mode:
        raise SafeFailure("Runtime identity does not match the explicitly selected harness mode.")
    if mode == "managed_identity":
        if managed_approved != "true":
            raise SafeFailure("Managed identity requires explicit approved-VM execution handoff.")
        arguments = ["login", "--identity", "--allow-no-subscriptions"]
        if "client_id" in identity:
            arguments.extend(("--client-id", identity["client_id"]))
        # Private CLI bootstrap only; the receiver independently uses the real SDK MI credential.
        run_az(arguments, output="none")
    else:
        run_az(["account", "show"])


def read_secret(config, field):
    ref = config[field]
    secret_id = config["vault_url"].rstrip("/") + "/secrets/" + ref["name"]
    if "version" in ref:
        secret_id += "/" + ref["version"]
    value = run_az(["keyvault", "secret", "show", "--id", secret_id, "--query", "value"])
    if not isinstance(value, str) or not value:
        raise SafeFailure("Selected secret has no usable string value.")
    return value


def jaas_string(value):
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise SafeFailure("Local JAAS fixture does not support control characters.")
    return value.replace("\\", "\\\\").replace('"', '\\"')


def write_private(path, content, uid=None):
    path.write_text(content, encoding="utf-8")
    if uid is not None:
        os.chown(path, uid, uid)
    path.chmod(0o400 if uid is not None else 0o600)


def configure_broker(values, reject=False):
    username, password = values["username"], values["password"]
    # JAAS option keys cannot represent arbitrary usernames without ambiguity.
    if not re.fullmatch(r"[A-Za-z0-9_]{1,128}", username) or username == "plain":
        raise SafeFailure("Local broker needs an alphanumeric/underscore username other than plain.")
    selected = password
    if reject:
        selected = secrets.token_urlsafe(48)
        while selected == password:
            selected = secrets.token_urlsafe(48)
    text = (
        "KafkaServer {\n"
        " org.apache.kafka.common.security.plain.PlainLoginModule required\n"
        ' username="plain" password="plain-secret"\n'
        ' user_plain="plain-secret"\n'
        f' user_{username}="{jaas_string(selected)}";\n'
        "};\n"
    )
    write_private(Path("/broker-auth/server.conf"), text, uid=1000)
    if reject:
        values["rejected_broker_password"] = selected
        write_private(Path("/private/reference.json"), json.dumps(values))


def summarize_logs(logs, expected, values):
    bodies = []
    for line in logs.splitlines():
        if line.startswith("{"):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                raise SafeFailure("Receiver emitted malformed JSON output.") from None
            if isinstance(record, dict) and "body" in record:
                bodies.append(record["body"])
    candidates = []
    for value in values.values():
        candidates.extend((value, json.dumps(value, ensure_ascii=True)[1:-1], quote(value, safe="")))
    leaked = any(value and value in logs for value in candidates)
    failure_pattern = (
        r"Azure Key Vault (identity|client|username|password|acquisition)"
        r" acquisition failed \((authentication|authorization|not_found|throttled|transient|"
        r"invalid_value|invalid_response|timeout|configuration|service)\)"
    )
    return {
        "LeakDetected": leaked,
        "OutputCount": len(bodies),
        "ExactBodies": len(bodies) == len(expected)
        and all(isinstance(body, str) for body in bodies)
        and sorted(bodies) == sorted(expected),
        "StartupFailures": [
            {"Stage": stage, "Category": category}
            for stage, category in re.findall(failure_pattern, logs)
        ],
        "BrokerAuthentication": bool(re.search(
            r"(?i)SASL authentication (error|failed)|Authentication failure|SaslAuthenticationFailed",
            logs,
        )),
    }


def main():
    os.umask(0o077)
    operation = sys.argv[1]
    if operation == "scan":
        request = json.load(sys.stdin)
        values = json.loads(Path("/private/reference.json").read_text(encoding="utf-8"))
        print(json.dumps(summarize_logs(request["logs"], request["expected"], values)))
        return
    config = validate_config(json.loads(Path("/input/runtime.json").read_text(encoding="utf-8")))
    if operation == "bootstrap":
        # No discovery/probing here: approval identifies the actual VM before this is allowed.
        prepare_identity(
            config, os.environ.get("AKV_IDENTITY_MODE"),
            os.environ.get("AKV_MANAGED_IDENTITY_APPROVED"),
        )
        values = {
            "username": read_secret(config, "username_secret"),
            "password": read_secret(config, "password_secret"),
        }
        configure_broker(values)
        write_private(Path("/private/reference.json"), json.dumps(values))
        flag = Path("/runtime/authorization-enabled")
        flag.write_text(str("authorization_vault_url" in config).lower(), encoding="ascii")
        # Only a boolean; the non-root VM driver must be able to read this root-created file.
        flag.chmod(0o644)
    elif operation == "render":
        template = Path("/validation/receiver.template.yaml").read_text(encoding="utf-8")
        rendered = render_config(template, config, sys.argv[2], sys.argv[3])
        write_private(Path("/runtime/receiver.yaml"), rendered)
    elif operation == "reject-broker":
        values = json.loads(Path("/private/reference.json").read_text(encoding="utf-8"))
        # Kafka is stopped first, so replace its read-only file safely.
        Path("/broker-auth/server.conf").unlink()
        configure_broker(values, reject=True)
    else:
        raise SafeFailure("Unknown private helper operation.")


if __name__ == "__main__":
    try:
        main()
    except SafeFailure as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
    except (OSError, ValueError, KeyError, TypeError, IndexError, subprocess.SubprocessError) as error:
        # Never print raw exceptions: SDK/CLI/file errors can contain private inputs.
        print(f"Private helper failed ({type(error).__name__}); raw details suppressed.", file=sys.stderr)
        sys.exit(1)
