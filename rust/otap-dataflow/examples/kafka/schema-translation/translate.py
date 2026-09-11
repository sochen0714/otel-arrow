#!/usr/bin/env python3
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Local prototype of the proposed Kafka customer-to-native configuration adapter."""

import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid
from fractions import Fraction

import yaml


class ConfigError(ValueError):
    """Invalid customer configuration or local adapter binding."""


class StrictLoader(yaml.SafeLoader):
    """Reject duplicate keys and aliases instead of silently losing input."""

    def compose_node(self, parent, index):
        if self.check_event(yaml.AliasEvent):
            raise ConfigError("YAML aliases are not supported by this prototype")
        return super().compose_node(parent, index)

    def construct_mapping(self, node, deep=False):
        result = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, str):
                raise ConfigError("YAML mapping keys must be strings")
            if key in result:
                raise ConfigError("Duplicate YAML mapping key")
            result[key] = self.construct_object(value_node, deep=deep)
        return result


class EnvScalar(str):
    """A placeholder replaced by a complete JSON-quoted string at engine startup."""


class NativeDumper(yaml.SafeDumper):
    """Keep runtime placeholders distinct from literal customer strings."""


def _literal(dumper, value):
    # The engine expands raw text before parsing YAML. Escape literal dollars,
    # including regex end anchors and customer text resembling ${env:...}.
    return dumper.represent_scalar("tag:yaml.org,2002:str", value.replace("$", "$$"))


NativeDumper.add_representer(str, _literal)
NativeDumper.add_representer(
    EnvScalar,
    lambda dumper, value: dumper.represent_scalar("tag:yaml.org,2002:str", value),
)


def load_yaml(path):
    try:
        with Path(path).open(encoding="utf-8") as stream:
            return yaml.load(stream, Loader=StrictLoader)
    except yaml.YAMLError as exc:
        # Parser errors normally include source lines, which might contain
        # mistakenly supplied credentials. Report location without input text.
        mark = getattr(exc, "problem_mark", None)
        location = f" at line {mark.line + 1}" if mark else ""
        raise ConfigError(f"Invalid YAML{location}") from None


def object_fields(value, path, allowed, required=()):
    if not isinstance(value, dict):
        raise ConfigError(f"{path} must be an object")
    if any(key not in allowed for key in value):
        raise ConfigError(f"{path} contains an unsupported field")
    missing = set(required) - value.keys()
    if missing:
        raise ConfigError(f"{path} requires {', '.join(sorted(missing))}")
    return value


def text(value, path):
    if not isinstance(value, str) or not value.strip() or "\0" in value:
        raise ConfigError(f"{path} must be a non-empty string without NUL characters")
    return value


def boolean(value, path):
    if type(value) is not bool:
        raise ConfigError(f"{path} must be true or false")
    return value


def choice(value, path, allowed):
    if not isinstance(value, str) or value not in allowed:
        raise ConfigError(f"{path} must be one of {', '.join(allowed)}")
    return value


def strings(value, path, nonempty=False):
    if not isinstance(value, list) or (nonempty and not value):
        raise ConfigError(f"{path} must be {'a non-empty' if nonempty else 'an'} array")
    result = [text(item, path) for item in value]
    if len(result) != len(set(result)):
        raise ConfigError(f"{path} contains duplicate entries")
    return result


def duration_ms(value):
    value = text(value, "consumerLag.refreshInterval")
    parts = list(re.finditer(r"(\d+(?:\.\d+)?)(ms|s|m|h)", value))
    if not parts or "".join(part.group() for part in parts) != value:
        raise ConfigError("refreshInterval requires durations such as 500ms, 1.5s or 1m30s")
    factors = {"ms": 1, "s": 1000, "m": 60000, "h": 3600000}
    total = sum(Fraction(p[1]) * factors[p[2]] for p in parts)
    if total.denominator != 1 or not 0 < total <= 2**64 - 1:
        raise ConfigError("refreshInterval must fit in positive, whole u64 milliseconds")
    return int(total)


def broker(value):
    value = text(value, "kafka.brokers[]")
    match = re.fullmatch(r"(\[[^\]]+\]|[A-Za-z0-9_.-]+):([0-9]{1,5})", value)
    if not match or not 1 <= int(match[2]) <= 65535:
        raise ConfigError("kafka.brokers[] requires host:port, without credentials or a URI")
    if match[1].startswith("["):
        try:
            ipaddress.IPv6Address(match[1][1:-1])
        except ipaddress.AddressValueError:
            raise ConfigError("Invalid bracketed IPv6 broker address") from None
    return value


def validate_bindings(bindings):
    root = object_fields(bindings, "bindings", {"references"}, {"references"})
    references = root["references"]
    if not isinstance(references, dict):
        raise ConfigError("bindings.references must be an object")
    for reference, binding in references.items():
        text(reference, "local reference")
        if not isinstance(binding, dict):
            raise ConfigError("Each local reference binding must be an object")
        if "usernameEnv" in binding or "passwordEnv" in binding:
            required = allowed = {"usernameEnv", "passwordEnv"}
        elif "caFile" in binding:
            required = allowed = {"caFile"}
        else:
            required = {"certFile", "keyFile"}
            allowed = required | {"keyPasswordEnv"}
        object_fields(binding, "local reference binding", allowed, required)
        for key, value in binding.items():
            text(value, f"binding.{key}")
            if key.endswith("Env"):
                if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", value) or value.endswith("__JSON"):
                    raise ConfigError(
                        f"binding.{key} requires an uppercase environment variable name "
                        "not ending in __JSON"
                    )
    return references


def runtime_environment(bindings, environ=None):
    """Encode secrets in memory; never insert raw values into generated YAML."""
    source = dict(os.environ if environ is None else environ)
    result = dict(source)
    for binding in validate_bindings(bindings).values():
        for key, variable in binding.items():
            if key.endswith("Env"):
                value = source.get(variable)
                if not isinstance(value, str) or not value or "\0" in value:
                    raise ConfigError(f"Required runtime variable {variable} is missing or empty")
                result[variable + "__JSON"] = json.dumps(value, ensure_ascii=True)
    return result


def translate(document, bindings, *, instance_id, membership, rebalance_strategy, auth_policy):
    """Map one logical receiver into one complete local receiver-to-noop pipeline."""
    root = object_fields(document, "receiver", {"name", "type", "kafka"}, {"name", "type", "kafka"})
    name = text(root["name"], "name")
    # A bounded local naming convention, not a finalized customer schema restriction.
    for value, path in ((name, "name"), (instance_id, "instance-id")):
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value):
            raise ConfigError(f"{path} requires 1-128 ASCII letters, digits, dots, hyphens or underscores")
    choice(root["type"], "type", ("Kafka",))
    choice(membership, "membership", ("dynamic", "static"))
    choice(rebalance_strategy, "rebalance-strategy",
           ("range", "round_robin", "cooperative_sticky", "native-default"))
    choice(auth_policy, "auth-policy", ("sasl-only", "authenticated", "allow-anonymous"))
    k = object_fields(
        root["kafka"], "kafka",
        {"brokers", "auth", "tls", "subscription", "delivery", "enrichment", "observability"},
        {"brokers", "tls", "subscription"},
    )
    refs = validate_bindings(bindings)

    def resolve(reference, field, required):
        reference = text(reference, field)
        binding = refs.get(reference)
        if binding is None or not required <= binding.keys():
            raise ConfigError(f"{field} has no local binding of the required kind")
        return binding

    def env(variable):
        return EnvScalar("${env:" + variable + "__JSON}")

    subscription = object_fields(
        k["subscription"], "subscription", {"consumerGroup", "signals"}, {"consumerGroup", "signals"}
    )
    native = {
        "brokers": ",".join(broker(b) for b in strings(k["brokers"], "brokers", nonempty=True)),
        "group_id": text(subscription["consumerGroup"], "subscription.consumerGroup"),
        "client_id": f"{name}-{instance_id}",
        "commit": {"mode": "manual"},
        "auto_offset_reset": "latest",
        "enable_idempotency": False,
    }
    if membership == "static":
        native["group_instance_id"] = f"{name}-{instance_id}"
    if rebalance_strategy != "native-default":
        native["rebalance_strategy"] = rebalance_strategy

    tls = object_fields(k["tls"], "tls", {"caReference", "mtlsClientCertificateReference"})
    native["tls"] = {}
    if "caReference" in tls:
        binding = resolve(tls["caReference"], "tls.caReference", {"caFile"})
        native["tls"]["ca_file"] = binding["caFile"]
    if "mtlsClientCertificateReference" in tls:
        binding = resolve(tls["mtlsClientCertificateReference"],
                          "tls.mtlsClientCertificateReference", {"certFile", "keyFile"})
        native["tls"].update(cert_file=binding["certFile"], key_file=binding["keyFile"])
        if "keyPasswordEnv" in binding:
            native["tls"]["key_password"] = env(binding["keyPasswordEnv"])
    if "auth" in k:
        auth = object_fields(k["auth"], "auth", {"sasl"}, {"sasl"})
        sasl = object_fields(auth["sasl"], "auth.sasl",
                             {"mechanism", "credentialReference"}, {"mechanism", "credentialReference"})
        mechanism = choice(sasl["mechanism"], "auth.sasl.mechanism",
                           ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"))
        binding = resolve(sasl["credentialReference"], "auth.sasl.credentialReference",
                          {"usernameEnv", "passwordEnv"})
        native["auth"] = {"sasl": {
            "mechanism": mechanism,
            "username": env(binding["usernameEnv"]),
            "password": env(binding["passwordEnv"]),
        }}
    elif auth_policy == "sasl-only":
        raise ConfigError("The selected local policy requires auth.sasl")
    elif auth_policy == "authenticated" and "cert_file" not in native["tls"]:
        raise ConfigError("The selected local policy requires SASL or an mTLS client certificate")

    signal_names = ("traces", "metrics", "logs")
    signals = object_fields(subscription["signals"], "subscription.signals", signal_names)
    seen = set()
    for signal in signal_names:
        if signal not in signals:
            continue
        path = f"subscription.signals.{signal}"
        config = object_fields(signals[signal], path, {"topics", "excludeTopics", "encoding"}, {"topics"})
        topics = strings(config["topics"], path + ".topics", nonempty=True)
        if seen.intersection(topics):
            raise ConfigError("Topics or identical regex subscriptions overlap across signals")
        seen.update(topics)
        for topic in topics:
            if not topic.startswith("^") and (
                not re.fullmatch(r"[A-Za-z0-9_.-]{1,249}", topic) or topic in (".", "..")
            ):
                raise ConfigError(f"{path}.topics contains an invalid literal Kafka topic")
        encoding = choice(config.get("encoding", "otlpProto"), path + ".encoding",
                          ("otlpProto", "otapProto"))
        native[signal] = {
            "topics": topics,
            "encoding": {"otlpProto": "otlp_proto", "otapProto": "otap_proto"}[encoding],
        }
        if "excludeTopics" in config:
            excludes = strings(config["excludeTopics"], path + ".excludeTopics")
            if excludes and not any(topic.startswith("^") for topic in topics):
                raise ConfigError(f"{path}.excludeTopics requires a regex subscription")
            native[signal]["exclude_topics"] = excludes
    if not seen:
        raise ConfigError("At least one signal with a non-empty topics array is required")

    delivery = object_fields(k.get("delivery", {}), "delivery", {"readCommitted"})
    committed = boolean(delivery.get("readCommitted", False), "delivery.readCommitted")
    native["isolation_level"] = "read_committed" if committed else "read_uncommitted"

    enrichment = object_fields(k.get("enrichment", {}), "enrichment", {"resourceAttributesFromHeaders"})
    rules = enrichment.get("resourceAttributesFromHeaders", [])
    if not isinstance(rules, list):
        raise ConfigError("resourceAttributesFromHeaders must be an array")
    headers = {}
    for rule in rules:
        rule = object_fields(rule, "header extraction", {"header", "attribute", "type"},
                             {"header", "attribute", "type"})
        header = text(rule["header"], "header extraction.header")
        if header in headers:
            raise ConfigError("Duplicate extraction header would overwrite an earlier rule")
        headers[header] = {
            "key": text(rule["attribute"], "header extraction.attribute"),
            "value_type": choice(rule["type"], "header extraction.type", ("string", "bool", "int", "float")),
        }
    if headers:
        native["resource_attrs_from_headers"] = headers

    observability = object_fields(k.get("observability", {}), "observability", {"consumerLag"})
    lag = object_fields(observability.get("consumerLag", {}), "consumerLag", {"enabled", "refreshInterval"})
    enabled = boolean(lag.get("enabled", False), "consumerLag.enabled")
    interval = duration_ms(lag["refreshInterval"]) if "refreshInterval" in lag else None
    if enabled:
        if interval is None:
            raise ConfigError("consumerLag.refreshInterval is required when enabled")
        native["lag_refresh_interval_ms"] = interval

    # The source-derived node ID and the harness sink ID must never collide.
    sink = f"{name}-sink"
    return {
        "version": "otel_dataflow/v1",
        "engine": {"telemetry": {"logs": {"level": "info"}}},
        "groups": {"default": {"pipelines": {"main": {
            "nodes": {
                name: {"type": "urn:otel:receiver:kafka", "config": native},
                sink: {"type": "urn:otel:exporter:noop"},
            },
            "connections": [{"from": name, "to": sink}],
        }}}},
    }


def write_config(path, config):
    path = Path(path)
    if path.suffix.lower() not in (".yaml", ".yml"):
        raise ConfigError("The output must use a .yaml or .yml extension")
    contents = yaml.dump(config, Dumper=NativeDumper, sort_keys=False, allow_unicode=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents, encoding="utf-8", newline="\n")


def validate_with_image(path, bindings, image):
    environment = runtime_environment(bindings)
    variables = sorted({
        value for binding in validate_bindings(bindings).values()
        for key, value in binding.items() if key.endswith("Env")
    })
    container = "kafka-schema-validate-" + uuid.uuid4().hex
    command = ["docker", "run", "--rm", "--name", container, "--pull", "never", "--network", "none",
               "--mount", f"type=bind,source={Path(path).resolve()},target=/input.yaml,readonly"]
    for variable in variables:
        command.extend(["-e", variable + "__JSON"])
    command.extend([image, "--config", "file:/input.yaml", "--validate-and-exit"])
    try:
        result = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired:
        # Killing the Docker CLI does not necessarily stop its container.
        cleanup = subprocess.run(["docker", "rm", "-f", container],
                                 capture_output=True, text=True, timeout=30)
        if cleanup.returncode:
            raise ConfigError(
                f"Native validation timed out; container cleanup failed for {container}"
            ) from None
        raise ConfigError("Native validation timed out; its container was removed") from None
    if result.returncode:
        details = result.stdout + result.stderr
        for variable in variables:
            raw = environment[variable]
            for value in (environment[variable + "__JSON"], json.dumps(raw)[1:-1], raw):
                details = details.replace(value, "[redacted]")
        raise ConfigError(f"Native validation failed (exit {result.returncode}):\n{details}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--bindings", type=Path, required=True)
    parser.add_argument("--instance-id", required=True)
    parser.add_argument("--membership", choices=("dynamic", "static"), required=True)
    parser.add_argument("--rebalance-strategy",
                        choices=("range", "round_robin", "cooperative_sticky", "native-default"), required=True)
    parser.add_argument("--auth-policy", choices=("sasl-only", "authenticated", "allow-anonymous"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--validate-image", help="Validate with an existing Kafka-enabled Docker engine image")
    args = parser.parse_args()
    try:
        if args.output.resolve() in (args.input.resolve(), args.bindings.resolve()):
            raise ConfigError("The output must not overwrite customer input or bindings")
        bindings = load_yaml(args.bindings)
        config = translate(
            load_yaml(args.input), bindings,
            instance_id=args.instance_id, membership=args.membership,
            rebalance_strategy=args.rebalance_strategy, auth_policy=args.auth_policy,
        )
        write_config(args.output, config)
        if args.validate_image:
            validate_with_image(args.output, bindings, args.validate_image)
    except (ConfigError, OSError) as exc:
        print(f"Translation failed: {exc}", file=sys.stderr)
        return 1
    status = "native validation passed" if args.validate_image else "native engine validation is still required"
    print(f"Generated {args.output}; {status}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
