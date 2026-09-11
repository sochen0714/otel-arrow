#!/bin/sh
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
set -eu
umask 077

# The launcher generates only these characters. Refuse other input rather than
# interpolate arbitrary strings into the broker's JAAS configuration.
case "${KAFKA_DEMO_USERNAME:?}" in
  *[!A-Za-z0-9_]*) echo "Invalid generated username" >&2; exit 1 ;;
esac
case "${KAFKA_DEMO_PASSWORD:?}${KAFKA_DEMO_STORE_PASSWORD:?}" in
  *[!A-Za-z0-9]*) echo "Invalid generated password" >&2; exit 1 ;;
esac
if [ -e /private/kafka.keystore.jks ]; then
  if [ "${KAFKA_DEMO_REUSE_CERTS:-false}" = true ]; then
    for file in kafka.keystore.jks kafka.truststore.jks kafka_keystore_creds \
      kafka_sslkey_creds kafka_truststore_creds kafka_server_jaas.conf kafka.crt; do
      test -s "/private/$file" || { echo "Incomplete certificates; recreate demo volumes" >&2; exit 1; }
    done
    printf 'KafkaServer {\n org.apache.kafka.common.security.plain.PlainLoginModule required\n user_%s="%s";\n};\n' \
      "$KAFKA_DEMO_USERNAME" "$KAFKA_DEMO_PASSWORD" | cmp -s - /private/kafka_server_jaas.conf || {
        echo "Stored SASL credentials do not match; recreate demo volumes" >&2; exit 1;
      }
    for file in kafka_keystore_creds kafka_sslkey_creds kafka_truststore_creds; do
      printf '%s' "$KAFKA_DEMO_STORE_PASSWORD" | cmp -s - "/private/$file" || {
        echo "Stored certificate password does not match; recreate demo volumes" >&2; exit 1;
      }
    done
    openssl x509 -checkend 0 -noout -in /public/ca.crt
    openssl x509 -checkend 0 -noout -in /private/kafka.crt
    echo "Reusing this project's unexpired certificates and matching credentials."
    exit 0
  fi
  echo "Refusing to overwrite an existing project's certificates" >&2
  exit 1
fi
chmod 700 /private
chmod 755 /public

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout /private/ca.key -out /public/ca.crt -days 2 \
  -subj "/CN=OTAP schema translation local CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"
openssl req -newkey rsa:2048 -nodes \
  -keyout /private/kafka.key -out /private/kafka.csr \
  -subj "/CN=kafka"
printf '%s\n' 'subjectAltName=DNS:kafka' 'extendedKeyUsage=serverAuth' \
  > /private/kafka.ext
openssl x509 -req -in /private/kafka.csr \
  -CA /public/ca.crt -CAkey /private/ca.key \
  -CAserial /private/ca.srl -CAcreateserial \
  -out /private/kafka.crt -days 2 -extfile /private/kafka.ext
openssl pkcs12 -export -in /private/kafka.crt -inkey /private/kafka.key \
  -certfile /public/ca.crt -name kafka -out /private/kafka.p12 \
  -passout env:KAFKA_DEMO_STORE_PASSWORD
keytool -importkeystore -noprompt \
  -srckeystore /private/kafka.p12 -srcstoretype PKCS12 \
  -srcstorepass:env KAFKA_DEMO_STORE_PASSWORD \
  -destkeystore /private/kafka.keystore.jks -deststoretype JKS \
  -deststorepass:env KAFKA_DEMO_STORE_PASSWORD \
  -destkeypass:env KAFKA_DEMO_STORE_PASSWORD
keytool -importcert -noprompt -alias local-ca -file /public/ca.crt \
  -keystore /private/kafka.truststore.jks -storetype JKS \
  -storepass:env KAFKA_DEMO_STORE_PASSWORD

printf '%s' "$KAFKA_DEMO_STORE_PASSWORD" > /private/kafka_keystore_creds
printf '%s' "$KAFKA_DEMO_STORE_PASSWORD" > /private/kafka_sslkey_creds
printf '%s' "$KAFKA_DEMO_STORE_PASSWORD" > /private/kafka_truststore_creds
printf 'KafkaServer {\n org.apache.kafka.common.security.plain.PlainLoginModule required\n user_%s="%s";\n};\n' \
  "$KAFKA_DEMO_USERNAME" "$KAFKA_DEMO_PASSWORD" \
  > /private/kafka_server_jaas.conf

# Only the broker (uid 1000) needs private material. Engines mount the separate
# public volume containing only ca.crt, readable by their nonroot uid.
find /private -type f -exec chmod 400 {} \;
chown -R 1000:1000 /private
chmod 644 /public/ca.crt
echo "Generated a two-day local CA and broker credentials."
