#!/usr/bin/env bash
set -e

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP_SERVER="${KAFKA_INTERNAL_SERVERS:-kafka:9092}"
KAFKA_TOPICS_BIN="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"

# Fallback if kafka-topics.sh is in PATH instead of /opt/kafka/bin
if ! docker exec -i "${KAFKA_CONTAINER}" test -x "${KAFKA_TOPICS_BIN}"; then
  KAFKA_TOPICS_BIN="kafka-topics.sh"
fi

docker exec -i "${KAFKA_CONTAINER}" "${KAFKA_TOPICS_BIN}" \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create --if-not-exists \
  --topic energy.raw \
  --partitions 3 \
  --replication-factor 1

docker exec -i "${KAFKA_CONTAINER}" "${KAFKA_TOPICS_BIN}" \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create --if-not-exists \
  --topic energy.cleaned \
  --partitions 3 \
  --replication-factor 1

docker exec -i "${KAFKA_CONTAINER}" "${KAFKA_TOPICS_BIN}" \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create --if-not-exists \
  --topic energy.dlq \
  --partitions 1 \
  --replication-factor 1

docker exec -i "${KAFKA_CONTAINER}" "${KAFKA_TOPICS_BIN}" \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --list
