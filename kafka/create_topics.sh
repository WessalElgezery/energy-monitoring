#!/usr/bin/env bash
set -e

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP_SERVER="${KAFKA_INTERNAL_SERVERS:-kafka:9092}"

docker exec -i "${KAFKA_CONTAINER}" kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create --if-not-exists \
  --topic energy.raw \
  --partitions 3 \
  --replication-factor 1

docker exec -i "${KAFKA_CONTAINER}" kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create --if-not-exists \
  --topic energy.cleaned \
  --partitions 3 \
  --replication-factor 1

docker exec -i "${KAFKA_CONTAINER}" kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create --if-not-exists \
  --topic energy.dlq \
  --partitions 1 \
  --replication-factor 1

docker exec -i "${KAFKA_CONTAINER}" kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --list
