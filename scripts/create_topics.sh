#!/usr/bin/env bash
# Create the sensor.raw topic (3 partitions; events are keyed by machine_id). Safe to re-run.
set -euo pipefail
cd "$(dirname "$0")/.."
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh
docker compose exec -T kafka "$KAFKA_TOPICS" --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic sensor.raw --partitions 3 --replication-factor 1
docker compose exec -T kafka "$KAFKA_TOPICS" --bootstrap-server localhost:9092 \
  --describe --topic sensor.raw
