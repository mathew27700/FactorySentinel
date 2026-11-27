#!/usr/bin/env bash
# Create the three topics from docs/contract.md. Safe to re-run.
#   sensor.raw     3 partitions, keyed by machine_id (one machine always lands in one partition)
#   sensor.dlq     rejected events with the reason
#   sensor.alerts  one message per new alert
set -euo pipefail
cd "$(dirname "$0")/.."
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh
create() {
  docker compose exec -T kafka "$KAFKA_TOPICS" --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1
}
create sensor.raw 3
create sensor.dlq 1
create sensor.alerts 1
docker compose exec -T kafka "$KAFKA_TOPICS" --bootstrap-server localhost:9092 --list
