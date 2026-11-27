#!/usr/bin/env bash
# Start a fresh run: stop the stream, empty the live tables, forget Kafka offsets and Spark state.
# The warehouse history (dims, history facts, failures) is kept.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose stop simulator streaming
scripts/psql.sh -c "TRUNCATE live_readings, agg_machine_day, alerts, rejected_events"
scripts/psql.sh -c "DELETE FROM fact_machine_day WHERE load_source = 'stream'" \
  || echo "(no warehouse tables yet, skipped)"
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh
for t in sensor.raw sensor.dlq sensor.alerts; do
  docker compose exec -T kafka "$KAFKA_TOPICS" --bootstrap-server localhost:9092 --delete --topic "$t" || true
done
sleep 5   # topic deletion is not instant
scripts/create_topics.sh
docker compose run --rm --no-deps --entrypoint sh streaming -c 'rm -rf /data/checkpoints /data/lake'
rm -f runtime/faults.json
echo "clean. Start again with: make stream"
