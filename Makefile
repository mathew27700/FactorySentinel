.PHONY: up up-all down logs ps test lint topics db-init load-reference seed stream reset-live quality psql

# Core services only: Kafka, Postgres, Grafana. Enough for both tracks to start.
up:
	docker compose up -d kafka postgres grafana

# Everything, including Airflow and Prometheus (needs about 6 GB of RAM for Docker).
up-all:
	docker compose --profile batch --profile monitoring up -d

down:
	docker compose --profile batch --profile monitoring --profile cluster down

logs:
	docker compose logs -f --tail=100 $(s)

ps:
	docker compose ps

test:
	pytest tests/unit -q

lint:
	ruff check .

topics:
	scripts/create_topics.sh

db-init:
	scripts/db_init.sh

load-reference:
	scripts/load_reference.sh

seed:
	scripts/psql.sh -q -f - < warehouse/dev/seed_live.sql

# Start the live path: the simulator and the Spark job.
stream:
	docker compose up -d --build simulator streaming

reset-live:
	scripts/reset_live.sh

quality:
	scripts/run_quality.sh

psql:
	scripts/psql.sh
