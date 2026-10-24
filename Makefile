.PHONY: up down logs ps test lint

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps

test:
	pytest tests/unit -q

lint:
	ruff check .
