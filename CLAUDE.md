# CLAUDE.md

Guidance for Claude Code (and humans) working in this repository.

## Project

FactorySentinel is a real-time IoT sensor pipeline for a university data engineering
graduation project. Everything runs from one `docker compose up`.

- **Live path:** a Python simulator (`simulator/`) sends machine sensor events to Kafka. Spark
  Structured Streaming (`streaming/`) validates them, routes bad events to a dead-letter topic,
  computes windowed aggregates and detects anomalies. Results go to Postgres and a live Grafana
  dashboard.
- **Batch path:** raw events are written to a Parquet lake. Airflow (`airflow/dags/`) loads them
  into a star-schema warehouse (`warehouse/`) with data-quality checks.
- **Monitoring path:** Prometheus and Grafana (`monitoring/`) monitor the pipeline itself.

See `docs/architecture.md` for the diagram.

## Workflow

- Never commit to `main`. All changes go through a pull request.
- Branch names: `type/issue-number-short-description`, e.g. `feat/12-anomaly-detection`.
  Types: `feat`, `fix`, `chore`, `docs`, `test`, `refactor`.
- Rebase on `origin/main` before opening a PR.
- One PR per issue, ideally under about 400 changed lines.
- The PR body includes `Closes #N`.
- Squash merge after 1 approval and green CI.

Custom commands: `/start-task <issue-number>` and `/open-pr`.

## Commits

Conventional Commits, in the form `type(area): message`, e.g.
`feat(streaming): add 1-minute windowed averages`. Areas match the `area:` labels:
`simulator`, `streaming`, `warehouse`, `airflow`, `monitoring`, `devops`, `docs`.

## Commands

- `make up` — start the stack (needs `.env`; copy it from `.env.example`)
- `make down` — stop the stack
- `make test` — unit tests (`pytest tests/unit -q`)
- `make lint` — `ruff check .`

## Rules

- Never commit `.env`, data, checkpoints or Parquet files. Only `.env.example` is committed.
- Pin Docker image versions to exact tags; never use `latest`. Keep the Spark image tag and the
  `pyspark` version in `requirements-dev.txt` in sync.
- Any change to `docker-compose.yml` must still pass `docker compose config -q`.
- Python targets 3.11 with a line length of 100; `make lint` and `make test` must pass before
  opening a PR.
