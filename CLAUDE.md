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

## Contract

`docs/contract.md` is the source of truth for everything after Kafka: topics, validation rules,
alert rules, Postgres tables, ports and environment variables. `docs/event-schema.md` defines
the event.

- Never change a name, type, column or threshold from the contract without asking first.
- If the contract and the code disagree, the contract wins.
- Two clocks: `event_time` is dataset time (2015) and drives windows and the warehouse;
  `ingest_time` is the real clock and drives the Live dashboard.

## How to work

- Before coding, read the contract and the files you will touch. Give a plan of 5 lines or
  fewer, then build.
- Work in small steps. After each change, run it and show the proof: the command and its output.
  Never say "this should work".
- Fix errors yourself: read the logs (`make logs s=<service>`), find the cause, fix it, run again.
- Check current documentation (Context7) before using a Kafka, Spark, Grafana or Airflow option
  you are not sure about. The versions here are recent: Spark 4.2, Kafka 4.4, Airflow 3.3,
  Grafana 13.
- Keep it simple. No extra services, frameworks or abstractions beyond what the step asks for.
- Every write to Postgres must be safe to repeat (`INSERT ... ON CONFLICT`).
- When a step is done, explain what was built in plain language: each file in the order the data
  flows through it, then 3 things I must be able to explain to an examiner.

## Two tracks in parallel

- Track A (the stream) owns: `simulator/`, `streaming/`, `scripts/inject_fault.py`
- Track B (storage, warehouse, dashboards) owns: `warehouse/`, `monitoring/`, `airflow/`,
  `docs/`, `README.md`
- Shared, ask before changing: `docker-compose.yml`, `Makefile`, `.env.example`,
  `docs/contract.md`, `docs/event-schema.md`. In shared files, add only your own lines.

## Commands

- `make up` — start Kafka, Postgres and Grafana (needs `.env`; copy it from `.env.example`)
- `make up-all` — also start Airflow and Prometheus
- `make down` — stop the stack
- `make topics` — create the three Kafka topics
- `make db-init` — create every table and view
- `make load-reference` — load the dataset CSV files and the history half of the warehouse
- `make seed` — fill the live tables with replayed data, to build dashboards without the stream
- `make stream` — start the simulator and the Spark job
- `make reset-live` — empty the live tables, topics and checkpoints for a fresh run
- `make quality` — run the data-quality checks
- `make psql` — SQL prompt on the project database
- `make logs s=<service>` — follow one service's logs
- `make test` — unit tests (`pytest tests/unit -q`)
- `make lint` — `ruff check .`

## Rules

- Never commit `.env`, data, checkpoints or Parquet files. Only `.env.example` is committed.
- Pin Docker image versions to exact tags; never use `latest`. Keep the Spark image tag and the
  `pyspark` version in `requirements-dev.txt` in sync.
- Any change to `docker-compose.yml` must still pass `docker compose config -q`.
- Python targets 3.11 with a line length of 100; `make lint` and `make test` must pass before
  opening a PR.
