# Week 1 · Omar (Ingestion)

## Done this week
- Set up WSL, Docker and the Python environment; Kafka and Postgres run healthy.
- Downloaded the Azure Predictive Maintenance dataset and profiled all five files
  (`docs/data-profile.md`): 876,100 hourly readings, 100 machines, no missing values.
- Wrote the datasource decision (`docs/decisions/001-datasource.md`).
- Wrote event schema v1 (`docs/event-schema.md`), including a `source` field.
- Wrote `scripts/download_data.sh` and `scripts/create_topics.sh`; created topic `sensor.raw`
  with 3 partitions.
- Wrote simulator v0 with a unit test. It replays the telemetry CSV as schema v1 events keyed by
  `machine_id`.
- Sent 2,400 real events (24 hours x 100 machines) to `sensor.raw`. The offsets of the 3
  partitions add up to 2,400.

## Pull requests merged
- None merged yet. Open and waiting for review:
  - #2 docs: data profile, datasource decision and event schema v1
  - #3 chore(scripts): download_data and create_topics scripts
  - #5 feat(simulator): replay telemetry csv to kafka

## Not done, and why
- Ahmed has not yet confirmed that his job reads the events from `sensor.raw`. I sent them and am
  waiting for his confirmation.
- Nothing is merged yet because the PRs still need a reviewer. PRs #3 and #5 were opened after
  the 8:00 pm deadline.

## Blockers / help I need
- A reviewer for PRs #2, #3 and #5. The schema in #2 needs Ahmed and Kerollos, especially: UTC
  timestamps, the `source` field, field names for error/failure events, and windows on
  `event_time`.
- There are no issues on the board, so branches have no issue numbers.

## Plan for next week
- Finish `simulator/Dockerfile` and the compose service so the simulator starts with the stack.
- Add error and failure events from the other dataset files, and update the schema doc.
- Add `simulator/config.yaml` (machines and date range).
- Sit with Ahmed to fix any parsing problems on his side.

## What I learned
- What a Kafka topic, partition and key are, and why the key keeps one machine's events in order.
- The branch, commit, pull request and review flow.

## Hours spent (rough)
- About 4 hours on Saturday; setup done earlier in the week is not included.
