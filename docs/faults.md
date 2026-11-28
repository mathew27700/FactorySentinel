# Faults and restart test (A6)

Observed on a clean `make reset-live && make stream` run. Each row says what the tables showed when I checked, not what the guide predicts.

| Fault | Command | What changed | Observed |
|---|---|---|---|
| spike | `--machine 7 --type spike --seconds 20` | `alerts` | 24 s after start: HIGH_VOLT x15 and HIGH_VIBRATION x14 for machine 7 (warning) |
| drift | `--machine 9 --type drift --seconds 60` | `alerts` (24h rules) | 85 s after start: VIBRATION_HIGH_24H x3 (critical) for machine 9. HIGH_VIBRATION x2 (warning) also present, origin not checked |
| malformed | `--machine 3 --type malformed --seconds 20` | `rejected_events` | 28 s after start: rows 0 -> 19. Job stayed up. Machine 3 day 2015-07-15 has 19 readings in `agg_machine_day` |
| late | `--machine 5 --type late --seconds 60` | `agg_machine_day` | 85 s after start: day 2015-07-16 has 22 in the window table but 24 in `live_readings` |
| duplicate | `--machine 11 --type duplicate --seconds 20` | nothing visible | `live_readings` 492 rows = 492 distinct event_time. Daily count min = max = 24 |

Not checked: the warehouse quality checks (incomplete_day, window_does_not_match_readings) and the sensor.dlq topic contents.

## Restart test

`docker compose kill streaming` then `docker compose up -d streaming`, 60 s later: `live_readings` total 19200 = 19200 distinct (machine_id, event_time). Every finished day has exactly 24 readings per machine (min = max = 24).

## Lake

`data/lake/readings` holds `date=2015-07-01`, `date=2015-07-02`, `date=2015-07-03` and `_spark_metadata`. The file sink does not drop a message the simulator sent twice, so a duplicate stays twice in the lake.
