# FactorySentinel contract (v2)

Everything two people need to agree on so they can build in parallel: Track A (the stream)
writes what is described here, Track B (storage, warehouse, dashboards) reads it. Changing this
file needs both people to agree.

The event itself is defined in [event-schema.md](event-schema.md). This file adds everything
after Kafka.

## 1. The event (summary of event-schema.md)

```json
{
  "event_id": "5b1d6c0e-3f1a-4c1e-9a55-0d3f6a2b7c11",
  "event_type": "reading",
  "machine_id": 1,
  "event_time": "2015-07-01T06:00:00Z",
  "ingest_time": "2026-11-27T18:00:00Z",
  "schema_version": 1,
  "source": "dataset",
  "volt": 176.217853,
  "rotate": 418.504078,
  "pressure": 113.077935,
  "vibration": 45.087686
}
```

Two clocks, and it matters which one you use:

| Field | Clock | Used for |
|---|---|---|
| `event_time` | Dataset time (2015). One reading per machine per hour. | Windows, watermarks, the warehouse, the History dashboard. |
| `ingest_time` | Real time, when the simulator sent the event. | The Live dashboard and freshness checks. |

Speed: `SIM_SPEED=3600` replays one dataset hour per real second, so 100 machines send
100 events per second and a dataset day passes every 24 seconds.

Split of the year: January to June 2015 is **history** (loaded in one batch from the CSV).
The stream replays from `SIM_START=2015-07-01`.

Only `event_type = "reading"` is built today. `error` and `failure` events stay "proposed".

## 2. Kafka topics

| Topic | Partitions | Content |
|---|---|---|
| `sensor.raw` | 3 | Every event from the simulator, good or bad. Key = `machine_id`. |
| `sensor.dlq` | 1 | Rejected events: `{"reason": "...", "raw": "<original text>", "rejected_at": "<UTC>"}` |
| `sensor.alerts` | 1 | One JSON message per new alert (same fields as the `alerts` table). |

## 3. Validation rules

An event is **rejected** when any of these is true. The reason is the first rule that fails.

1. The value is not a JSON object.
2. A field from the schema is missing or has the wrong type (`machine_id` and `schema_version`
   are integers, the four sensors are numbers, the rest are strings).
3. `event_type` is not `reading`.
4. `machine_id` is not between 1 and 100.
5. `event_time` or `ingest_time` cannot be parsed, or `event_time` is more than 10 minutes
   later than `ingest_time`. The 10 minutes allow for clocks that disagree slightly.
6. A sensor is outside its possible range: `volt` 0 to 500, `rotate` 0 to 1000, `pressure` 0 to
   300, `vibration` 0 to 150.
7. `source` is not `dataset` or `injected`.

Rejected events go to `sensor.dlq` and to the `rejected_events` table. They never stop the job.

## 4. Alert rules

Numbers come from the data profile (mean and standard deviation of each sensor) and from what
the daily averages look like on the day before a recorded failure.

Per reading (`scope = reading`, severity `warning`): one extreme value, about 4.5 standard
deviations from the fleet mean.

| Rule | Condition |
|---|---|
| `HIGH_VOLT` | `volt` > 240 |
| `LOW_ROTATION` | `rotate` < 210 |
| `HIGH_PRESSURE` | `pressure` > 150 |
| `HIGH_VIBRATION` | `vibration` > 65 |

Per finished dataset day (`scope = day`, severity `critical`): the 24-hour average of one
machine has shifted. Only days with at least 20 readings are judged.

| Rule | Condition | Typical failure that follows |
|---|---|---|
| `VOLT_HIGH_24H` | average `volt` > 185 | comp1 |
| `ROTATION_LOW_24H` | average `rotate` < 410 | comp2 |
| `PRESSURE_HIGH_24H` | average `pressure` > 112 | comp3 |
| `VIBRATION_HIGH_24H` | average `vibration` > 46 | comp4 |

`alert_id = md5(machine_id | rule | time)`, where time is the reading's `event_time` or the
day's `window_start`, written like `2015-07-01T06:00:00Z`. The same alert can never be stored
twice.

## 5. Postgres tables

Database `factorysentinel`. DDL is in `warehouse/ddl/`; create everything with `make db-init`.

Live path (written by the Spark job, read by Grafana):

| Table | One row per | Primary key | Write rule |
|---|---|---|---|
| `live_readings` | reading | `(machine_id, event_time)` | `ON CONFLICT ... DO UPDATE` |
| `agg_machine_day` | machine per finished dataset day | `(machine_id, window_start)` | `ON CONFLICT ... DO UPDATE` |
| `alerts` | alert | `alert_id` | `ON CONFLICT DO NOTHING` |
| `rejected_events` | rejected Kafka message | `(source_partition, source_offset)` | `ON CONFLICT DO NOTHING` |

Batch path (written by SQL in `warehouse/load/`):

| Table | Grain | Filled from |
|---|---|---|
| `stg_*` (5 tables) | the dataset CSV files as they are | `scripts/load_reference.sh` |
| `dim_machine` | machine | `stg_machines` |
| `dim_date` | calendar day | generated |
| `fact_machine_day` | machine per day; `load_source` is `history` or `stream` | `stg_telemetry` (history), `live_readings` (stream) |
| `fact_failure` | recorded component failure | `stg_failures` |
| `dq_results` | data-quality check per run | `v_quality_checks` |

Views: `v_machine_baseline`, `v_live_vs_baseline`, `v_failure_detection`, `v_alert_scorecard`,
`v_quality_checks`.

## 6. Services and ports

| Service | Inside Docker | From your laptop | Started by |
|---|---|---|---|
| Kafka | `kafka:9092` | `localhost:29092` | `make up` |
| Postgres | `postgres:5432` | `localhost:5432` | `make up` |
| Grafana | | http://localhost:3000 | `make up` |
| Simulator, Spark job | | Spark UI http://localhost:4040 | `make stream` |
| Airflow | | http://localhost:8081 | `make up-all` |
| Prometheus | | http://localhost:9090 | `make up-all` |

## 7. Environment variables (`.env`)

```
POSTGRES_USER=factorysentinel
POSTGRES_PASSWORD=change_me
POSTGRES_DB=factorysentinel
KAFKA_BOOTSTRAP=kafka:9092
GRAFANA_ADMIN_PASSWORD=change_me
AIRFLOW_ADMIN_PASSWORD=change_me
SIM_SPEED=3600
SIM_START=2015-07-01
SIM_MAX_HOURS=0
SPARK_MASTER=local[2]
```

Only needed for a cloud endpoint (Fabric Eventstream or Azure Event Hubs). Put connection
strings in single quotes, because they contain `;` and `=`.

```
CLOUD_BOOTSTRAP=<bootstrap server>            # copy clean events to a second, cloud endpoint
CLOUD_TOPIC=<topic name>
CLOUD_SASL_PASSWORD='<connection string>'
KAFKA_SECURITY_PROTOCOL=SASL_SSL              # only when the cloud endpoint replaces local Kafka
KAFKA_SASL_PASSWORD='<connection string>'
POSTGRES_HOST=<server>.postgres.database.azure.com
POSTGRES_SSLMODE=require
```
