# Event schema v1

One JSON object (UTF-8) per Kafka message.

- **Topic:** `sensor.raw` (3 partitions)
- **Message key:** `machine_id` as a string, so all events of one machine go to the same
  partition and stay in order.
- **Owner:** Omar (Ingestion). Any change goes through a pull request reviewed by Ahmed and
  Kerollos.

## Common fields (every event)

| Field | Type | Meaning |
|---|---|---|
| `event_id` | string (UUID4) | Unique id of this event. |
| `event_type` | string | `reading`, `error` or `failure`. |
| `machine_id` | integer | Machine number, 1 to 100 (`machineID` in the dataset). |
| `event_time` | string, ISO-8601 UTC | When the event happened in the dataset, e.g. `2015-01-01T06:00:00Z`. |
| `ingest_time` | string, ISO-8601 UTC | When the simulator sent the event. |
| `schema_version` | integer | `1`. |
| `source` | string | `dataset` for replayed rows, `injected` for events created by fault injection. |

## `reading` events

Sent for every row of `PdM_telemetry.csv`. Extra fields:

| Field | Type | Dataset column |
|---|---|---|
| `volt` | float | `volt` |
| `rotate` | float | `rotate` |
| `pressure` | float | `pressure` |
| `vibration` | float | `vibration` |

```json
{
  "event_id": "5b1d6c0e-3f1a-4c1e-9a55-0d3f6a2b7c11",
  "event_type": "reading",
  "machine_id": 1,
  "event_time": "2015-01-01T06:00:00Z",
  "ingest_time": "2026-11-07T20:00:00Z",
  "schema_version": 1,
  "source": "dataset",
  "volt": 176.217853,
  "rotate": 418.504078,
  "pressure": 113.077935,
  "vibration": 45.087686
}
```

## `error` and `failure` events (week 2, proposed)

Built from `PdM_errors.csv` and `PdM_failures.csv`. They carry no sensor fields. Field names
below are a proposal and are finalized in week 2.

| Event type | Extra field | Type | Dataset column | Example value |
|---|---|---|---|---|
| `error` | `error_id` | string | `errorID` | `error1` |
| `failure` | `component` | string | `failure` | `comp4` |

## Rules

- **Timestamps:** the dataset has no timezone. We treat `datetime` as UTC and write it with a
  trailing `Z`.
- **Not applicable fields are omitted**, not set to null (an `error` event has no `volt`).
- **`event_time` is replayed historical time** (2015 to 2016). **`ingest_time` is the real
  current time.** Windows and watermarks should use `event_time`.
- **Late or out-of-order events** (week 3) keep their original `event_time`.
- **Malformed events** (week 3, fault injection) deliberately break this schema, so the
  processing job can send them to the dead-letter topic.

## To confirm with Ahmed and Kerollos

1. Timestamps are treated as UTC.
2. The `source` field (`dataset` or `injected`).
3. Field names for `error` and `failure` events.
4. Windows use `event_time`, not `ingest_time`.
