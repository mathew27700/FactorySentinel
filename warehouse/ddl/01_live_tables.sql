-- Live path tables. Written by the Spark job, read by Grafana. See docs/contract.md, section 5.

CREATE TABLE IF NOT EXISTS live_readings (
    machine_id   integer          NOT NULL,
    event_time   timestamptz      NOT NULL,   -- dataset time (2015)
    event_id     uuid             NOT NULL,
    ingest_time  timestamptz      NOT NULL,   -- when the simulator sent it (real clock)
    source       text             NOT NULL,   -- dataset | injected
    volt         double precision NOT NULL,
    rotate       double precision NOT NULL,
    pressure     double precision NOT NULL,
    vibration    double precision NOT NULL,
    stored_at    timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (machine_id, event_time)
);
CREATE INDEX IF NOT EXISTS ix_live_readings_ingest ON live_readings (ingest_time DESC);

CREATE TABLE IF NOT EXISTS agg_machine_day (
    machine_id     integer     NOT NULL,
    window_start   timestamptz NOT NULL,      -- start of the dataset day (UTC)
    window_end     timestamptz NOT NULL,
    reading_count  integer     NOT NULL,
    avg_volt       double precision,
    avg_rotate     double precision,
    avg_pressure   double precision,
    avg_vibration  double precision,
    max_volt       double precision,
    min_rotate     double precision,
    max_pressure   double precision,
    max_vibration  double precision,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (machine_id, window_start)
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id    text             PRIMARY KEY,
    machine_id  integer          NOT NULL,
    rule        text             NOT NULL,
    severity    text             NOT NULL CHECK (severity IN ('warning', 'critical')),
    scope       text             NOT NULL CHECK (scope IN ('reading', 'day')),
    metric      text             NOT NULL,
    value       double precision NOT NULL,
    threshold   double precision NOT NULL,
    event_time  timestamptz      NOT NULL,    -- dataset time of the reading, or start of the day
    created_at  timestamptz      NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_alerts_created ON alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS ix_alerts_machine_time ON alerts (machine_id, event_time);

CREATE TABLE IF NOT EXISTS rejected_events (
    source_partition integer     NOT NULL,
    source_offset    bigint      NOT NULL,
    reason           text        NOT NULL,
    raw_payload      text        NOT NULL,
    rejected_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_partition, source_offset)
);
CREATE INDEX IF NOT EXISTS ix_rejected_time ON rejected_events (rejected_at DESC);
