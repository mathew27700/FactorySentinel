-- Star schema for history. Two dimensions, two facts.

CREATE TABLE IF NOT EXISTS dim_machine (
    machine_key serial  PRIMARY KEY,
    machine_id  integer NOT NULL UNIQUE,
    model       text    NOT NULL,
    age_years   integer NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key    integer PRIMARY KEY,          -- 20150701
    full_date   date    NOT NULL UNIQUE,
    year        integer NOT NULL,
    month       integer NOT NULL,
    day_name    text    NOT NULL,
    is_weekend  boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_machine_day (
    machine_key    integer NOT NULL REFERENCES dim_machine (machine_key),
    date_key       integer NOT NULL REFERENCES dim_date (date_key),
    reading_count  integer NOT NULL,
    avg_volt       double precision NOT NULL,
    avg_rotate     double precision NOT NULL,
    avg_pressure   double precision NOT NULL,
    avg_vibration  double precision NOT NULL,
    max_volt       double precision NOT NULL,
    min_rotate     double precision NOT NULL,
    max_pressure   double precision NOT NULL,
    max_vibration  double precision NOT NULL,
    alert_count    integer NOT NULL DEFAULT 0,
    load_source    text    NOT NULL CHECK (load_source IN ('history', 'stream')),
    loaded_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (machine_key, date_key)
);
COMMENT ON TABLE fact_machine_day IS 'Grain: one row per machine per dataset day (UTC).';

CREATE TABLE IF NOT EXISTS fact_failure (
    machine_key  integer     NOT NULL REFERENCES dim_machine (machine_key),
    date_key     integer     NOT NULL REFERENCES dim_date (date_key),
    failed_at    timestamptz NOT NULL,
    component    text        NOT NULL,        -- comp1 .. comp4
    PRIMARY KEY (machine_key, failed_at, component)
);
COMMENT ON TABLE fact_failure IS 'Grain: one row per recorded component failure.';

CREATE TABLE IF NOT EXISTS dq_results (
    run_at      timestamptz NOT NULL DEFAULT now(),
    check_name  text        NOT NULL,
    failed_rows bigint      NOT NULL,
    passed      boolean     NOT NULL,
    detail      text
);
