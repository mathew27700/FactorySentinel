-- Staging tables: the five dataset CSV files, loaded as they are by scripts/load_reference.sh.

CREATE TABLE IF NOT EXISTS stg_telemetry (
    datetime timestamp, machine_id integer,
    volt double precision, rotate double precision, pressure double precision, vibration double precision
);
CREATE TABLE IF NOT EXISTS stg_machines (machine_id integer, model text, age integer);
CREATE TABLE IF NOT EXISTS stg_failures (datetime timestamp, machine_id integer, failure text);
CREATE TABLE IF NOT EXISTS stg_errors   (datetime timestamp, machine_id integer, error_id text);
CREATE TABLE IF NOT EXISTS stg_maint    (datetime timestamp, machine_id integer, comp text);
