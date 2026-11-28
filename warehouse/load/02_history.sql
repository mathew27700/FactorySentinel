-- History: the first half of 2015, loaded in one batch from the CSV staging table.
-- The stream replays the second half (SIM_START=2015-07-01), so the two never overlap.
INSERT INTO fact_machine_day
    (machine_key, date_key, reading_count, avg_volt, avg_rotate, avg_pressure, avg_vibration,
     max_volt, min_rotate, max_pressure, max_vibration, load_source)
SELECT m.machine_key, to_char(t.datetime, 'YYYYMMDD')::integer, count(*),
       avg(t.volt), avg(t.rotate), avg(t.pressure), avg(t.vibration),
       max(t.volt), min(t.rotate), max(t.pressure), max(t.vibration), 'history'
FROM stg_telemetry t
JOIN dim_machine m ON m.machine_id = t.machine_id
WHERE t.datetime < timestamp '2015-07-01'
GROUP BY 1, 2
ON CONFLICT (machine_key, date_key) DO NOTHING;

INSERT INTO fact_failure (machine_key, date_key, failed_at, component)
SELECT m.machine_key, to_char(f.datetime, 'YYYYMMDD')::integer, f.datetime AT TIME ZONE 'UTC', f.failure
FROM stg_failures f
JOIN dim_machine m ON m.machine_id = f.machine_id
ON CONFLICT DO NOTHING;
