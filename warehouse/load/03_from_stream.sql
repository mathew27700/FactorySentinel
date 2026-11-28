-- Roll the live readings up into the warehouse. Run it as often as you like (Airflow runs it hourly).
INSERT INTO fact_machine_day
    (machine_key, date_key, reading_count, avg_volt, avg_rotate, avg_pressure, avg_vibration,
     max_volt, min_rotate, max_pressure, max_vibration, alert_count, load_source)
SELECT m.machine_key, r.date_key, r.reading_count, r.avg_volt, r.avg_rotate, r.avg_pressure,
       r.avg_vibration, r.max_volt, r.min_rotate, r.max_pressure, r.max_vibration,
       coalesce(a.alert_count, 0), 'stream'
FROM (
    SELECT machine_id, to_char(event_time AT TIME ZONE 'UTC', 'YYYYMMDD')::integer AS date_key,
           count(*) AS reading_count,
           avg(volt) AS avg_volt, avg(rotate) AS avg_rotate,
           avg(pressure) AS avg_pressure, avg(vibration) AS avg_vibration,
           max(volt) AS max_volt, min(rotate) AS min_rotate,
           max(pressure) AS max_pressure, max(vibration) AS max_vibration
    FROM live_readings
    GROUP BY 1, 2
) r
JOIN dim_machine m ON m.machine_id = r.machine_id
LEFT JOIN (
    SELECT machine_id, to_char(event_time AT TIME ZONE 'UTC', 'YYYYMMDD')::integer AS date_key,
           count(*) AS alert_count
    FROM alerts
    GROUP BY 1, 2
) a ON a.machine_id = r.machine_id AND a.date_key = r.date_key
ON CONFLICT (machine_key, date_key) DO UPDATE SET
    reading_count = EXCLUDED.reading_count,
    avg_volt = EXCLUDED.avg_volt, avg_rotate = EXCLUDED.avg_rotate,
    avg_pressure = EXCLUDED.avg_pressure, avg_vibration = EXCLUDED.avg_vibration,
    max_volt = EXCLUDED.max_volt, min_rotate = EXCLUDED.min_rotate,
    max_pressure = EXCLUDED.max_pressure, max_vibration = EXCLUDED.max_vibration,
    alert_count = EXCLUDED.alert_count, load_source = 'stream', loaded_at = now()
WHERE fact_machine_day.load_source = 'stream';   -- never overwrite a history row
