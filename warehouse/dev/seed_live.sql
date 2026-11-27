-- Fake a running stream so dashboards can be built before the real one exists.
-- Uses real dataset rows from staging (1 July to 13 September 2015) and spreads them over the
-- last 30 minutes, at the same pace the simulator uses: one dataset hour per real second.
-- The real stream later overwrites these rows (same primary key), so nothing needs cleaning up.

INSERT INTO live_readings (machine_id, event_time, event_id, ingest_time, source, volt, rotate, pressure, vibration)
SELECT machine_id, datetime AT TIME ZONE 'UTC', gen_random_uuid(),
       now() - (extract(epoch FROM timestamp '2015-09-14 00:00' - datetime) / 3600) * interval '1 second',
       'dataset', volt, rotate, pressure, vibration
FROM stg_telemetry
WHERE datetime >= timestamp '2015-07-01' AND datetime < timestamp '2015-09-14'
ON CONFLICT (machine_id, event_time) DO NOTHING;

INSERT INTO agg_machine_day
    (machine_id, window_start, window_end, reading_count, avg_volt, avg_rotate, avg_pressure, avg_vibration,
     max_volt, min_rotate, max_pressure, max_vibration)
SELECT machine_id, date_trunc('day', event_time, 'UTC'),
       date_trunc('day', event_time, 'UTC') + interval '1 day', count(*),
       avg(volt), avg(rotate), avg(pressure), avg(vibration), max(volt), min(rotate), max(pressure), max(vibration)
FROM live_readings
GROUP BY 1, 2
ON CONFLICT (machine_id, window_start) DO NOTHING;

-- Per-reading alerts, same rules as docs/contract.md section 4.
INSERT INTO alerts (alert_id, machine_id, rule, severity, scope, metric, value, threshold, event_time, created_at)
SELECT md5(r.machine_id || '|' || x.rule || '|' || to_char(r.event_time AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')),
       r.machine_id, x.rule, 'warning', 'reading', x.metric, x.value, x.threshold, r.event_time, r.ingest_time
FROM live_readings r
CROSS JOIN LATERAL (VALUES
    ('HIGH_VOLT', 'volt', r.volt, 240.0, r.volt > 240),
    ('LOW_ROTATION', 'rotate', r.rotate, 210.0, r.rotate < 210),
    ('HIGH_PRESSURE', 'pressure', r.pressure, 150.0, r.pressure > 150),
    ('HIGH_VIBRATION', 'vibration', r.vibration, 65.0, r.vibration > 65)
) AS x (rule, metric, value, threshold, broken)
WHERE x.broken
ON CONFLICT (alert_id) DO NOTHING;

-- 24-hour alerts.
INSERT INTO alerts (alert_id, machine_id, rule, severity, scope, metric, value, threshold, event_time, created_at)
SELECT md5(a.machine_id || '|' || x.rule || '|' || to_char(a.window_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')),
       a.machine_id, x.rule, 'critical', 'day', x.metric, x.value, x.threshold, a.window_start,
       (SELECT max(ingest_time) FROM live_readings r
         WHERE r.machine_id = a.machine_id AND r.event_time < a.window_end)
FROM agg_machine_day a
CROSS JOIN LATERAL (VALUES
    ('VOLT_HIGH_24H', 'avg_volt', a.avg_volt, 185.0, a.avg_volt > 185),
    ('ROTATION_LOW_24H', 'avg_rotate', a.avg_rotate, 410.0, a.avg_rotate < 410),
    ('PRESSURE_HIGH_24H', 'avg_pressure', a.avg_pressure, 112.0, a.avg_pressure > 112),
    ('VIBRATION_HIGH_24H', 'avg_vibration', a.avg_vibration, 46.0, a.avg_vibration > 46)
) AS x (rule, metric, value, threshold, broken)
WHERE x.broken AND a.reading_count >= 20
ON CONFLICT (alert_id) DO NOTHING;

INSERT INTO rejected_events (source_partition, source_offset, reason, raw_payload, rejected_at)
VALUES (0, -1, 'not valid JSON', 'not json at all', now() - interval '9 minutes'),
       (1, -2, 'missing required field: event_id', '{"machine_id": 3}', now() - interval '6 minutes'),
       (2, -3, 'wrong type for field: volt', '{"machine_id": 3, "volt": "high"}', now() - interval '4 minutes'),
       (0, -4, 'not valid JSON', '{"event_id": "5b1d6c0e", "event_ty', now() - interval '1 minute')
ON CONFLICT DO NOTHING;
