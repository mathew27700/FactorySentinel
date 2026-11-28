-- Data-quality checks. Each row is one check; failed_rows = 0 means it passed.
CREATE OR REPLACE VIEW v_quality_checks AS
SELECT 'duplicate_event_id' AS check_name,
       (SELECT count(*) FROM (SELECT event_id FROM live_readings GROUP BY 1 HAVING count(*) > 1) d) AS failed_rows,
       'the same event stored under two keys' AS detail
UNION ALL
SELECT 'values_out_of_range',
       (SELECT count(*) FROM live_readings
         WHERE volt NOT BETWEEN 0 AND 500 OR rotate NOT BETWEEN 0 AND 1000
            OR pressure NOT BETWEEN 0 AND 300 OR vibration NOT BETWEEN 0 AND 150),
       'validation should have rejected these'
UNION ALL
SELECT 'unknown_machine',
       (SELECT count(*) FROM (SELECT DISTINCT machine_id FROM live_readings) r
         WHERE NOT EXISTS (SELECT 1 FROM dim_machine m WHERE m.machine_id = r.machine_id)),
       'machine in the stream that is not in dim_machine'
UNION ALL
SELECT 'incomplete_day',
       (SELECT count(*) FROM agg_machine_day WHERE reading_count <> 24),
       'a finished day should hold 24 hourly readings'
UNION ALL
SELECT 'window_does_not_match_readings',
       (SELECT count(*) FROM agg_machine_day a
         WHERE a.reading_count <> (SELECT count(*) FROM live_readings r
                                    WHERE r.machine_id = a.machine_id
                                      AND r.event_time >= a.window_start AND r.event_time < a.window_end)),
       'readings arrived too late for their window'
UNION ALL
SELECT 'fact_without_dimension',
       (SELECT count(*) FROM fact_machine_day f
         WHERE NOT EXISTS (SELECT 1 FROM dim_machine m WHERE m.machine_key = f.machine_key)
            OR NOT EXISTS (SELECT 1 FROM dim_date d WHERE d.date_key = f.date_key)),
       'fact row pointing at a missing dimension row'
UNION ALL
SELECT 'stream_is_stale',
       (SELECT CASE WHEN max(ingest_time) > now() - interval '2 minutes' THEN 0 ELSE 1 END FROM live_readings),
       'no new reading in the last 2 minutes (expected only while the simulator runs)';
