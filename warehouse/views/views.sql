-- What "normal" looks like for each machine, learned from the history half of the warehouse.
CREATE OR REPLACE VIEW v_machine_baseline AS
SELECT m.machine_id, m.model, m.age_years, count(*) AS days,
       avg(f.avg_volt) AS volt_mean,           stddev_samp(f.avg_volt) AS volt_sd,
       avg(f.avg_rotate) AS rotate_mean,       stddev_samp(f.avg_rotate) AS rotate_sd,
       avg(f.avg_pressure) AS pressure_mean,   stddev_samp(f.avg_pressure) AS pressure_sd,
       avg(f.avg_vibration) AS vibration_mean, stddev_samp(f.avg_vibration) AS vibration_sd
FROM fact_machine_day f
JOIN dim_machine m USING (machine_key)
WHERE f.load_source = 'history' AND f.reading_count = 24
GROUP BY m.machine_id, m.model, m.age_years;

-- Live versus history: the newest finished day of every machine, as a distance from its own normal.
CREATE OR REPLACE VIEW v_live_vs_baseline AS
SELECT z.*,
       CASE WHEN z.worst_z > 3 THEN 'abnormal' WHEN z.worst_z > 2 THEN 'watch' ELSE 'normal' END AS status
FROM (
    SELECT a.machine_id, b.model, b.age_years, a.window_start AS day, a.reading_count,
           round(a.avg_volt::numeric, 1) AS avg_volt,
           round(((a.avg_volt - b.volt_mean) / nullif(b.volt_sd, 0))::numeric, 2) AS volt_z,
           round(a.avg_rotate::numeric, 1) AS avg_rotate,
           round(((a.avg_rotate - b.rotate_mean) / nullif(b.rotate_sd, 0))::numeric, 2) AS rotate_z,
           round(a.avg_pressure::numeric, 1) AS avg_pressure,
           round(((a.avg_pressure - b.pressure_mean) / nullif(b.pressure_sd, 0))::numeric, 2) AS pressure_z,
           round(a.avg_vibration::numeric, 1) AS avg_vibration,
           round(((a.avg_vibration - b.vibration_mean) / nullif(b.vibration_sd, 0))::numeric, 2) AS vibration_z,
           round(greatest(abs((a.avg_volt - b.volt_mean) / nullif(b.volt_sd, 0)),
                          abs((a.avg_rotate - b.rotate_mean) / nullif(b.rotate_sd, 0)),
                          abs((a.avg_pressure - b.pressure_mean) / nullif(b.pressure_sd, 0)),
                          abs((a.avg_vibration - b.vibration_mean) / nullif(b.vibration_sd, 0)))::numeric, 2)
               AS worst_z
    FROM agg_machine_day a
    JOIN v_machine_baseline b USING (machine_id)
    WHERE a.window_start = (SELECT max(window_start) FROM agg_machine_day)
) z;

-- Did our alerts see the recorded failures coming? One row per failure inside the streamed period.
CREATE OR REPLACE VIEW v_failure_detection AS
SELECT m.machine_id, f.failed_at, f.component,
       EXISTS (
           SELECT 1 FROM alerts a
           WHERE a.machine_id = m.machine_id AND a.scope = 'day'
             AND a.event_time >= date_trunc('day', f.failed_at, 'UTC') - interval '2 days'
             AND a.event_time <  date_trunc('day', f.failed_at, 'UTC')
       ) AS warned_before
FROM fact_failure f
JOIN dim_machine m USING (machine_key)
WHERE f.failed_at >= (SELECT min(window_start) + interval '2 days' FROM agg_machine_day)
  AND f.failed_at <  (SELECT max(window_end) FROM agg_machine_day);

-- The same question from the other side: how many day alerts were followed by a failure?
CREATE OR REPLACE VIEW v_alert_scorecard AS
SELECT (SELECT count(*) FROM v_failure_detection) AS failures,
       (SELECT count(*) FROM v_failure_detection WHERE warned_before) AS failures_warned,
       (SELECT round(100.0 * count(*) FILTER (WHERE warned_before) / nullif(count(*), 0), 1)
          FROM v_failure_detection) AS pct_failures_warned,
       d.alert_days,
       d.alert_days_followed,
       round(100.0 * d.alert_days_followed / nullif(d.alert_days, 0), 1) AS pct_alert_days_followed
FROM (
    SELECT count(*) AS alert_days,
           count(*) FILTER (WHERE EXISTS (
               SELECT 1 FROM fact_failure f JOIN dim_machine m USING (machine_key)
               WHERE m.machine_id = x.machine_id
                 AND f.failed_at >= x.day AND f.failed_at < x.day + interval '3 days')) AS alert_days_followed
    FROM (SELECT DISTINCT machine_id, event_time AS day FROM alerts WHERE scope = 'day') x
) d;
