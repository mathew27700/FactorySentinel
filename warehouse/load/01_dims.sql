-- Dimensions. Safe to run again.
INSERT INTO dim_machine (machine_id, model, age_years)
SELECT machine_id, model, age FROM stg_machines
ON CONFLICT (machine_id) DO UPDATE SET model = EXCLUDED.model, age_years = EXCLUDED.age_years;

INSERT INTO dim_date (date_key, full_date, year, month, day_name, is_weekend)
SELECT to_char(d, 'YYYYMMDD')::integer, d::date, extract(year FROM d)::integer,
       extract(month FROM d)::integer, trim(to_char(d, 'Day')), extract(isodow FROM d) IN (5, 6)
FROM generate_series(date '2014-01-01', date '2016-12-31', interval '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;
