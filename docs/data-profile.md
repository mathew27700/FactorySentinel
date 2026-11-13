# Data profile: Azure Predictive Maintenance

**Source:** Kaggle `arnabbiswas1/microsoft-azure-predictive-maintenance` (downloaded 2026-11-07).
**Method:** pandas on the five CSV files in `data/raw/` (not committed to git).

## Files

| File | Rows | Columns |
|---|---|---|
| `PdM_telemetry.csv` | 876,100 | `datetime`, `machineID`, `volt`, `rotate`, `pressure`, `vibration` |
| `PdM_errors.csv` | 3,919 | `datetime`, `machineID`, `errorID` |
| `PdM_failures.csv` | 761 | `datetime`, `machineID`, `failure` |
| `PdM_maint.csv` | 3,286 | `datetime`, `machineID`, `comp` |
| `PdM_machines.csv` | 100 | `machineID`, `model`, `age` |

## Telemetry

- **Time range:** 2015-01-01 06:00:00 to 2016-01-01 06:00:00.
- **Machines:** 100 (`machineID` 1 to 100).
- **Readings per machine:** 8,761 for every machine (100 x 8,761 = 876,100).
- **Frequency:** hourly. All 876,000 gaps between consecutive readings of the same machine are exactly 1 hour.
- **Duplicates (same machine and time):** 0.
- **Missing values:** 0 in every column.
- **Timestamp format:** `YYYY-MM-DD HH:MM:SS` with no timezone in the file. The timezone to use for `event_time` must be agreed (see event schema).

| Sensor | Min | Mean | Max | Std |
|---|---|---|---|---|
| `volt` | 97.33 | 170.78 | 255.12 | 15.51 |
| `rotate` | 138.43 | 446.61 | 695.02 | 52.67 |
| `pressure` | 51.24 | 100.86 | 185.95 | 11.05 |
| `vibration` | 14.88 | 40.39 | 76.79 | 5.37 |

The maximum values are several standard deviations above the mean. We have not checked whether they are errors or happen before failures.

## Errors

3,919 rows. Counts by `errorID`: error1 1,010; error2 988; error3 838; error4 727; error5 356.

## Failures

761 rows. Counts by `failure`: comp2 259; comp1 192; comp4 179; comp3 131.

## Maintenance

3,286 rows with columns `datetime`, `machineID`, `comp`. The first rows are dated 2014, so the maintenance history starts before the telemetry period (seen in the first three rows only, full range not checked).

## Machines

100 rows with `machineID`, `model` and `age`. Not profiled further.

## Missing and odd values

- No missing values in any of the five files.
- No duplicate (machine, time) rows in telemetry.
- Not checked yet: duplicates in errors, failures and maintenance; whether failure timestamps match telemetry timestamps; distribution of values beyond the summary above.

## What this means for ingestion

- One full replay of the telemetry is 876,100 events (8,761 hourly timestamps x 100 machines).
- Reading events carry `volt`, `rotate`, `pressure`, `vibration`. Error and failure events (week 2) carry `errorID` or `failure` instead of sensor values.
