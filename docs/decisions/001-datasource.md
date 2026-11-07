# Decision 001: Data source

**Date:** 2026-11-07
**Status:** Approved
**Decision:** We use the Microsoft Azure Predictive Maintenance dataset from Kaggle
(`arnabbiswas1/microsoft-azure-predictive-maintenance`).

## What we verified
- The dataset downloads with the Kaggle CLI (about 31 MB zipped).
- It contains five files: `PdM_telemetry.csv`, `PdM_errors.csv`, `PdM_failures.csv`,
  `PdM_maint.csv`, `PdM_machines.csv`.
- The Kaggle CLI reports the license as "unknown".

## Why (from the project plan)
The member guide proposes this dataset and describes it as 100 machines with hourly readings
over 2015, plus recorded failures and errors. We chose it because that matches our design:
many machines (events keyed by `machine_id`, 3 partitions) and recorded failures we can use
to check that the system detects real events. Exact row counts and time range are in
`docs/data-profile.md`.

## Alternative considered
MetroPT-3 (UCI) was the other candidate listed in the member guide. We did not download or
profile it.

## Open point
Whether the data comes from a real factory or is simulated has not been verified yet. Check the
dataset page on Kaggle before describing it in the demo.
