#!/usr/bin/env bash
# Batch path: load the five dataset CSV files into staging, unchanged. Safe to re-run.
# (Step B3 adds the dimensions, the history half of the warehouse and the failure facts.)
set -euo pipefail
cd "$(dirname "$0")/.."
copy() {  # copy <table> <columns> <file>
  scripts/psql.sh -q -c "TRUNCATE $1"
  scripts/psql.sh -q -c "\copy $1 ($2) FROM pstdin WITH (FORMAT csv, HEADER true)" < "data/raw/$3"
  echo "$1: $(scripts/psql.sh -At -c "SELECT count(*) FROM $1") rows"
}
copy stg_machines  "machine_id, model, age"                                  PdM_machines.csv
copy stg_failures  "datetime, machine_id, failure"                           PdM_failures.csv
copy stg_errors    "datetime, machine_id, error_id"                          PdM_errors.csv
copy stg_maint     "datetime, machine_id, comp"                              PdM_maint.csv
copy stg_telemetry "datetime, machine_id, volt, rotate, pressure, vibration" PdM_telemetry.csv
