#!/usr/bin/env bash
# Create every table and view. Safe to re-run (CREATE ... IF NOT EXISTS, CREATE OR REPLACE VIEW).
set -euo pipefail
cd "$(dirname "$0")/.."
shopt -s nullglob   # a folder with no .sql files yet is fine
for f in warehouse/ddl/*.sql warehouse/views/*.sql warehouse/quality/*.sql; do
  echo "== $f"
  scripts/psql.sh -q -f - < "$f"
done
scripts/psql.sh -c '\dt' -c '\dv'
