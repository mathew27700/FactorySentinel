#!/usr/bin/env bash
# Run the data-quality checks, store the result, and fail (exit 1) if any check failed.
set -euo pipefail
cd "$(dirname "$0")/.."
scripts/psql.sh -q -c "INSERT INTO dq_results (check_name, failed_rows, passed, detail)
                       SELECT check_name, failed_rows, failed_rows = 0, detail FROM v_quality_checks"
scripts/psql.sh -c "SELECT CASE WHEN failed_rows = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
                           check_name, failed_rows, detail FROM v_quality_checks ORDER BY 1, 2"
failed=$(scripts/psql.sh -At -c "SELECT count(*) FROM v_quality_checks WHERE failed_rows > 0")
if [ "$failed" -gt 0 ]; then echo "$failed check(s) failed"; exit 1; fi
echo "all checks passed"
