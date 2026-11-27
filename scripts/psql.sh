#!/usr/bin/env bash
# Run psql inside the postgres container against the project database.
#   scripts/psql.sh                       interactive prompt
#   scripts/psql.sh -f - < file.sql       run a file
#   scripts/psql.sh -c "SELECT 1"         run one statement
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
# No arguments = interactive prompt (needs a terminal). Otherwise never allocate one, so the
# output is clean when another script captures it.
if [ $# -eq 0 ]; then TTY=""; else TTY="-T"; fi
docker compose exec $TTY postgres psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"
