#!/usr/bin/env bash
# Stop the temporary local PostgreSQL instance.
set -euo pipefail

PGBIN="${MTLOADER_PGBIN:-/workspace/pgroot/usr/lib/postgresql/15/bin}"
PGDATA="${MTLOADER_PGDATA:-/workspace/pgdata}"

if [ -f "$PGDATA/postmaster.pid" ]; then
    "$PGBIN/pg_ctl" -D "$PGDATA" -m fast stop
    echo "stopped"
else
    echo "not running"
fi
