#!/usr/bin/env bash
# Start the temporary local PostgreSQL instance used by the test-suite.
# Binaries were extracted from Debian packages into /workspace/pgroot (no root
# needed); data lives in /workspace/pgdata; the server listens only on a unix
# socket under /workspace/pgsock (port 55432).
set -euo pipefail

PGBIN="${MTLOADER_PGBIN:-/workspace/pgroot/usr/lib/postgresql/15/bin}"
PGDATA="${MTLOADER_PGDATA:-/workspace/pgdata}"
PGSOCK="${MTLOADER_PGSOCK:-/workspace/pgsock}"
PGPORT="${MTLOADER_PGPORT:-55432}"
PGUSER="${MTLOADER_PGUSER:-postgres}"

if "$PGBIN/pg_isready" -h "$PGSOCK" -p "$PGPORT" >/dev/null 2>&1; then
    echo "already running (socket $PGSOCK, port $PGPORT)"
    exit 0
fi

mkdir -p "$PGSOCK"
if [ ! -f "$PGDATA/PG_VERSION" ]; then
    mkdir -p "$PGDATA"
    chmod 700 "$PGDATA"
    "$PGBIN/initdb" -D "$PGDATA" -U "$PGUSER" --auth=trust --no-locale -E UTF8
fi

"$PGBIN/pg_ctl" -D "$PGDATA" -l "$PGDATA/server.log" \
    -o "-k $PGSOCK -c listen_addresses='' -p $PGPORT" -w start
echo "temporary PostgreSQL up (socket $PGSOCK, port $PGPORT)"
