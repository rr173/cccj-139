#!/usr/bin/env bash
# Run the full test-suite against the temporary local PostgreSQL instance.
set -euo pipefail
cd "$(dirname "$0")/.."
scripts/pg_start.sh
exec venv/bin/python -m pytest "$@"
