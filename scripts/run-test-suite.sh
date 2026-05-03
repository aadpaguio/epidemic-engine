#!/usr/bin/env bash
# Single entrypoint for the automated test suite (pytest only — no operational smoke scripts).
#
# Usage (from repo root):
#   ./scripts/run-test-suite.sh
#
# Paste-friendly: prints section banners; pytest uses -v -ra (verbose + skip reasons).
#
# Layers:
#   - tests/unit, tests/spark: fast logic tests.
#   - tests/cluster: offline Phase 1 manifest contracts (Tekton + Spark raw_db job).
#   - tests/db: set EPIDEMIC_TEST_PG_URL for live Postgres / Timescale checks.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

case "${1:-}" in
  -h|--help)
    cat <<'EOF'
Usage: ./scripts/run-test-suite.sh

Runs: uv run pytest tests -v --tb=short -ra

Environment:
  EPIDEMIC_TEST_PG_URL   Optional; enables tests/db integration tests.
EOF
    exit 0
    ;;
esac

echo "================================================================================"
echo " epidemic-engine test suite — $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo " repo: ${ROOT}"
echo "================================================================================"
echo

echo "---------- pytest (tests/) ----------"
echo "Hint: DB tests need EPIDEMIC_TEST_PG_URL or a repo-root .env with DB_HOST=localhost"
echo "      (after: oc port-forward svc/timescaledb-service 5432:5432) — see docs/local-db-connection.md"
echo

py_rc=0
uv run pytest tests -v --tb=short -ra || py_rc=$?

echo
echo "================================================================================"
echo " suite finished — exit code: ${py_rc}"
echo "================================================================================"

exit "$py_rc"
