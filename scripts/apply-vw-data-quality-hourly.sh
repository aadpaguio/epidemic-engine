#!/usr/bin/env bash
# Apply only vw_data_quality_hourly (no full database-init replay, no StatefulSet restart).
# Uses credentials from secret epidemic-db-spark (same as TimescaleDB StatefulSet).
#
# Usage:
#   ./scripts/apply-vw-data-quality-hourly.sh
#   NAMESPACE=ds551-2026-spring-a5f0d1 DB_POD=timescaledb-0 ./scripts/apply-vw-data-quality-hourly.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SQL_FILE="${ROOT}/infra/sql/vw_data_quality_hourly.sql"

NAMESPACE="${NAMESPACE:-ds551-2026-spring-a5f0d1}"
DB_POD="${DB_POD:-timescaledb-0}"
SECRET="${SECRET:-epidemic-db-spark}"
DB_NAME="${DB_NAME:-epidemic}"

if [[ ! -f "$SQL_FILE" ]]; then
  echo "missing $SQL_FILE" >&2
  exit 1
fi

USER_B64="$(oc get secret "$SECRET" -n "$NAMESPACE" -o jsonpath='{.data.username}')"
PASS_B64="$(oc get secret "$SECRET" -n "$NAMESPACE" -o jsonpath='{.data.password}')"
DB_USER="$(printf '%s' "$USER_B64" | base64 -d)"
export PGPASSWORD="$(printf '%s' "$PASS_B64" | base64 -d)"

echo "Applying vw_data_quality_hourly to ${DB_NAME} via pod ${DB_POD} (namespace ${NAMESPACE}) …"
oc exec -i "$DB_POD" -n "$NAMESPACE" -- \
  env PGPASSWORD="$PGPASSWORD" psql -h 127.0.0.1 -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 <"$SQL_FILE"

echo "Done. Verify with: SELECT COUNT(*) FROM vw_data_quality_hourly;"
