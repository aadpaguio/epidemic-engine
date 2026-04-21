#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "[gate] scripts/run-hourly-spark-gated.sh is deprecated."
echo "[gate] Switching to scripts/run-hourly-spark-external.sh (no nested oc exec)."
exec bash scripts/run-hourly-spark-external.sh "$@"
