#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

python -m venv .venv
.venv/bin/pip install -q -r ml/requirements.txt
.venv/bin/python -m ml.local_predict_smoke "$@"
