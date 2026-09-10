#!/usr/bin/env bash
set -euo pipefail

cd /opt/busan
set -a
# shellcheck disable=SC1091
source /opt/busan/.env
set +a

SCRATCH=/opt/busan/scratch/shopping_performance_backfill
PYTHON=/opt/busan/venv/bin/python3

exec 9>"$SCRATCH/run.lock"
flock -n 9 || {
  echo "another shopping shadow run is active" >&2
  exit 3
}

nice -n 10 ionice -c 3 "$PYTHON" "$SCRATCH/shopping_performance_sync.py" \
  --production-db /opt/busan/procurement_contracts.db \
  --agency-db /opt/busan/busan_agencies_master.db \
  --shadow-db "$SCRATCH/shopping_backfill.db" \
  --start 20260101 \
  --end 20260909 \
  --ascending \
  --max-requests 700 \
  --reserve-requests 150 \
  --min-free-gb 8 \
  --max-shadow-mb 512

nice -n 10 ionice -c 3 env PYTHONPATH=/opt/busan \
  "$PYTHON" "$SCRATCH/shopping_performance_impact.py" \
  --production-db /opt/busan/procurement_contracts.db \
  --agency-db /opt/busan/busan_agencies_master.db \
  --company-db /opt/busan/busan_companies_master.db \
  --shadow-db "$SCRATCH/shopping_backfill.db" \
  --output-dir "$SCRATCH/impact" \
  --start 20260101 \
  --end 20260909
