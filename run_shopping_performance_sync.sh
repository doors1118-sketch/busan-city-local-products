#!/usr/bin/env bash
set -euo pipefail

cd /opt/busan
set -a
# shellcheck disable=SC1091
source /opt/busan/.env
set +a

exec /opt/busan/venv/bin/python3 /opt/busan/shopping_performance_sync.py \
  --live \
  --lag-days 2 \
  --lookback-days 7 \
  --max-requests 100 \
  --reserve-requests 150 \
  --min-free-gb 8 \
  --max-shadow-mb 512
