#!/usr/bin/env bash
set -euo pipefail
cd /opt/busan
set -a
source /opt/busan/.env
set +a
exec 9>/opt/busan/.procurement_pipeline.lock
flock -w 3600 9 || exit 75
exec /opt/busan/venv/bin/python3 refresh_procurement_caches.py
