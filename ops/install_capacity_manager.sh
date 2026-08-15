#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ENV_FILE=/etc/busan-capacity-manager.env

install -D -o root -g root -m 0755 \
  "$ROOT_DIR/ops/busan_disk_capacity_manager.py" \
  /usr/local/sbin/busan-disk-capacity-manager.py
install -D -o root -g root -m 0644 \
  "$ROOT_DIR/ops/systemd/busan-disk-capacity-manager.service" \
  /etc/systemd/system/busan-disk-capacity-manager.service
install -D -o root -g root -m 0644 \
  "$ROOT_DIR/ops/systemd/busan-disk-capacity-manager.timer" \
  /etc/systemd/system/busan-disk-capacity-manager.timer

if [[ ! -s "$ENV_FILE" ]]; then
  umask 077
  grep -E '^export NCP_(ACCESS|SECRET)_KEY=' /opt/busan/.env \
    | cut -c8- > "$ENV_FILE"
fi

if [[ $(grep -c '^NCP_.*KEY=' "$ENV_FILE") -ne 2 ]]; then
  echo "NCP Object Storage keys are not configured in $ENV_FILE" >&2
  exit 1
fi

chown root:root "$ENV_FILE"
chmod 0600 "$ENV_FILE"
systemd-analyze verify \
  /etc/systemd/system/busan-disk-capacity-manager.service \
  /etc/systemd/system/busan-disk-capacity-manager.timer
systemctl daemon-reload
systemctl enable --now busan-disk-capacity-manager.timer
systemctl start busan-disk-capacity-manager.service
systemctl show busan-disk-capacity-manager.service \
  -p Result -p ExecMainStatus -p ActiveState

