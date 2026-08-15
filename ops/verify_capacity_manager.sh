#!/usr/bin/env bash
set -u
echo '===SERVICES==='
systemctl is-active busan-api busan-dashboard busan-advisor-pilot credit-guarantee-dashboard minsaeng100 nginx 2>&1 || true
echo '===TIMERS==='
systemctl is-active busan-disk-capacity-manager.timer credit-guarantee-backup.timer minsaeng100-backup.timer busan-company-cache-sync.timer
echo '===CAPACITY_UNIT==='
systemctl show busan-disk-capacity-manager.service -p Result -p ExecMainStatus
echo '===PUBLIC==='
for url in \
  'https://busanproduct.co.kr/' \
  'https://busanproduct.co.kr/vendor-ui/' \
  'https://busanproduct.co.kr/minsaeng100/' \
  'https://busanproduct.co.kr/api/social-enterprise/dashboard'
do
  printf '%s ' "$url"
  curl -L -sS -o /dev/null -w '%{http_code}\n' --max-time 20 "$url"
done
echo '===PORTS==='
ss -lntp | grep -E ':(8000|8001|8501) ' || true
echo '===DISK==='
df -h /
echo '===STATE==='
cat /var/lib/busan-capacity-manager/state.json
