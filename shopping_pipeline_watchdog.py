"""Daily and on-failure shopping-only notification; no healthy-state SMS."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import sys

import alert_check


def main():
    alerts = alert_check.check_shopping_pipeline_sync()
    if '--failed' in sys.argv:
        alerts.append(('CRITICAL', '🚨 [경보] 종합쇼핑몰 자동수집 작업 실패·유보: 기존 실적 보존, 재시도 및 로그 확인 필요'))
    if not alerts:
        print('SHOPPING_WATCHDOG_OK')
        return 0
    if '--check-only' in sys.argv:
        print(json.dumps(alerts, ensure_ascii=False))
        return 2
    path = Path('sync_log/shopping_watchdog_notification.json')
    fingerprint = hashlib.sha256(json.dumps(alerts, ensure_ascii=False).encode()).hexdigest()
    day = datetime.date.today().isoformat()
    previous = json.loads(path.read_text()) if path.exists() else {}
    if previous == {'day': day, 'fingerprint': fingerprint}:
        return 0
    config = alert_check.load_config()
    # Existing approved recipients/configuration only. Do not print credentials.
    message = '[부산 조달 운영알림] 종합쇼핑몰 자동점검\n' + '\n'.join(msg for _, msg in alerts)
    result = alert_check.send_ncp_sms(message[:1800], config)
    if result is not True:
        raise RuntimeError('shopping notification API acceptance was not confirmed')
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps({'day': day, 'fingerprint': fingerprint}), encoding='utf-8')
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
