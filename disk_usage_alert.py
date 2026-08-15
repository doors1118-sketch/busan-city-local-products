#!/usr/bin/env python3
"""Standalone disk usage SMS alert.

Runs from cron. Sends at most one SMS per day while the configured filesystem is
at or above DISK_USAGE_ALERT_THRESHOLD.
"""
import argparse
import datetime
import json
import os
from pathlib import Path

from alert_check import check_disk_usage, load_config, send_ncp_sms


STATE_FILE = Path(os.getenv("DISK_USAGE_ALERT_STATE", "alert_log/disk_usage_alert_state.json"))


def _load_state() -> dict:
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="print result without sending SMS")
    args = parser.parse_args()

    alerts = check_disk_usage()
    now = datetime.datetime.now()
    today = now.strftime("%Y-%m-%d")
    state = _load_state()

    if not alerts:
        state["last_status"] = "ok"
        state["last_checked_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
        _save_state(state)
        print("disk_usage_alert: ok")
        return 0

    message = "\n".join([
        f"[부산 조달 운영알림] {now.strftime('%Y-%m-%d %H:%M')}",
        "서버 디스크 사용률 경고",
        "",
        *[msg.replace("⚠️ ", "") for _, msg in alerts],
        "",
        "조치: /opt/busan/backups, /opt/advisor/cache/company/archive, 로그 보존량 확인",
    ])

    if args.dry_run:
        print("disk_usage_alert: dry-run")
        print(message)
        return 0

    if state.get("last_sent_date") == today:
        print(f"disk_usage_alert: already sent today ({today})")
        state["last_status"] = "warning"
        state["last_checked_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
        _save_state(state)
        return 0

    config = load_config()
    send_ncp_sms(message, config)
    state["last_status"] = "warning"
    state["last_checked_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
    state["last_sent_date"] = today
    _save_state(state)
    print(f"disk_usage_alert: sent ({today})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
