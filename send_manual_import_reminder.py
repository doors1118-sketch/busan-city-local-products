#!/usr/bin/env python3
"""Send monthly SMS reminder for manual procurement reference imports."""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

from alert_check import _sanitize_sens_text, send_ncp_sms


DEFAULT_CONFIG = Path("alert_config.json")


def build_message(today: dt.date | None = None) -> str:
    today = today or dt.datetime.now().date()
    ymd = today.strftime("%Y%m%d")
    title = "\ubd80\uc0b0 \uc870\ub2ec \uc6b4\uc601\uc54c\ub9bc"
    check_target = "\uc218\ub3d9 \uc784\ud3ec\ud2b8 \ub300\uc0c1 \ud655\uc778"
    items = [
        "\uc911\uc18c\uae30\uc5c5\uc790\uac04 \uacbd\uc7c1\uc81c\ud488 \ubc0f \uacf5\uc0ac\uc6a9\uc790\uc7ac \ub0b4\uc5ed",
        "\uacf5\uc0ac\uc6a9\uc790\uc7ac \ud488\ubaa9",
        "\uc138\ubd80\ud488\ubaa9\ubcc4 \ud544\uc218\ud2b9\uc774\uc0ac\ud56d \ubaa9\ub85d",
        "\uc801\uaca9\uc870\ud569 \ud604\ud669",
        "\uc870\ud569\uacf5\ub3d9\uc0ac\uc5c5\uc81c\ud488",
    ]
    upload = "\uc5c5\ub85c\ub4dc"
    run = "\uc2e4\ud589"
    verify = "\ud655\uc778"
    verify_text = "product_policy_summary \uac74\uc218/\ubd80\uc0b0 \uc870\ud569/\uc9c1\uc811\uc0dd\uc0b0 \ub9e4\uce6d"
    return "\n".join(
        [
            f"[{title}] {today:%Y-%m-%d}",
            check_target,
            "",
            *[f"{idx}. {text}" for idx, text in enumerate(items, 1)],
            "",
            f"{upload}: /opt/busan/import_sources/smpp_product_policy_{ymd}/",
            f"{run}: import_smpp_product_policy_files.py",
            f"{verify}: {verify_text}",
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Monthly manual import SMS reminder.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="alert_config.json path")
    parser.add_argument("--dry-run", action="store_true", help="Print sanitized message without sending SMS")
    args = parser.parse_args()

    config_path = Path(args.config)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    message = build_message()
    sanitized, changed, truncated = _sanitize_sens_text(message, 2000)
    byte_len = len(sanitized.encode("euc-kr", errors="ignore"))

    if args.dry_run:
        print(f"dry_run=True bytes={byte_len} changed={changed} truncated={truncated}")
        print(sanitized)
        return 0

    print(f"monthly_manual_import_reminder bytes={byte_len}")
    send_ncp_sms(message, config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
