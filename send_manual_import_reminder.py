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
    check_target = "SMPP 품목정책 수동 임포트 대상 확인"
    required_items = [
        "공사용자재 품목.xls",
        "세부품목별_필수특이사항_목록.xls",
        "적격조합 현황.xls",
        "조합공동사업제품.xls",
    ]
    reference_items = [
        "중소기업자간 경쟁제품 일반 목록",
    ]
    upload = "업로드"
    run = "실행"
    verify = "확인"
    verify_text = "product_policy_summary 건수/부산 조합/직접생산 매칭"
    return "\n".join(
        [
            f"[{title}] {today:%Y-%m-%d}",
            check_target,
            "",
            "필수 업로드:",
            *[f"{idx}. {text}" for idx, text in enumerate(required_items, 1)],
            "",
            "참고 확인:",
            *[f"- {text}" for text in reference_items],
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
