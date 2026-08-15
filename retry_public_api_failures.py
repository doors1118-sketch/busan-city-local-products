"""Retry idempotent public-API jobs recorded in the recovery queue."""

from __future__ import annotations

import argparse
import fcntl
import os
import sqlite3
import sys
from pathlib import Path

import daily_pipeline_sync as pipeline
from public_api_recovery import (
    pending_prespec_dates,
    record_prespec_attempt,
    seed_prespec_issue_history,
)


def _prespec_failures(target_date: str) -> list[dict[str, object]]:
    with pipeline.API_ISSUES_LOCK:
        return [
            dict(issue)
            for issue in pipeline.API_ISSUES
            if str(issue.get("target_date")) == target_date
            and str(issue.get("api_name") or "").startswith("prespec_")
            and issue.get("issue_type") in {"retry_exhausted", "result_code"}
        ]


def _clear_in_memory_issues(target_date: str) -> None:
    with pipeline.API_ISSUES_LOCK:
        pipeline.API_ISSUES[:] = [
            issue
            for issue in pipeline.API_ISSUES
            if str(issue.get("target_date")) != target_date
        ]


def recover(limit: int, dry_run: bool = False) -> int:
    conn = sqlite3.connect(pipeline.DB_PATH, timeout=30)
    try:
        seeded = seed_prespec_issue_history(conn)
        conn.commit()
        dates = pending_prespec_dates(conn, limit=limit)
        print(f"복구 큐 확인: 신규 {seeded}건 / 실행 대상 {len(dates)}건")
        if dry_run:
            for target_date in dates:
                print(f"[DRY-RUN] prespec {target_date}")
            return 0

        failed = 0
        for target_date in dates:
            print(f"[자동 복구] 사전규격 {target_date} 재수집 시작")
            _clear_in_memory_issues(target_date)
            try:
                pipeline.sync_prespec(target_date, conn_path=pipeline.DB_PATH)
                failures = _prespec_failures(target_date)
                if failures:
                    detail = "; ".join(
                        f"{item.get('api_name')}:{item.get('detail')}" for item in failures
                    )
                    record_prespec_attempt(
                        conn, target_date, success=False, error=detail
                    )
                    failed += 1
                    print(f"[자동 복구] 사전규격 {target_date} 실패")
                else:
                    record_prespec_attempt(conn, target_date, success=True)
                    print(f"[자동 복구] 사전규격 {target_date} 성공")
                conn.commit()
            except Exception as exc:
                record_prespec_attempt(
                    conn,
                    target_date,
                    success=False,
                    error=f"{type(exc).__name__}: {exc}",
                )
                conn.commit()
                failed += 1
                print(f"[자동 복구] 사전규격 {target_date} 예외: {type(exc).__name__}")
        return 1 if failed else 0
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="공공데이터 API 실패일 자동 복구")
    parser.add_argument("--limit", type=int, default=3, help="한 번에 복구할 최대 날짜 수")
    parser.add_argument("--dry-run", action="store_true", help="복구 대상을 조회만 함")
    args = parser.parse_args()

    lock_path = Path(os.environ.get("PUBLIC_API_RECOVERY_LOCK", "/tmp/busan-public-api-recovery.lock"))
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("다른 공공데이터 자동 복구 작업이 실행 중입니다.")
            return 0
        return recover(limit=max(1, args.limit), dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
