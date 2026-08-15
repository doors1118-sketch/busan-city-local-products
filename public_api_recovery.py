"""Persistent queue helpers for safe public-API recovery jobs."""

from __future__ import annotations

import datetime as dt
import sqlite3
from collections.abc import Iterable, Mapping


PRESPEC_RECOVERY_TYPE = "prespec"


def ensure_recovery_queue(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS api_recovery_queue (
            recovery_type TEXT NOT NULL,
            target_date TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempt_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            last_attempt_at TEXT,
            recovered_at TEXT,
            last_error TEXT,
            PRIMARY KEY (recovery_type, target_date)
        )
        """
    )


def _now_text(now: dt.datetime | None = None) -> str:
    return (now or dt.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")


def enqueue_prespec_date(
    conn: sqlite3.Connection,
    target_date: str,
    *,
    now: dt.datetime | None = None,
) -> bool:
    target_date = str(target_date or "").strip()
    if len(target_date) != 8 or not target_date.isdigit():
        return False
    ensure_recovery_queue(conn)
    before = conn.total_changes
    conn.execute(
        """
        INSERT OR IGNORE INTO api_recovery_queue (
            recovery_type, target_date, status, attempt_count, created_at
        ) VALUES (?, ?, 'pending', 0, ?)
        """,
        (PRESPEC_RECOVERY_TYPE, target_date, _now_text(now)),
    )
    return conn.total_changes > before


def enqueue_prespec_issues(
    conn: sqlite3.Connection,
    issues: Iterable[Mapping[str, object]],
    *,
    now: dt.datetime | None = None,
) -> int:
    queued = 0
    for issue in issues:
        if issue.get("issue_type") not in {"retry_exhausted", "result_code"}:
            continue
        if not str(issue.get("api_name") or "").startswith("prespec_"):
            continue
        if enqueue_prespec_date(conn, str(issue.get("target_date") or ""), now=now):
            queued += 1
    return queued


def seed_prespec_issue_history(conn: sqlite3.Connection) -> int:
    ensure_recovery_queue(conn)
    issue_table_exists = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = 'api_call_issues'
        LIMIT 1
        """
    ).fetchone()
    if not issue_table_exists:
        return 0
    rows = conn.execute(
        """
        SELECT DISTINCT target_date
        FROM api_call_issues
        WHERE api_name LIKE 'prespec_%'
          AND issue_type IN ('retry_exhausted', 'result_code')
        ORDER BY target_date
        """
    ).fetchall()
    queued = 0
    for (target_date,) in rows:
        if enqueue_prespec_date(conn, target_date):
            queued += 1
    return queued


def pending_prespec_dates(conn: sqlite3.Connection, limit: int = 3) -> list[str]:
    ensure_recovery_queue(conn)
    rows = conn.execute(
        """
        SELECT target_date
        FROM api_recovery_queue
        WHERE recovery_type = ?
          AND status != 'success'
        ORDER BY target_date
        LIMIT ?
        """,
        (PRESPEC_RECOVERY_TYPE, max(1, int(limit))),
    ).fetchall()
    return [str(row[0]) for row in rows]


def record_prespec_attempt(
    conn: sqlite3.Connection,
    target_date: str,
    *,
    success: bool,
    error: str = "",
    now: dt.datetime | None = None,
) -> None:
    ensure_recovery_queue(conn)
    timestamp = _now_text(now)
    conn.execute(
        """
        UPDATE api_recovery_queue
        SET status = ?,
            attempt_count = attempt_count + 1,
            last_attempt_at = ?,
            recovered_at = ?,
            last_error = ?
        WHERE recovery_type = ? AND target_date = ?
        """,
        (
            "success" if success else "failed",
            timestamp,
            timestamp if success else None,
            None if success else str(error or "recovery_failed")[:500],
            PRESPEC_RECOVERY_TYPE,
            target_date,
        ),
    )
