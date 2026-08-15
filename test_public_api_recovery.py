import datetime as dt
import sqlite3

from public_api_recovery import (
    enqueue_prespec_issues,
    pending_prespec_dates,
    record_prespec_attempt,
    seed_prespec_issue_history,
)


def make_connection():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE api_call_issues (
            target_date TEXT,
            api_name TEXT,
            issue_type TEXT
        )
        """
    )
    return conn


def test_history_seed_handles_new_database():
    conn = sqlite3.connect(":memory:")
    assert seed_prespec_issue_history(conn) == 0
    assert pending_prespec_dates(conn) == []


def test_queue_only_accepts_prespec_retry_exhaustion():
    conn = make_connection()
    now = dt.datetime(2026, 8, 15, 8, 0, 0)
    issues = [
        {"target_date": "20260814", "api_name": "prespec_공사", "issue_type": "retry_exhausted"},
        {"target_date": "20260814", "api_name": "prespec_용역", "issue_type": "exception"},
        {"target_date": "20260814", "api_name": "contract_x", "issue_type": "retry_exhausted"},
        {"target_date": "20260813", "api_name": "prespec_용역", "issue_type": "result_code"},
    ]
    assert enqueue_prespec_issues(conn, issues, now=now) == 2
    assert pending_prespec_dates(conn) == ["20260813", "20260814"]


def test_successful_recovery_is_not_reopened_by_history_seed():
    conn = make_connection()
    conn.executemany(
        "INSERT INTO api_call_issues VALUES (?, ?, ?)",
        [
            ("20260803", "prespec_공사", "retry_exhausted"),
            ("20260803", "prespec_용역", "retry_exhausted"),
            ("20260804", "prespec_용역", "retry_exhausted"),
        ],
    )
    assert seed_prespec_issue_history(conn) == 2
    assert pending_prespec_dates(conn) == ["20260803", "20260804"]

    record_prespec_attempt(
        conn,
        "20260803",
        success=True,
        now=dt.datetime(2026, 8, 15, 8, 10, 0),
    )
    assert seed_prespec_issue_history(conn) == 0
    assert pending_prespec_dates(conn) == ["20260804"]


def test_failed_recovery_remains_pending_for_next_run():
    conn = make_connection()
    conn.execute(
        "INSERT INTO api_call_issues VALUES ('20260804', 'prespec_용역', 'retry_exhausted')"
    )
    seed_prespec_issue_history(conn)
    record_prespec_attempt(conn, "20260804", success=False, error="timeout")
    assert pending_prespec_dates(conn) == ["20260804"]
