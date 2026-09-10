import datetime
import sqlite3

import alert_check
import daily_pipeline_sync


def _create_shopping_log(path):
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE shopping_sync_log (
            target_date TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            source_total INTEGER NOT NULL,
            source_received INTEGER NOT NULL,
            busan_rows INTEGER NOT NULL,
            stored_rows INTEGER NOT NULL,
            request_count INTEGER NOT NULL,
            rate_limit_remaining INTEGER,
            error TEXT,
            completed_at TEXT
        )
    """)
    conn.commit()
    return conn


def test_daily_contract_pipeline_excludes_shopping_performance():
    assert '쇼핑몰' in daily_pipeline_sync.APIS
    assert '쇼핑몰' not in daily_pipeline_sync.DAILY_CONTRACT_CATEGORIES
    assert len(daily_pipeline_sync.DAILY_CONTRACT_CATEGORIES) == 6


def test_shopping_alert_accepts_exact_completed_day(monkeypatch, tmp_path):
    db_path = tmp_path / "procurement.db"
    conn = _create_shopping_log(db_path)
    conn.execute(
        """INSERT INTO shopping_sync_log VALUES
        (?, 'complete', 10352, 10352, 510, 510, 11, 866, NULL, ?)""",
        ("20260908", "2026-09-10 02:30:00"),
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(alert_check, "DB_PATH", str(db_path))

    alerts = alert_check.check_shopping_pipeline_sync(
        now=datetime.datetime(2026, 9, 10, 9, 0), lag_days=2
    )

    assert alerts == []


def test_shopping_alert_rejects_partial_api_result(monkeypatch, tmp_path):
    db_path = tmp_path / "procurement.db"
    conn = _create_shopping_log(db_path)
    conn.execute(
        """INSERT INTO shopping_sync_log VALUES
        (?, 'complete', 10352, 999, 50, 50, 1, 866, NULL, ?)""",
        ("20260908", "2026-09-10 02:30:00"),
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(alert_check, "DB_PATH", str(db_path))

    alerts = alert_check.check_shopping_pipeline_sync(
        now=datetime.datetime(2026, 9, 10, 9, 0), lag_days=2
    )

    assert alerts and alerts[0][0] == "CRITICAL"
    assert "999/10,352" in alerts[0][1]


def test_shopping_alert_rejects_missing_day(monkeypatch, tmp_path):
    db_path = tmp_path / "procurement.db"
    conn = _create_shopping_log(db_path)
    conn.close()
    monkeypatch.setattr(alert_check, "DB_PATH", str(db_path))

    alerts = alert_check.check_shopping_pipeline_sync(
        now=datetime.datetime(2026, 9, 10, 9, 0), lag_days=2
    )

    assert alerts and alerts[0][0] == "CRITICAL"
    assert "20260908" in alerts[0][1]


def test_shopping_alert_treats_confirmed_zero_as_warning(monkeypatch, tmp_path):
    db_path = tmp_path / "procurement.db"
    conn = _create_shopping_log(db_path)
    conn.execute(
        """INSERT INTO shopping_sync_log VALUES
        (?, 'complete_zero', 0, 0, 0, 0, 2, 850, NULL, ?)""",
        ("20260908", "2026-09-10 02:30:00"),
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(alert_check, "DB_PATH", str(db_path))

    alerts = alert_check.check_shopping_pipeline_sync(
        now=datetime.datetime(2026, 9, 10, 9, 0), lag_days=2
    )

    assert alerts and alerts[0][0] == "WARNING"
    assert "API 2회 확인" in alerts[0][1]
