import sqlite3
from pathlib import Path

import shopping_performance_sync as sync


def test_page_count_uses_exact_ceiling():
    assert sync.page_count(0) == 1
    assert sync.page_count(1) == 1
    assert sync.page_count(999) == 1
    assert sync.page_count(1000) == 2
    assert sync.page_count(1998) == 2


def test_normalize_items_supports_both_public_api_shapes():
    assert sync.normalize_items([{"a": 1}]) == [{"a": 1}]
    assert sync.normalize_items({"item": [{"a": 1}]}) == [{"a": 1}]
    assert sync.normalize_items({"item": {"a": 1}}) == [{"a": 1}]
    assert sync.normalize_items(None) == []


def test_date_key_and_inclusive_dates():
    assert sync.date_key("2026-09-09") == "20260909"
    assert sync.date_key("202609091230") == "20260909"
    assert sync.date_key("bad") is None
    assert sync.inclusive_dates("20260908", "20260910") == [
        "20260908",
        "20260909",
        "20260910",
    ]


def test_shadow_schema_and_insert_are_idempotent(tmp_path: Path):
    path = tmp_path / "shadow.db"
    conn = sqlite3.connect(path)
    columns = ["dlvrReqNo", "prdctSno", "dlvrReqChgOrd", "dminsttCd", "prdctAmt"]
    sync.initialize_shadow(conn, columns)
    row = {
        "dlvrReqNo": "A",
        "prdctSno": "1",
        "dlvrReqChgOrd": "0",
        "dminsttCd": "B",
        "prdctAmt": "100",
    }
    conn.execute("BEGIN IMMEDIATE")
    sync.store_rows(conn, [row], columns)
    conn.commit()
    conn.execute("BEGIN IMMEDIATE")
    sync.store_rows(conn, [row], columns)
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM shopping_cntrct").fetchone()[0] == 1
    sync.record_status(
        conn,
        "20260909",
        "complete",
        source_total=1,
        source_received=1,
        busan_rows=1,
        stored_rows=1,
        request_count=1,
        rate_remaining=800,
    )
    assert sync.status_summary(conn, path)["completed_days"] == 1
    conn.close()


def test_live_partition_replacement_removes_stale_rows(tmp_path: Path):
    path = tmp_path / "live.db"
    conn = sqlite3.connect(path)
    columns = [
        "dlvrReqNo",
        "prdctSno",
        "dlvrReqChgOrd",
        "dlvrReqRcptDate",
        "dminsttCd",
        "prdctAmt",
    ]
    sync.initialize_shadow(conn, columns)
    old_rows = [
        {"dlvrReqNo": "OLD", "prdctSno": "1", "dlvrReqChgOrd": "0", "dlvrReqRcptDate": "2026-09-09", "dminsttCd": "B", "prdctAmt": "10"},
        {"dlvrReqNo": "KEEP", "prdctSno": "1", "dlvrReqChgOrd": "0", "dlvrReqRcptDate": "2026-09-08", "dminsttCd": "B", "prdctAmt": "20"},
    ]
    conn.execute("BEGIN IMMEDIATE")
    sync.store_rows(conn, old_rows, columns)
    conn.commit()
    replacement = {
        "dlvrReqNo": "NEW",
        "prdctSno": "1",
        "dlvrReqChgOrd": "0",
        "dlvrReqRcptDate": "20260909",
        "dminsttCd": "B",
        "prdctAmt": "30",
    }
    conn.execute("BEGIN IMMEDIATE")
    sync.store_rows(
        conn,
        [replacement],
        columns,
        target_date="20260909",
        replace_partition=True,
    )
    conn.commit()
    assert conn.execute(
        "SELECT dlvrReqNo FROM shopping_cntrct ORDER BY dlvrReqNo"
    ).fetchall() == [("KEEP",), ("NEW",)]
    conn.close()


def test_storage_guard_blocks_low_free_space(monkeypatch, tmp_path: Path):
    class Usage:
        total = 100
        used = 95
        free = 5

    monkeypatch.setattr(sync.shutil, "disk_usage", lambda _: Usage())
    shadow = tmp_path / "shadow.db"
    shadow.write_bytes(b"")
    try:
        sync.checked_storage(tmp_path, shadow, min_free_bytes=10, max_shadow_bytes=100)
    except sync.StorageGuardTriggered:
        pass
    else:
        raise AssertionError("storage guard did not trigger")


def test_live_storage_guard_does_not_apply_shadow_file_limit(monkeypatch, tmp_path: Path):
    class Usage:
        total = 10_000
        used = 1_000
        free = 9_000

    monkeypatch.setattr(sync.shutil, "disk_usage", lambda _: Usage())
    result = sync.checked_storage(
        tmp_path,
        None,
        min_free_bytes=8_000,
        max_shadow_bytes=1,
    )
    assert result["guarded_file_bytes"] == 0


def test_quota_guard_reserves_requests():
    client = object.__new__(sync.ApiClient)
    client.request_count = 10
    client.max_requests = 20
    client.rate_limit_remaining = 155
    client.reserve_requests = 150
    client._ensure_budget(5)
    try:
        client._ensure_budget(6)
    except sync.QuotaReserveReached:
        pass
    else:
        raise AssertionError("quota reserve did not trigger")


def test_zero_result_is_confirmed_with_second_request():
    class Client:
        request_count = 0

        def __init__(self):
            self.results = [([], 0), ([{"dlvrReqNo": "A"}], 1)]

        def fetch_day(self, _target_date):
            self.request_count += 1
            rows, total = self.results.pop(0)
            return rows, total, 1

    client = Client()
    rows, total, requests_used = sync.fetch_day_with_zero_confirmation(
        client, "20260909"
    )
    assert total == 1
    assert rows == [{"dlvrReqNo": "A"}]
    assert requests_used == 2


def test_api_key_is_redacted_from_logged_errors():
    value = sync.safe_error_text(
        "HTTP 500 https://example.test/path?serviceKey=top-secret-value&pageNo=1"
    )
    assert "top-secret-value" not in value
    assert "serviceKey=***" in value
