import sqlite3
import contextlib
import datetime
from pathlib import Path

import pytest

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


LIVE_COLUMNS = [
    "dlvrReqNo", "prdctSno", "dlvrReqChgOrd", "dlvrReqRcptDate",
    "dminsttCd", "prdctAmt",
]


def _live_fixture(tmp_path):
    production = tmp_path / "production.db"
    agency = tmp_path / "agency.db"
    conn = sqlite3.connect(production)
    cols = ", ".join(f'"{name}" TEXT' for name in LIVE_COLUMNS)
    conn.execute(f"CREATE TABLE shopping_cntrct ({cols})")
    sync.initialize_live_sync_log(conn)
    with sqlite3.connect(agency) as ac:
        ac.execute("CREATE TABLE agency_master (dminsttCd TEXT)")
        ac.execute("INSERT INTO agency_master VALUES ('B')")
    return production, agency, conn


def _row(day, identity="OLD"):
    return {
        "dlvrReqNo": identity, "prdctSno": "1", "dlvrReqChgOrd": "0",
        "dlvrReqRcptDate": day, "dminsttCd": "B", "prdctAmt": "100",
    }


def _stub_main_boundaries(monkeypatch, client):
    monkeypatch.setattr(sync, "load_env_file", lambda _: None)
    monkeypatch.setattr(sync, "file_lock", lambda _: contextlib.nullcontext())
    monkeypatch.setattr(sync, "ApiClient", lambda *args, **kwargs: client)
    monkeypatch.setattr(
        sync, "checked_storage", lambda *a, **k: {"filesystem_free": 100 * 1024**3}
    )


def test_live_zero_source_preserves_existing_nonempty_partition(monkeypatch, tmp_path):
    production, agency, conn = _live_fixture(tmp_path)
    sync.store_rows(conn, [_row("20260909")], LIVE_COLUMNS)
    conn.commit()

    class ZeroClient:
        request_count = 0
        rate_limit_remaining = 800

        def fetch_day(self, day):
            self.request_count += 1
            return [], 0, 1

    _stub_main_boundaries(monkeypatch, ZeroClient())
    code = sync.main([
        "--live", "--production-db", str(production), "--agency-db", str(agency),
        "--start", "20260909", "--end", "20260909",
    ])
    assert code != 0
    assert conn.execute("SELECT dlvrReqNo, prdctAmt FROM shopping_cntrct").fetchall() == [("OLD", "100")]
    assert conn.execute("SELECT status FROM shopping_sync_log").fetchone() == ("failed",)
    conn.close()


def test_all_day_failures_return_nonzero(monkeypatch, tmp_path):
    production, agency, conn = _live_fixture(tmp_path)

    class FailedClient:
        request_count = 0
        rate_limit_remaining = 800

        def fetch_day(self, day):
            self.request_count += 1
            raise sync.SyncError("upstream outage fixture")

    _stub_main_boundaries(monkeypatch, FailedClient())
    assert sync.main([
        "--live", "--production-db", str(production), "--agency-db", str(agency),
        "--start", "20260909", "--end", "20260909",
    ]) != 0
    assert conn.execute("SELECT status FROM shopping_sync_log").fetchone() == ("failed",)
    conn.close()


def test_quota_deferral_and_unattempted_dates_return_nonzero(monkeypatch, tmp_path, capsys):
    production, agency, conn = _live_fixture(tmp_path)

    class QuotaClient:
        request_count = 0
        rate_limit_remaining = 150

        def fetch_day(self, day):
            raise sync.QuotaReserveReached("fixture reserve")

    _stub_main_boundaries(monkeypatch, QuotaClient())
    assert sync.main([
        "--live", "--production-db", str(production), "--agency-db", str(agency),
        "--start", "20260908", "--end", "20260909",
    ]) != 0
    output = capsys.readouterr().out
    assert '"unprocessed_days": 1' in output
    assert '"remaining_days": 2' in output
    assert conn.execute("SELECT target_date, status FROM shopping_sync_log").fetchone() == ("20260909", "deferred_quota")
    conn.close()


def test_live_selection_recovers_old_failures_and_gaps_without_refetching_existing_history(monkeypatch, tmp_path):
    _, _, conn = _live_fixture(tmp_path)
    for day in sync.inclusive_dates("20260901", "20260911"):
        if day not in ("20260902", "20260904"):
            sync.store_rows(conn, [_row(day, day)], LIVE_COLUMNS)
    conn.commit()
    sync.record_status(conn, "20260901", "failed")
    sync.record_status(conn, "20260902", "complete_zero", request_count=2)
    sync.record_status(conn, "20260903", "deferred_quota")

    class FrozenDate(datetime.date):
        @classmethod
        def today(cls):
            return cls(2026, 9, 13)

    monkeypatch.setattr(sync.dt, "date", FrozenDate)
    args = sync.build_parser().parse_args(["--live", "--coverage-start", "20260901"])
    dates = sync.target_dates(args, conn)
    assert dates == [
        "20260911", "20260901", "20260903", "20260904",
        "20260910", "20260909", "20260908", "20260907", "20260906", "20260905",
    ]
    assert "20260902" not in dates
    conn.close()


def test_existing_history_without_logs_is_not_scheduled_again(monkeypatch, tmp_path):
    _, _, conn = _live_fixture(tmp_path)
    for day in sync.inclusive_dates("20260901", "20260911"):
        sync.store_rows(conn, [_row(day, day)], LIVE_COLUMNS)
    conn.commit()

    class FrozenDate(datetime.date):
        @classmethod
        def today(cls):
            return cls(2026, 9, 13)

    monkeypatch.setattr(sync.dt, "date", FrozenDate)
    args = sync.build_parser().parse_args(["--live", "--coverage-start", "20260901"])
    assert sync.target_dates(args, conn) == list(reversed(sync.inclusive_dates("20260905", "20260911")))
    conn.close()


def test_date_write_and_status_rollback_together(monkeypatch, tmp_path):
    production, agency, conn = _live_fixture(tmp_path)
    sync.store_rows(conn, [_row("20260909")], LIVE_COLUMNS)
    conn.commit()

    class CompleteClient:
        request_count = 0
        rate_limit_remaining = 800

        def fetch_day(self, day):
            self.request_count += 1
            return [_row(day, "NEW")], 1, 1

    original = sync.record_status

    def fail_success_status(connection, day, status, **kwargs):
        if status == "complete":
            raise sqlite3.OperationalError("fixture status insert failure")
        return original(connection, day, status, **kwargs)

    _stub_main_boundaries(monkeypatch, CompleteClient())
    monkeypatch.setattr(sync, "record_status", fail_success_status)
    assert sync.main([
        "--live", "--production-db", str(production), "--agency-db", str(agency),
        "--start", "20260909", "--end", "20260909",
    ]) != 0
    assert conn.execute("SELECT dlvrReqNo, prdctAmt FROM shopping_cntrct").fetchall() == [("OLD", "100")]
    assert conn.execute("SELECT status FROM shopping_sync_log").fetchone() == ("failed",)
    conn.close()


def _page_client(pages):
    client = object.__new__(sync.ApiClient)
    client.request_count = 0
    client.max_requests = 100
    client.reserve_requests = 150
    client.rate_limit_remaining = 1000

    def fetch_page(day, page_no):
        client.request_count += 1
        return pages[page_no - 1]

    client.fetch_page = fetch_page
    return client


def test_duplicate_pages_do_not_pass_source_completeness():
    first = [_row("20260909", str(i)) for i in range(999)]
    client = _page_client([(first, 1000), ([first[0]], 1000)])
    with pytest.raises(sync.SyncError, match="duplicate source key"):
        client.fetch_day("20260909")


@pytest.mark.parametrize("missing_column", sync.KEY_COLUMNS)
def test_missing_source_identity_is_not_accepted(missing_column):
    invalid = _row("20260909")
    invalid[missing_column] = None
    client = _page_client([([invalid], 1)])
    with pytest.raises(sync.SyncError, match="missing source key"):
        client.fetch_day("20260909")


def test_wrong_source_receipt_date_is_not_accepted():
    client = _page_client([([_row("20260908")], 1)])
    with pytest.raises(sync.SyncError, match="source receipt date"):
        client.fetch_day("20260909")


def test_complete_distinct_pages_are_accepted():
    first = [_row("20260909", str(i)) for i in range(999)]
    client = _page_client([(first, 1000), ([_row("20260909", "LAST")], 1000)])
    rows, total, requests = client.fetch_day("20260909")
    assert total == len(rows) == 1000
    assert requests == 2


@pytest.mark.parametrize("second_page, second_total, message", [
    ([], 1000, "incomplete source rows"),
    ([_row("20260909", "LAST")], 1001, "totalCount changed"),
])
def test_partial_or_changing_page_totals_are_rejected(second_page, second_total, message):
    first = [_row("20260909", str(i)) for i in range(999)]
    client = _page_client([(first, 1000), (second_page, second_total)])
    with pytest.raises(sync.SyncError, match=message):
        client.fetch_day("20260909")


def test_numeric_zero_change_order_remains_a_valid_identity(tmp_path):
    value = _row("20260909")
    value["dlvrReqChgOrd"] = 0
    client = _page_client([([value], 1)])
    rows, _, _ = client.fetch_day("20260909")
    _, _, conn = _live_fixture(tmp_path)
    sync.store_rows(conn, rows, LIVE_COLUMNS)
    conn.commit()
    sync.store_rows(conn, rows, LIVE_COLUMNS)
    conn.commit()
    assert conn.execute("SELECT dlvrReqChgOrd FROM shopping_cntrct").fetchall() == [("0",)]
    conn.close()
