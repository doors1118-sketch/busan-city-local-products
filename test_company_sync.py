import sqlite3
import tempfile
import unittest
from pathlib import Path

from company_locality import ensure_locality_schema, fail_sync_job, start_sync_job
from maintenance_lock import LocalityPaths, configure_locality_paths
from company_sync import (
    CompanySyncPolicy,
    IncompleteCompanyBatch,
    fetch_complete_change_batch,
    pending_supplier_dates,
    sync_company_change_date,
)


NOW = "2026-08-16 12:00:00+09:00"


def item(suffix, **overrides):
    value = {
        "bizno": f"123456789{suffix}",
        "corpNm": "Example Supplier",
        "rgnNm": "부산광역시",
        "hdoffceDivNm": "본사",
        "chgDt": "202608160900",
    }
    value.update(overrides)
    return value


class FakePages:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def __call__(self, page_number):
        self.calls.append(page_number)
        result = self.pages[page_number]
        if isinstance(result, BaseException):
            raise result
        return result


class CompanySyncTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        root = Path(self.tempdir.name)
        self.company_db_path = root / "company.db"
        self.procurement_db_path = root / "procurement.db"
        self.paths = LocalityPaths(
            self.company_db_path,
            self.procurement_db_path,
            root / "coordination" / "maintenance.lock",
            root / "coordination" / "transition.json",
            root / "coordination" / "writes_paused",
            root / "coordination" / "active_generation.json",
        )
        configure_locality_paths(self.paths)
        self.paths.pointer_path.parent.mkdir(parents=True)
        self.paths.pointer_path.write_text('{"active_generation_id":null}', encoding="ascii")
        for database_path in (self.company_db_path, self.procurement_db_path):
            conn = sqlite3.connect(database_path)
            conn.execute(
                """
                CREATE TABLE company_master (
                    bizno TEXT PRIMARY KEY,
                    corpNm TEXT,
                    rgnNm TEXT,
                    hdoffceDivNm TEXT,
                    chgDt TEXT,
                    adrs TEXT,
                    dtlAdrs TEXT
                )
                """
            )
            ensure_locality_schema(conn, paths=self.paths)
            conn.close()
        self.conn = sqlite3.connect(self.company_db_path)
        self.fast_policy = CompanySyncPolicy(
            max_attempts=3,
            requests_per_second=1000,
            daily_call_budget=20,
            backoff_seconds=0,
            jitter_seconds=0,
        )

    def tearDown(self):
        self.conn.close()
        self.tempdir.cleanup()

    def test_missing_second_page_raises_and_applies_nothing(self):
        fetch = FakePages({1: ([item("1")], 1000), 2: RuntimeError("timeout")})
        with self.assertRaises(IncompleteCompanyBatch):
            sync_company_change_date(
                "20260816", fetch, self.company_db_path, policy=self.fast_policy
            )
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM company_master").fetchone()[0], 0
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT status FROM company_sync_job_log WHERE job_name='company_changes' AND source_date='20260816'"
            ).fetchone()[0],
            "failed",
        )

    def test_count_mismatch_raises(self):
        fetch = FakePages({1: ([item("1")], 2)})
        with self.assertRaisesRegex(IncompleteCompanyBatch, "expected=2 received=1"):
            fetch_complete_change_batch("20260816", fetch, policy=self.fast_policy)

    def test_duplicate_source_identity_raises_even_when_count_matches(self):
        fetch = FakePages({1: ([item("1"), item("1")], 2)})
        with self.assertRaisesRegex(IncompleteCompanyBatch, "duplicate source identity"):
            fetch_complete_change_batch("20260816", fetch, policy=self.fast_policy)

    def test_total_count_drift_raises_before_any_apply(self):
        fetch = FakePages({1: ([item("1")], 1000), 2: ([item("2")], 999)})
        with self.assertRaisesRegex(IncompleteCompanyBatch, "totalCount drift"):
            sync_company_change_date(
                "20260816", fetch, self.company_db_path, policy=self.fast_policy
            )
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM company_master").fetchone()[0], 0
        )

    def test_supplier_failure_is_pending_even_when_contract_date_succeeded(self):
        start_sync_job(self.conn, "company_changes", "20260815", started_at=NOW)
        fail_sync_job(
            self.conn,
            "company_changes",
            "20260815",
            "page 2 timeout",
            completed_at=NOW,
        )
        self.assertEqual(
            pending_supplier_dates(self.conn, "20260816"), ["20260815", "20260816"]
        )

    def test_complete_batch_applies_then_records_success_and_metrics(self):
        summary = sync_company_change_date(
            "20260816", FakePages({1: ([item("1")], 1)}), self.company_db_path, policy=self.fast_policy
        )
        self.assertEqual((summary.received, summary.applied), (1, 1))
        self.assertEqual(
            self.conn.execute(
                "SELECT status, expected_rows, received_rows, page_count, call_count, call_budget, circuit_state "
                "FROM company_sync_job_log WHERE job_name='company_changes' AND source_date='20260816'"
            ).fetchone(),
            ("success", 1, 1, 1, 1, 20, "closed"),
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT response_class, response_count FROM company_sync_response_metric "
                "WHERE job_name='company_changes' AND source_date='20260816'"
            ).fetchall(),
            [("success", 1)],
        )

    def test_budget_exhaustion_is_failed_and_persisted(self):
        policy = CompanySyncPolicy(
            max_attempts=3,
            requests_per_second=1000,
            daily_call_budget=1,
            backoff_seconds=0,
            jitter_seconds=0,
        )
        fetch = FakePages({1: ([item("1")], 1000), 2: ([item("2")], 1000)})
        with self.assertRaisesRegex(IncompleteCompanyBatch, "call budget exhausted"):
            sync_company_change_date("20260816", fetch, self.company_db_path, policy=policy)
        self.assertEqual(
            self.conn.execute(
                "SELECT status, call_count, call_budget, circuit_state FROM company_sync_job_log "
                "WHERE job_name='company_changes' AND source_date='20260816'"
            ).fetchone(),
            ("failed", 1, 1, "budget_exhausted"),
        )


if __name__ == "__main__":
    unittest.main()
