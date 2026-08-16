from __future__ import annotations

from datetime import date
import sqlite3
import tempfile
import unittest
from pathlib import Path

from company_locality import apply_company_changes, ensure_locality_schema, status_at
from company_reconcile import (
    biznos_for_bucket,
    bucket_for_bizno,
    drain_revalidation_queue,
    revalidate_bucket,
)
from maintenance_lock import LocalityPaths, configure_locality_paths, guarded_write_session


NOW = "2026-08-16 12:00:00+09:00"


def source_item(bizno, region, division, changed_at):
    return {
        "bizno": bizno,
        "corpNm": "Example Supplier",
        "rgnNm": region,
        "hdoffceDivNm": division,
        "chgDt": changed_at,
    }


class FakeCompanyClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def lookup(self, bizno):
        self.calls.append(bizno)
        response = self.responses[bizno]
        if isinstance(response, Exception):
            raise response
        return response


class CompanyReconcileTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        root = Path(self.tempdir.name)
        self.paths = LocalityPaths.for_in_memory_tests(
            root / "maintenance.lock", root / "transition.json", root / "marker", root / "pointer.json"
        )
        configure_locality_paths(self.paths)
        self.paths.pointer_path.write_text('{"active_generation_id":null}', encoding="ascii")
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute(
            "CREATE TABLE company_master (bizno TEXT PRIMARY KEY, corpNm TEXT, rgnNm TEXT, hdoffceDivNm TEXT, chgDt TEXT)"
        )
        self.conn.execute(
            "INSERT INTO company_master VALUES (?, ?, ?, ?, ?)",
            ("1234567890", "Bootstrap", "부산광역시", "본사", ""),
        )
        self.conn.commit()
        ensure_locality_schema(self.conn, paths=self.paths)

    def tearDown(self):
        self.conn.close()
        self.tempdir.cleanup()

    def test_bucket_assignment_is_stable(self):
        self.assertEqual(bucket_for_bizno("1234567890"), bucket_for_bizno("123-45-67890"))

    def test_bucket_lists_each_normalized_supplier_once_in_order(self):
        with guarded_write_session(self.conn):
            self.conn.execute(
                "INSERT INTO company_locality_status (bizno, status, source_effective_at, observed_at, last_verified_at) VALUES (?, ?, ?, ?, ?)",
                ("9876543210", "unverified", NOW, NOW, NOW),
            )
        bucket = bucket_for_bizno("9876543210", 7)
        self.assertIn("9876543210", biznos_for_bucket(self.conn, bucket, 7))
        self.assertEqual(biznos_for_bucket(self.conn, bucket, 7), sorted(biznos_for_bucket(self.conn, bucket, 7)))

    def test_revalidation_reactivates_returning_supplier_without_changing_old_event(self):
        apply_company_changes(
            self.conn,
            [source_item("1234567890", "경남", "본사", "202608150900")],
            "20260815",
            "outbound",
            NOW,
        )
        source = FakeCompanyClient({"1234567890": source_item("1234567890", "부산", "본사", "202608160900")})
        result = revalidate_bucket(self.conn, source, date(2026, 8, 16), bucket_count=1, workers=1)
        self.assertEqual(result.activated, 1)
        self.assertEqual(status_at(self.conn, "1234567890", "2026-08-16 09:00:00"), "active_local")
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM company_locality_event WHERE job_id = 'outbound'").fetchone()[0],
            1,
        )

    def test_revalidation_preserves_outbound_and_branch_lifecycle(self):
        source = FakeCompanyClient({"1234567890": source_item("1234567890", "경남", "지사", "202608160900")})
        result = revalidate_bucket(self.conn, source, date(2026, 8, 16), bucket_count=1, workers=1)
        self.assertEqual((result.deactivated, result.branch_changed), (1, 0))
        self.assertEqual(status_at(self.conn, "1234567890", "2026-08-16 09:00:00"), "moved_out")

    def test_unverified_direct_response_retains_confirmed_status_and_queues_recovery(self):
        before = self.conn.execute(
            "SELECT last_verified_at FROM company_locality_status WHERE bizno = '1234567890'"
        ).fetchone()[0]
        result = revalidate_bucket(
            self.conn,
            FakeCompanyClient({"1234567890": None}),
            date(2026, 8, 16),
            bucket_count=1,
            workers=1,
        )
        self.assertEqual(result.failed, 1)
        self.assertEqual(
            self.conn.execute("SELECT status, last_verified_at FROM company_locality_status WHERE bizno = '1234567890'").fetchone(),
            ("active_local", before),
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT status, attempt_count, last_response_class FROM company_revalidation_queue WHERE bizno = '1234567890'"
            ).fetchone(),
            ("failed", 1, "unauthoritative_empty"),
        )

    def test_due_queue_is_drained_before_today_bucket_and_respects_budget(self):
        with guarded_write_session(self.conn):
            self.conn.execute(
                "INSERT INTO company_locality_status (bizno, status, source_effective_at, observed_at, last_verified_at) VALUES (?, ?, ?, ?, ?)",
                ("2222222222", "unverified", "1900-01-01 00:00:00+09:00", NOW, NOW),
            )
            self.conn.execute(
                "INSERT INTO company_revalidation_queue (bizno, status, next_attempt_at) VALUES (?, 'pending', ?)",
                ("2222222222", "2026-08-16 00:00:00+09:00"),
            )
        source = FakeCompanyClient({"2222222222": source_item("2222222222", "부산", "본사", "202608160900")})
        result = drain_revalidation_queue(self.conn, source, "2026-08-16 10:00:00+09:00", request_budget=1)
        self.assertEqual((result.calls, result.activated), (1, 1))
        self.assertEqual(source.calls, ["2222222222"])
        self.assertEqual(
            self.conn.execute("SELECT status FROM company_revalidation_queue WHERE bizno = '2222222222'").fetchone()[0],
            "complete",
        )
