from __future__ import annotations

from datetime import date
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from pathlib import Path

from company_locality import apply_company_changes, ensure_locality_schema, status_at
from company_reconcile import (
    CompanyLookupError,
    RevalidationPolicy,
    biznos_for_bucket,
    bucket_for_bizno,
    drain_revalidation_queue,
    main,
    revalidate_bucket,
)
from maintenance_lock import (
    LocalityPaths,
    configure_locality_paths,
    guarded_write_session,
    read_control_revision,
    read_data_generation,
)


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

    def test_authoritative_apply_rolls_back_when_verification_completion_fails(self):
        before = self.conn.execute(
            "SELECT rgnNm FROM company_master WHERE bizno = '1234567890'"
        ).fetchone()[0]
        verified_before = self.conn.execute(
            "SELECT last_verified_at FROM company_locality_status WHERE bizno = '1234567890'"
        ).fetchone()[0]
        with patch("company_reconcile._mark_verified", side_effect=RuntimeError("post-status failure")):
            with self.assertRaisesRegex(RuntimeError, "post-status failure"):
                revalidate_bucket(
                    self.conn,
                    FakeCompanyClient({"1234567890": source_item("1234567890", "경남", "본사", "202608160900")}),
                    date(2026, 8, 16),
                    bucket_count=1,
                    workers=1,
                )
        self.assertEqual(
            self.conn.execute(
                "SELECT status, last_verified_at FROM company_locality_status WHERE bizno = '1234567890'"
            ).fetchone(),
            ("active_local", verified_before),
        )
        self.assertEqual(
            self.conn.execute("SELECT rgnNm FROM company_master WHERE bizno = '1234567890'").fetchone()[0],
            before,
        )
        queue = self.conn.execute(
            "SELECT status, attempt_count, last_response_class, last_success_at FROM company_revalidation_queue"
        ).fetchone()
        self.assertEqual(queue[:2], ("pending", 1))
        self.assertEqual(queue[2:], (None, None))
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM company_sync_response_metric "
                "WHERE job_name = 'company_revalidation'"
            ).fetchone()[0],
            0,
        )

    def test_queue_is_guarded_cache_input_without_control_clock_increment(self):
        before = (read_data_generation(self.conn), read_control_revision(self.conn))
        with self.assertRaisesRegex(sqlite3.IntegrityError, "guarded session"):
            self.conn.execute("INSERT INTO company_revalidation_queue (bizno, status) VALUES ('9999999999', 'pending')")
        with guarded_write_session(self.conn):
            self.conn.execute("INSERT INTO company_revalidation_queue (bizno, status) VALUES ('9999999999', 'pending')")
        self.assertEqual(
            (read_data_generation(self.conn), read_control_revision(self.conn)),
            (before[0] + 1, before[1]),
        )

    def test_expired_queue_lease_is_reclaimed_before_lookup(self):
        with guarded_write_session(self.conn):
            self.conn.execute(
                "INSERT INTO company_revalidation_queue "
                "(bizno, status, attempt_count, next_attempt_at, lease_owner, lease_expires_at) "
                "VALUES (?, 'pending', 4, ?, 'dead-worker', ?)",
                ("1234567890", "2026-08-16 00:00:00+09:00", "2026-08-16 09:59:59+09:00"),
            )
        source = FakeCompanyClient({"1234567890": source_item("1234567890", "부산", "본사", "202608160900")})
        drain_revalidation_queue(self.conn, source, "2026-08-16 10:00:00+09:00", request_budget=1)
        self.assertEqual(source.calls, ["1234567890"])
        self.assertEqual(
            self.conn.execute(
                "SELECT status, attempt_count, lease_owner, lease_expires_at FROM company_revalidation_queue WHERE bizno='1234567890'"
            ).fetchone(),
            ("complete", 5, None, None),
        )

    def test_budget_and_circuit_deferrals_are_deterministic(self):
        with guarded_write_session(self.conn):
            self.conn.execute(
                "INSERT INTO company_locality_status (bizno, status, source_effective_at, observed_at, last_verified_at) VALUES (?, 'active_local', ?, ?, ?)",
                ("2222222222", "1900-01-01 00:00:00+09:00", NOW, NOW),
            )
        source = FakeCompanyClient({
            "1234567890": source_item("1234567890", "부산", "본사", "202608160900"),
            "2222222222": source_item("2222222222", "부산", "본사", "202608160900"),
        })
        result = revalidate_bucket(
            self.conn, source, date(2026, 8, 16), bucket_count=1, workers=1,
            policy=RevalidationPolicy(max_attempts=1, daily_call_budget=1),
        )
        self.assertEqual((result.calls, result.deferred, result.circuit_state), (1, 1, "budget_exhausted"))

    def test_circuit_breaker_defers_remaining_claims_after_a_terminal_failure(self):
        with guarded_write_session(self.conn):
            self.conn.execute(
                "INSERT INTO company_locality_status (bizno, status, source_effective_at, observed_at, last_verified_at) VALUES (?, 'active_local', ?, ?, ?)",
                ("2222222222", "1900-01-01 00:00:00+09:00", NOW, NOW),
            )
        result = revalidate_bucket(
            self.conn,
            FakeCompanyClient({"1234567890": OSError("down"), "2222222222": source_item("2222222222", "부산", "본사", "202608160900")}),
            date(2026, 8, 16), bucket_count=1, workers=1,
            policy=RevalidationPolicy(max_attempts=1, circuit_failure_threshold=1),
        )
        self.assertEqual((result.calls, result.failed, result.deferred, result.circuit_state), (1, 1, 1, "open"))

    def test_malformed_and_transport_failures_keep_status_and_record_classes(self):
        policy = RevalidationPolicy(max_attempts=1)
        malformed = revalidate_bucket(
            self.conn, FakeCompanyClient({"1234567890": {"bizno": "1234567890"}}),
            date(2026, 8, 16), bucket_count=1, workers=1, policy=policy,
        )
        self.assertEqual(malformed.failed, 1)
        self.assertEqual(
            self.conn.execute("SELECT company_locality_status.status, last_response_class FROM company_locality_status "
                              "LEFT JOIN company_revalidation_queue USING (bizno) WHERE bizno='1234567890'").fetchone(),
            ("active_local", "invalid_response"),
        )
        transport = revalidate_bucket(
            self.conn, FakeCompanyClient({"1234567890": OSError("network down")}),
            date(2026, 8, 17), bucket_count=1, workers=1, policy=policy,
        )
        self.assertEqual(transport.failed, 1)
        self.assertEqual(
            self.conn.execute("SELECT last_response_class FROM company_revalidation_queue WHERE bizno='1234567890'").fetchone()[0],
            "exception",
        )

    def test_terminal_retry_after_sets_the_durable_retry_deadline(self):
        result = revalidate_bucket(
            self.conn,
            FakeCompanyClient({"1234567890": CompanyLookupError("HTTP 429", retry_after=900)}),
            "2026-08-16 10:00:00+09:00",
            bucket_count=1,
            workers=1,
            policy=RevalidationPolicy(max_attempts=1, backoff_seconds=5, jitter_seconds=2, random_uniform=lambda _a, _b: 1),
        )
        self.assertEqual(result.failed, 1)
        self.assertEqual(
            self.conn.execute(
                "SELECT retry_after_seconds, next_attempt_at FROM company_revalidation_queue WHERE bizno='1234567890'"
            ).fetchone(),
            (900.0, "2026-08-16 10:15:00+09:00"),
        )

    def test_apply_without_credentials_does_not_create_or_open_the_database(self):
        database_path = Path(self.tempdir.name) / "credential-preflight.db"
        with self.assertRaises(SystemExit) as error:
            main(["--date", "2026-08-16", "--apply", "--company-db", str(database_path)])
        self.assertEqual(error.exception.code, 2)
        self.assertFalse(database_path.exists())

    def test_apply_without_credentials_does_not_change_an_existing_database(self):
        database_path = Path(self.tempdir.name) / "credential-preflight-existing.db"
        database_path.write_bytes(b"leave this database untouched")
        with self.assertRaises(SystemExit) as error:
            main(["--date", "2026-08-16", "--apply", "--company-db", str(database_path)])
        self.assertEqual(error.exception.code, 2)
        self.assertEqual(database_path.read_bytes(), b"leave this database untouched")


class CompanyReconcileConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        root = Path(self.tempdir.name)
        self.database_path = root / "company.db"
        self.paths = LocalityPaths(
            self.database_path, root / "procurement.db", root / "maintenance.lock", root / "transition.json",
            root / "marker", root / "pointer.json",
        )
        configure_locality_paths(self.paths)
        self.paths.pointer_path.write_text('{"active_generation_id":null}', encoding="ascii")
        conn = sqlite3.connect(self.database_path)
        conn.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, corpNm TEXT, rgnNm TEXT, hdoffceDivNm TEXT, chgDt TEXT)")
        conn.execute("INSERT INTO company_master VALUES ('1234567890', 'Bootstrap', '부산', '본사', '')")
        conn.commit()
        ensure_locality_schema(conn, paths=self.paths)
        conn.commit()
        conn.close()
        procurement = sqlite3.connect(self.paths.procurement_db_path)
        procurement.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
        procurement.commit()
        ensure_locality_schema(procurement, paths=self.paths)
        procurement.commit()
        procurement.close()

    def tearDown(self):
        self.tempdir.cleanup()

    def test_only_one_concurrent_run_claims_a_due_supplier(self):
        started = threading.Event()
        release = threading.Event()

        class BlockingClient:
            def __init__(self):
                self.calls = 0
                self.lock = threading.Lock()

            def lookup(self, bizno):
                with self.lock:
                    self.calls += 1
                    started.set()
                release.wait(1)
                return source_item(bizno, "부산", "본사", "202608160900")

        client = BlockingClient()
        failures = []

        def run():
            conn = sqlite3.connect(self.database_path)
            try:
                revalidate_bucket(conn, client, date(2026, 8, 16), bucket_count=1, workers=1)
            except Exception as error:
                failures.append(error)
            finally:
                conn.close()

        first, second = threading.Thread(target=run), threading.Thread(target=run)
        first.start()
        self.assertTrue(started.wait(1))
        second.start()
        time.sleep(0.1)
        release.set()
        first.join(2)
        second.join(2)
        self.assertEqual((client.calls, failures), (1, []))

    def test_workers_allow_bounded_parallel_http_lookups(self):
        conn = sqlite3.connect(self.database_path)
        try:
            with guarded_write_session(conn):
                for bizno in ("2222222222", "3333333333"):
                    conn.execute(
                        "INSERT INTO company_locality_status (bizno, status, source_effective_at, observed_at, last_verified_at) VALUES (?, 'active_local', ?, ?, ?)",
                        (bizno, "1900-01-01 00:00:00+09:00", NOW, NOW),
                    )

            class ParallelClient:
                def __init__(self):
                    self.active = 0
                    self.maximum = 0
                    self.lock = threading.Lock()

                def lookup(self, bizno):
                    with self.lock:
                        self.active += 1
                        self.maximum = max(self.maximum, self.active)
                    time.sleep(0.05)
                    with self.lock:
                        self.active -= 1
                    return source_item(bizno, "부산", "본사", "202608160900")

            client = ParallelClient()
            revalidate_bucket(
                conn, client, date(2026, 8, 16), bucket_count=1, workers=3,
                policy=RevalidationPolicy(max_attempts=1, requests_per_second=0),
            )
        finally:
            conn.close()
        self.assertGreaterEqual(client.maximum, 2)
        self.assertLessEqual(client.maximum, 3)
