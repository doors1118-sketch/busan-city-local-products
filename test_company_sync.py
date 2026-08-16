import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import daily_pipeline_sync as pipeline
from company_locality import ensure_locality_schema, fail_sync_job, start_sync_job
from maintenance_lock import (
    LocalityPaths,
    WriteFenceError,
    configure_locality_paths,
    read_control_revision,
    read_data_generation,
    require_locality_paths,
    set_write_fence,
)
from company_sync import (
    CompanyFetchError,
    CompanyPage,
    CompanySyncPolicy,
    IncompleteCompanyBatch,
    fetch_complete_change_batch,
    finish_supplier_run,
    make_verified_company_page_reader,
    pending_supplier_dates,
    start_supplier_run,
    sync_company_change_date,
)
from public_api_recovery import enqueue_prespec_issues, pending_prespec_dates, record_prespec_attempt


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


def page(page_number, items, total_count, *, num_of_rows=999, **overrides):
    value = CompanyPage(tuple(items), total_count, page_number, num_of_rows)
    return value if not overrides else CompanyPage(
        value.items,
        value.total_count,
        value.page_number,
        value.num_of_rows,
        **overrides,
    )


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


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class StopPipeline(BaseException):
    pass


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
            root / "coordination" / "locality_writes_paused",
            root / "coordination" / "active_locality_generation.json",
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
        fetch = FakePages({1: page(1, [item("1")], 1000), 2: RuntimeError("timeout")})
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
        fetch = FakePages({1: page(1, [item("1")], 2)})
        with self.assertRaisesRegex(IncompleteCompanyBatch, "expected=2 received=1"):
            fetch_complete_change_batch("20260816", fetch, policy=self.fast_policy)

    def test_duplicate_source_identity_raises_even_when_count_matches(self):
        fetch = FakePages({1: page(1, [item("1"), item("1")], 2)})
        with self.assertRaisesRegex(IncompleteCompanyBatch, "duplicate source identity"):
            fetch_complete_change_batch("20260816", fetch, policy=self.fast_policy)

    def test_total_count_drift_raises_before_any_apply(self):
        fetch = FakePages({1: page(1, [item("1")], 1000), 2: page(2, [item("2")], 999)})
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
            "20260816", FakePages({1: page(1, [item("1")], 1)}), self.company_db_path, policy=self.fast_policy
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
        fetch = FakePages({1: page(1, [item("1")], 1000), 2: page(2, [item("2")], 1000)})
        with self.assertRaisesRegex(IncompleteCompanyBatch, "call budget exhausted"):
            sync_company_change_date("20260816", fetch, self.company_db_path, policy=policy)
        self.assertEqual(
            self.conn.execute(
                "SELECT status, call_count, call_budget, circuit_state FROM company_sync_job_log "
                "WHERE job_name='company_changes' AND source_date='20260816'"
            ).fetchone(),
            ("failed", 1, 1, "budget_exhausted"),
        )

    def test_malformed_response_attempt_is_counted_before_failure(self):
        with self.assertRaisesRegex(IncompleteCompanyBatch, "invalid page response"):
            sync_company_change_date("20260816", lambda _page: object(), self.company_db_path, policy=self.fast_policy)
        self.assertEqual(
            self.conn.execute(
                "SELECT status, call_count, call_budget, circuit_state FROM company_sync_job_log "
                "WHERE job_name='company_changes' AND source_date='20260816'"
            ).fetchone(),
            ("failed", 1, 20, "closed"),
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT response_class, response_count FROM company_sync_response_metric "
                "WHERE job_name='company_changes' AND source_date='20260816'"
            ).fetchall(),
            [("invalid_response", 1)],
        )

    def test_invalid_source_date_does_not_create_a_running_job(self):
        with self.assertRaisesRegex(ValueError, "source_date"):
            sync_company_change_date("not-a-date", lambda _page: object(), self.company_db_path, policy=self.fast_policy)
        self.assertIsNone(
            self.conn.execute(
                "SELECT status FROM company_sync_job_log WHERE job_name='company_changes' AND source_date='not-a-date'"
            ).fetchone()
        )

    def test_invalid_source_date_marks_supplied_run_failed_without_creating_date_job(self):
        run = start_supplier_run("20260816", self.company_db_path, policy=self.fast_policy)
        with self.assertRaisesRegex(ValueError, "source_date"):
            sync_company_change_date(
                "not-a-date", lambda _page: object(), self.company_db_path,
                policy=self.fast_policy, run=run,
            )
        finish_supplier_run(run)
        self.assertEqual(
            self.conn.execute(
                "SELECT status, circuit_state FROM company_sync_job_log "
                "WHERE job_name='company_changes_run' AND source_date='20260816'"
            ).fetchone(),
            ("failed", "closed"),
        )
        self.assertIsNone(
            self.conn.execute(
                "SELECT status FROM company_sync_job_log "
                "WHERE job_name='company_changes' AND source_date='not-a-date'"
            ).fetchone()
        )

    def test_missing_source_page_metadata_is_rejected_before_apply(self):
        missing = CompanyPage(tuple([item("1")]), 1, None, 999)
        with self.assertRaisesRegex(IncompleteCompanyBatch, "pageNo metadata"):
            sync_company_change_date("20260816", FakePages({1: missing}), self.company_db_path, policy=self.fast_policy)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM company_master").fetchone()[0], 0)

    def test_mismatched_source_page_metadata_is_rejected_before_apply(self):
        with self.assertRaisesRegex(IncompleteCompanyBatch, "numOfRows metadata"):
            sync_company_change_date(
                "20260816",
                FakePages({1: page(1, [item("1")], 1, num_of_rows=998)}),
                self.company_db_path,
                policy=self.fast_policy,
            )
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM company_master").fetchone()[0], 0)

    def test_shifted_source_page_number_is_rejected_before_apply(self):
        with self.assertRaisesRegex(IncompleteCompanyBatch, "pageNo metadata"):
            sync_company_change_date(
                "20260816",
                FakePages({1: page(2, [item("1")], 1)}),
                self.company_db_path,
                policy=self.fast_policy,
            )
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM company_master").fetchone()[0], 0)

    def test_missing_source_row_count_metadata_is_rejected_before_apply(self):
        missing = CompanyPage(tuple([item("1")]), 1, 1, None)
        with self.assertRaisesRegex(IncompleteCompanyBatch, "numOfRows metadata"):
            sync_company_change_date("20260816", FakePages({1: missing}), self.company_db_path, policy=self.fast_policy)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM company_master").fetchone()[0], 0)

    def test_explicit_not_found_code_without_body_metadata_is_verified_empty_batch(self):
        class Response:
            def read(self):
                return b'{"response":{"header":{"resultCode":"03","resultMsg":"NODATA"}}}'

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        reader = make_verified_company_page_reader(
            "20260816", api_url="https://supplier.example/api", service_key="key"
        )
        with patch("company_sync.urllib.request.urlopen", return_value=Response()):
            batch = fetch_complete_change_batch("20260816", reader, policy=self.fast_policy)
        self.assertEqual((batch.items, batch.total_count, batch.page_count), ((), 0, 0))

    def test_every_retry_attempt_is_persisted_as_a_response_metric(self):
        with self.assertRaises(IncompleteCompanyBatch):
            sync_company_change_date(
                "20260816", FakePages({1: RuntimeError("timeout")}), self.company_db_path, policy=self.fast_policy
            )
        self.assertEqual(
            self.conn.execute(
                "SELECT call_count, retry_count FROM company_sync_job_log "
                "WHERE job_name='company_changes' AND source_date='20260816'"
            ).fetchone(),
            (3, 2),
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT response_class, response_count FROM company_sync_response_metric "
                "WHERE job_name='company_changes' AND source_date='20260816'"
            ).fetchall(),
            [("exception", 3)],
        )

    def test_retry_after_qps_and_jitter_are_deterministic(self):
        clock = FakeClock()
        fetch = FakePages({
            1: CompanyFetchError("busy", retry_after=3),
            2: page(2, [item("2")], 2, num_of_rows=1),
        })
        calls = {1: 0}

        def reader(page_number):
            if page_number == 1:
                calls[1] += 1
                if calls[1] == 1:
                    raise CompanyFetchError("busy", retry_after=3)
                return page(1, [item("1")], 2, num_of_rows=1)
            return fetch(page_number)

        batch = fetch_complete_change_batch(
            "20260816",
            reader,
            rows_per_page=1,
            policy=CompanySyncPolicy(requests_per_second=2, backoff_seconds=0.5, jitter_seconds=0.25),
            clock=clock.monotonic,
            sleep=clock.sleep,
            random_uniform=lambda _low, _high: 0.25,
        )
        self.assertEqual((batch.total_count, batch.retry_count), (2, 1))
        self.assertEqual(clock.sleeps, [3.25, 0.5])

    def test_exponential_backoff_uses_injected_clock_and_randomness(self):
        clock = FakeClock()
        attempts = 0

        def reader(_page_number):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise RuntimeError("timeout")
            return page(1, [item("1")], 1)

        fetch_complete_change_batch(
            "20260816",
            reader,
            policy=CompanySyncPolicy(backoff_seconds=0.5, jitter_seconds=0.25),
            clock=clock.monotonic,
            sleep=clock.sleep,
            random_uniform=lambda _low, _high: 0.25,
        )
        self.assertEqual(clock.sleeps, [0.75, 1.25])

    def test_shared_supplier_run_budget_persists_and_blocks_later_dates(self):
        policy = CompanySyncPolicy(daily_call_budget=1, requests_per_second=1000, backoff_seconds=0, jitter_seconds=0)
        run = start_supplier_run("20260816", self.company_db_path, policy=policy)
        sync_company_change_date("20260814", FakePages({1: page(1, [item("1")], 1)}), self.company_db_path, policy=policy, run=run)
        second = FakePages({1: page(1, [item("2")], 1)})
        with self.assertRaisesRegex(IncompleteCompanyBatch, "call budget exhausted"):
            sync_company_change_date("20260815", second, self.company_db_path, policy=policy, run=run)
        self.assertEqual(second.calls, [])
        self.assertEqual(
            self.conn.execute(
                "SELECT call_count, call_budget, circuit_state FROM company_sync_job_log "
                "WHERE job_name='company_changes_run' AND source_date='20260816'"
            ).fetchone(),
            (1, 1, "budget_exhausted"),
        )
        resumed = start_supplier_run("20260816", self.company_db_path, policy=policy)
        self.assertEqual((resumed.controls.call_count, resumed.controls.circuit_state), (1, "budget_exhausted"))

    def test_shared_supplier_run_circuit_stops_later_dates(self):
        policy = CompanySyncPolicy(circuit_failure_threshold=1, requests_per_second=1000, backoff_seconds=0, jitter_seconds=0)
        run = start_supplier_run("20260816", self.company_db_path, policy=policy)
        with self.assertRaises(IncompleteCompanyBatch):
            sync_company_change_date("20260814", FakePages({1: RuntimeError("timeout")}), self.company_db_path, policy=policy, run=run)
        later = FakePages({1: page(1, [item("2")], 1)})
        with self.assertRaisesRegex(IncompleteCompanyBatch, "circuit open"):
            sync_company_change_date("20260815", later, self.company_db_path, policy=policy, run=run)
        self.assertEqual(later.calls, [])
        self.assertEqual(
            self.conn.execute(
                "SELECT call_count, circuit_state FROM company_sync_job_log "
                "WHERE job_name='company_changes_run' AND source_date='20260816'"
            ).fetchone(),
            (1, "open"),
        )

    def test_supplier_run_finishes_failed_after_non_circuit_failure_and_later_success(self):
        run = start_supplier_run("20260816", self.company_db_path, policy=self.fast_policy)
        with self.assertRaisesRegex(IncompleteCompanyBatch, "invalid page response"):
            sync_company_change_date(
                "20260814", lambda _page: object(), self.company_db_path, policy=self.fast_policy, run=run
            )
        self.assertEqual(
            self.conn.execute(
                "SELECT response_count FROM company_sync_response_metric "
                "WHERE job_name='company_changes_run' AND source_date='20260816' "
                "AND response_class='terminal_date_failure'"
            ).fetchone(),
            (1,),
        )
        self.assertTrue(start_supplier_run("20260816", self.company_db_path, policy=self.fast_policy).has_terminal_failure)
        sync_company_change_date(
            "20260815", FakePages({1: page(1, [item("2")], 1)}), self.company_db_path,
            policy=self.fast_policy, run=run,
        )
        finish_supplier_run(run)
        self.assertEqual(
            self.conn.execute(
                "SELECT status, circuit_state FROM company_sync_job_log "
                "WHERE job_name='company_changes_run' AND source_date='20260816'"
            ).fetchone(),
            ("failed", "closed"),
        )
        self.assertEqual(pending_supplier_dates(self.conn, "20260815"), ["20260814"])

    def test_ensure_schema_failure_marks_supplier_run_failed(self):
        run = start_supplier_run("20260816", self.company_db_path, policy=self.fast_policy)
        with patch("company_sync.ensure_locality_schema", side_effect=RuntimeError("schema unavailable")):
            with self.assertRaisesRegex(RuntimeError, "schema unavailable"):
                sync_company_change_date(
                    "20260814", FakePages({1: page(1, [item("1")], 1)}), self.company_db_path,
                    policy=self.fast_policy, run=run,
                )
        finish_supplier_run(run)
        self.assertEqual(
            self.conn.execute(
                "SELECT status, circuit_state FROM company_sync_job_log "
                "WHERE job_name='company_changes_run' AND source_date='20260816'"
            ).fetchone(),
            ("failed", "closed"),
        )

    def test_start_sync_job_failure_marks_supplier_run_failed(self):
        run = start_supplier_run("20260816", self.company_db_path, policy=self.fast_policy)
        original_start_sync_job = start_sync_job

        def fail_per_date_job(conn, job_name, source_date, **kwargs):
            if job_name == "company_changes":
                raise RuntimeError("job state unavailable")
            return original_start_sync_job(conn, job_name, source_date, **kwargs)

        with patch("company_sync.start_sync_job", side_effect=fail_per_date_job):
            with self.assertRaisesRegex(RuntimeError, "job state unavailable"):
                sync_company_change_date(
                    "20260814", FakePages({1: page(1, [item("1")], 1)}), self.company_db_path,
                    policy=self.fast_policy, run=run,
                )
        finish_supplier_run(run)
        self.assertEqual(
            self.conn.execute(
                "SELECT status, circuit_state FROM company_sync_job_log "
                "WHERE job_name='company_changes_run' AND source_date='20260816'"
            ).fetchone(),
            ("failed", "closed"),
        )

    def test_sync_one_day_configures_peer_fence_and_keeps_generation_clocks_separate(self):
        before = (read_data_generation(self.conn), read_control_revision(self.conn))
        original_company_path, original_procurement_path = pipeline.COMPANY_DB_PATH, pipeline.DB_PATH

        def supplier_step(_source_date):
            self.assertEqual(require_locality_paths(), self.paths)
            sync_company_change_date(
                "20260816", FakePages({1: page(1, [item("1")], 1)}), self.company_db_path, policy=self.fast_policy
            )
            after = (read_data_generation(self.conn), read_control_revision(self.conn))
            self.assertGreater(after[0], before[0])
            self.assertGreater(after[1], before[1])
            peer = sqlite3.connect(self.procurement_db_path)
            try:
                set_write_fence(peer, False, "test", "peer fence")
            finally:
                peer.close()
            with self.assertRaises(WriteFenceError):
                sync_company_change_date(
                    "20260815", FakePages({1: page(1, [item("2")], 1)}), self.company_db_path, policy=self.fast_policy
                )
            raise StopPipeline()

        try:
            pipeline.COMPANY_DB_PATH = str(self.company_db_path)
            pipeline.DB_PATH = str(self.procurement_db_path)
            with patch.dict(os.environ, {"LOCALITY_COORDINATION_DIR": str(self.paths.pointer_path.parent)}), patch.object(
                pipeline, "check_api_health", return_value=True
            ), patch.object(
                pipeline, "update_agency_master_daily", return_value=None
            ), patch.object(pipeline, "update_company_master_daily", side_effect=supplier_step):
                with self.assertRaises(StopPipeline):
                    pipeline.sync_one_day("20260816")
        finally:
            pipeline.COMPANY_DB_PATH, pipeline.DB_PATH = original_company_path, original_procurement_path

    def test_supplier_failure_does_not_stop_contract_pipeline(self):
        calls = []
        original_argv = pipeline.sys.argv
        supplier_run = SimpleNamespace(controls=SimpleNamespace(circuit_state="closed"))
        try:
            pipeline.sys.argv = ["daily_pipeline_sync.py", "20260816"]
            with patch.object(pipeline, "configure_company_locality_runtime_paths", return_value=self.paths), patch.object(
                pipeline, "supplier_dates_for_run", return_value=["20260816"]
            ), patch.object(pipeline, "start_supplier_run", return_value=supplier_run), patch.object(
                pipeline, "finish_supplier_run", return_value=None
            ), patch.object(pipeline, "update_company_master_daily", side_effect=IncompleteCompanyBatch("timeout")), patch.object(
                pipeline, "sync_one_day", side_effect=lambda source_date, **kwargs: calls.append((source_date, kwargs)) or True
            ), patch.object(pipeline, "record_sync_success", side_effect=StopPipeline):
                with self.assertRaises(StopPipeline):
                    pipeline.main()
        finally:
            pipeline.sys.argv = original_argv
        self.assertEqual(calls, [("20260816", {"sync_supplier": False})])

    def test_supplier_run_start_failure_does_not_stop_contract_pipeline(self):
        calls = []
        original_argv = pipeline.sys.argv
        try:
            pipeline.sys.argv = ["daily_pipeline_sync.py", "20260816"]
            with patch.object(pipeline, "configure_company_locality_runtime_paths", return_value=self.paths), patch.object(
                pipeline, "supplier_dates_for_run", return_value=["20260816"]
            ), patch.object(pipeline, "start_supplier_run", side_effect=IncompleteCompanyBatch("coordinator unavailable")), patch.object(
                pipeline, "sync_one_day", side_effect=lambda source_date, **kwargs: calls.append((source_date, kwargs)) or True
            ), patch.object(pipeline, "record_sync_success", side_effect=StopPipeline):
                with self.assertRaises(StopPipeline):
                    pipeline.main()
        finally:
            pipeline.sys.argv = original_argv
        self.assertEqual(calls, [("20260816", {"sync_supplier": False})])

    def test_reader_setup_failure_marks_run_failed_and_does_not_stop_contract_pipeline(self):
        calls = []
        original_argv = pipeline.sys.argv
        original_company_path = pipeline.COMPANY_DB_PATH
        try:
            pipeline.sys.argv = ["daily_pipeline_sync.py", "20260816"]
            pipeline.COMPANY_DB_PATH = str(self.company_db_path)
            with patch.object(pipeline, "configure_company_locality_runtime_paths", return_value=self.paths), patch.object(
                pipeline, "supplier_dates_for_run", return_value=["20260816"]
            ), patch.object(
                pipeline, "make_verified_company_page_reader", side_effect=RuntimeError("reader setup unavailable")
            ), patch.object(
                pipeline, "sync_one_day", side_effect=lambda source_date, **kwargs: calls.append((source_date, kwargs)) or True
            ), patch.object(pipeline, "record_sync_success", side_effect=StopPipeline):
                with self.assertRaises(StopPipeline):
                    pipeline.main()
        finally:
            pipeline.COMPANY_DB_PATH = original_company_path
            pipeline.sys.argv = original_argv
        self.assertEqual(calls, [("20260816", {"sync_supplier": False})])
        self.assertEqual(
            self.conn.execute(
                "SELECT status FROM company_sync_job_log "
                "WHERE job_name='company_changes_run'"
            ).fetchone(),
            ("failed",),
        )

    def test_invalid_supplier_date_from_caller_fails_run_and_contract_pipeline_continues(self):
        calls = []
        original_argv = pipeline.sys.argv
        original_company_path = pipeline.COMPANY_DB_PATH
        try:
            pipeline.sys.argv = ["daily_pipeline_sync.py", "20260816"]
            pipeline.COMPANY_DB_PATH = str(self.company_db_path)
            with patch.object(pipeline, "configure_company_locality_runtime_paths", return_value=self.paths), patch.object(
                pipeline, "supplier_dates_for_run", return_value=["not-a-date"]
            ), patch.object(
                pipeline, "sync_one_day", side_effect=lambda source_date, **kwargs: calls.append((source_date, kwargs)) or True
            ), patch.object(pipeline, "record_sync_success", side_effect=StopPipeline):
                with self.assertRaises(StopPipeline):
                    pipeline.main()
        finally:
            pipeline.COMPANY_DB_PATH = original_company_path
            pipeline.sys.argv = original_argv
        self.assertEqual(calls, [("20260816", {"sync_supplier": False})])
        self.assertEqual(
            self.conn.execute(
                "SELECT status, circuit_state FROM company_sync_job_log "
                "WHERE job_name='company_changes_run' AND source_date='20260815'"
            ).fetchone(),
            ("failed", "closed"),
        )
        self.assertIsNone(
            self.conn.execute(
                "SELECT status FROM company_sync_job_log "
                "WHERE job_name='company_changes' AND source_date='not-a-date'"
            ).fetchone()
        )

    def test_public_api_recovery_queue_remains_retryable_after_a_failed_attempt(self):
        recovery = sqlite3.connect(":memory:")
        try:
            recovery.execute("CREATE TABLE api_call_issues (target_date TEXT, api_name TEXT, issue_type TEXT)")
            enqueue_prespec_issues(
                recovery,
                [{"target_date": "20260815", "api_name": "prespec_공사", "issue_type": "retry_exhausted"}],
            )
            record_prespec_attempt(recovery, "20260815", success=False, error="timeout")
            self.assertEqual(pending_prespec_dates(recovery), ["20260815"])
        finally:
            recovery.close()


if __name__ == "__main__":
    unittest.main()
