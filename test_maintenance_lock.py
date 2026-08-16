import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path

from company_locality import apply_company_changes, ensure_locality_schema
from maintenance_lock import (
    CheckpointError,
    WriteFenceError,
    checkpoint_wal,
    guarded_write_session,
    maintenance_lock,
    read_control_revision,
    read_data_generation,
    set_write_fence,
)


class MaintenanceLockTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "company.db"
        self.lock_path = Path(self.tempdir.name) / "locks" / "maintenance.lock"
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
        ensure_locality_schema(self.conn)

    def tearDown(self):
        self.conn.close()
        self.tempdir.cleanup()

    def test_lock_serializes_threads_using_the_same_absolute_path(self):
        entered = threading.Event()
        released = threading.Event()
        acquired = []

        def contender():
            with maintenance_lock(self.lock_path, 1):
                acquired.append("second")
                released.set()

        with maintenance_lock(self.lock_path, 1) as first:
            self.assertEqual(first, self.lock_path.resolve())
            thread = threading.Thread(target=contender)
            thread.start()
            time.sleep(0.05)
            self.assertEqual(acquired, [])
            entered.set()
        self.assertTrue(released.wait(1))
        thread.join()
        self.assertEqual(acquired, ["second"])

    def test_relative_and_absolute_paths_agree_on_one_lock(self):
        with maintenance_lock(self.lock_path, 1) as absolute_path:
            pass
        with maintenance_lock(self.lock_path.parent / "maintenance.lock", 1) as same_path:
            pass
        self.assertEqual(absolute_path, same_path)

    def test_lock_times_out_while_another_thread_holds_it(self):
        failures = []

        def contender():
            try:
                with maintenance_lock(self.lock_path, 0.05):
                    pass
            except TimeoutError:
                failures.append("timed-out")

        with maintenance_lock(self.lock_path, 1):
            thread = threading.Thread(target=contender)
            thread.start()
            thread.join()
        self.assertEqual(failures, ["timed-out"])

    def test_cache_input_and_control_writes_advance_separate_generation_clocks(self):
        before = (read_data_generation(self.conn), read_control_revision(self.conn))
        with guarded_write_session(self.conn):
            self.conn.execute(
                "INSERT INTO company_locality_status "
                "(bizno, status, source_effective_at, observed_at, last_verified_at) "
                "VALUES (?, 'active_local', ?, ?, ?)",
                ("1234567890", "2026-08-16 09:00:00+09:00", "now", "now"),
            )
        after_input = (read_data_generation(self.conn), read_control_revision(self.conn))
        set_write_fence(self.conn, False, "operator", "maintenance")
        after_control = (read_data_generation(self.conn), read_control_revision(self.conn))
        self.assertEqual(after_input, (before[0] + 1, before[1]))
        self.assertEqual(after_control, (after_input[0], after_input[1] + 2))

    def test_schema_configures_wal_and_a_bounded_busy_timeout(self):
        self.assertEqual(self.conn.execute("PRAGMA journal_mode").fetchone()[0], "wal")
        self.assertEqual(self.conn.execute("PRAGMA busy_timeout").fetchone()[0], 5000)

    def test_failed_checkpoint_raises_instead_of_claiming_success(self):
        class FailingConnection:
            def execute(self, _sql):
                return [(1, 3, 2)]

        with self.assertRaises(CheckpointError):
            checkpoint_wal(FailingConnection())

    def test_persisted_write_fence_blocks_a_new_connection(self):
        set_write_fence(self.conn, False, "operator", "maintenance")
        self.conn.close()
        self.conn = sqlite3.connect(self.db_path)
        with self.assertRaises(WriteFenceError):
            with guarded_write_session(self.conn):
                self.conn.execute("SELECT 1")

    def test_guarded_session_rejects_paused_marker_before_writing(self):
        marker = Path(self.tempdir.name) / "locality_writes_paused"
        marker.write_text("paused", encoding="ascii")
        with self.assertRaises(WriteFenceError):
            with guarded_write_session(self.conn, marker_path=marker):
                self.conn.execute("SELECT 1")

    def test_protected_table_rejects_direct_writes_even_before_fencing(self):
        with self.assertRaises(sqlite3.DatabaseError):
            self.conn.execute(
                "INSERT INTO company_locality_status "
                "(bizno, status, source_effective_at, observed_at, last_verified_at) "
                "VALUES ('1234567890', 'active_local', '2026-08-16 09:00:00+09:00', 'now', 'now')"
            )

    def test_fenced_supplier_apply_fails_closed_through_its_mandatory_guard(self):
        set_write_fence(self.conn, False, "operator", "maintenance")
        with self.assertRaises(WriteFenceError):
            apply_company_changes(
                self.conn,
                [{"bizno": "1234567890", "rgnNm": "부산", "hdoffceDivNm": "본사", "chgDt": "202608160900"}],
                "20260816",
                "blocked",
                "2026-08-16 12:00:00+09:00",
            )
