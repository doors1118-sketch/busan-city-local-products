import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path

from company_locality import apply_company_changes, ensure_locality_schema
from maintenance_lock import (
    CheckpointError,
    LocalityPaths,
    WriteFenceError,
    checkpoint_wal,
    guarded_write_session,
    maintenance_lock,
    read_control_revision,
    read_data_generation,
    require_locality_paths,
    set_write_fence,
)
from maintenance_lock import configure_locality_paths


class MaintenanceLockTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "company.db"
        self.procurement_path = Path(self.tempdir.name) / "procurement.db"
        self.lock_path = Path(self.tempdir.name) / "locks" / "maintenance.lock"
        root = Path(self.tempdir.name)
        self.paths = LocalityPaths(
            self.db_path, self.procurement_path, self.lock_path, root / "transition.json", root / "marker", root / "pointer.json"
        )
        configure_locality_paths(self.paths)
        self.paths.pointer_path.write_text('{"active_generation_id":null}', encoding="ascii")
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
        ensure_locality_schema(self.conn, paths=self.paths)
        procurement = sqlite3.connect(self.procurement_path)
        procurement.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
        ensure_locality_schema(procurement, paths=self.paths)
        procurement.close()

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
        marker = self.paths.marker_path
        marker.write_text("paused", encoding="ascii")
        with self.assertRaises(WriteFenceError):
                with guarded_write_session(self.conn, paths=self.paths):
                    self.conn.execute("SELECT 1")

    def test_no_peer_configuration_requires_a_durable_coordinator(self):
        test_paths = LocalityPaths.for_in_memory_tests(
            Path(self.tempdir.name) / "memory.lock",
            Path(self.tempdir.name) / "memory.transition.json",
            Path(self.tempdir.name) / "memory.marker",
            Path(self.tempdir.name) / "memory.pointer.json",
        )
        configure_locality_paths(test_paths)
        memory = sqlite3.connect(":memory:")
        try:
            memory.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
            ensure_locality_schema(memory, paths=test_paths)
            with self.assertRaises(WriteFenceError):
                with guarded_write_session(memory, paths=test_paths):
                    memory.execute("SELECT 1")
        finally:
            memory.close()

    def test_no_peer_paths_must_explicitly_declare_in_memory_test_mode(self):
        with self.assertRaises(ValueError):
            LocalityPaths(
                None,
                None,
                Path(self.tempdir.name) / "memory.lock",
                Path(self.tempdir.name) / "memory.transition.json",
                Path(self.tempdir.name) / "memory.marker",
                Path(self.tempdir.name) / "memory.pointer.json",
            )

    def test_file_backed_writer_rejects_explicit_no_peer_test_paths(self):
        test_paths = LocalityPaths.for_in_memory_tests(
            Path(self.tempdir.name) / "memory.lock",
            Path(self.tempdir.name) / "memory.transition.json",
            Path(self.tempdir.name) / "memory.marker",
            Path(self.tempdir.name) / "memory.pointer.json",
        )
        configure_locality_paths(test_paths)
        test_paths.pointer_path.write_text('{"active_generation_id":null}', encoding="ascii")
        file_conn = sqlite3.connect(Path(self.tempdir.name) / "file-backed.db")
        try:
            file_conn.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
            ensure_locality_schema(file_conn, paths=test_paths)
            with self.assertRaisesRegex(WriteFenceError, "file-backed writers"):
                apply_company_changes(
                    file_conn,
                    [{"bizno": "1234567890", "rgnNm": "부산", "hdoffceDivNm": "본사", "chgDt": "202608160900"}],
                    "20260816",
                    "no-peer-file",
                    "2026-08-16 12:00:00+09:00",
                )
        finally:
            file_conn.close()

    def test_canonical_equivalent_paths_are_accepted_after_configuration(self):
        equivalent = LocalityPaths(
            self.db_path.parent / "." / self.db_path.name,
            self.procurement_path.parent / "." / self.procurement_path.name,
            self.lock_path.parent / "." / self.lock_path.name,
            self.paths.journal_path.parent / "." / self.paths.journal_path.name,
            self.paths.marker_path.parent / "." / self.paths.marker_path.name,
            self.paths.pointer_path.parent / "." / self.paths.pointer_path.name,
        )
        self.assertEqual(require_locality_paths(equivalent), self.paths)

    def test_nonidentical_paths_are_rejected_after_configuration(self):
        alternate = LocalityPaths(
            self.db_path,
            self.procurement_path,
            Path(self.tempdir.name) / "alt" / "maintenance.lock",
            Path(self.tempdir.name) / "alt" / "transition.json",
            Path(self.tempdir.name) / "alt" / "marker",
            Path(self.tempdir.name) / "alt" / "pointer.json",
        )
        with self.assertRaises(WriteFenceError):
            require_locality_paths(alternate)

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

    def test_writer_requires_explicit_writable_initialization(self):
        uninitialized = sqlite3.connect(Path(self.tempdir.name) / "uninitialized.db")
        try:
            uninitialized.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
            with self.assertRaises(RuntimeError):
                apply_company_changes(
                    uninitialized,
                    [{"bizno": "1234567890", "rgnNm": "부산", "hdoffceDivNm": "본사", "chgDt": "202608160900"}],
                    "20260816",
                    "requires-init",
                    "2026-08-16 12:00:00+09:00",
                )
        finally:
            uninitialized.close()

    def test_writer_uses_the_deployed_coordination_paths_not_its_database_parent(self):
        coordination = Path(self.tempdir.name) / "coordination"
        paths = LocalityPaths(
            company_db_path=self.db_path,
            procurement_db_path=Path(self.tempdir.name) / "other-db" / "procurement.db",
            maintenance_path=coordination / "maintenance.lock",
            journal_path=coordination / "transition.json",
            marker_path=coordination / "locality_writes_paused",
            pointer_path=coordination / "active_locality_generation.json",
        )
        paths.marker_path.parent.mkdir(parents=True)
        paths.marker_path.write_text("paused", encoding="ascii")
        with self.assertRaises(WriteFenceError):
            apply_company_changes(
                self.conn,
                [{"bizno": "1234567890", "rgnNm": "부산", "hdoffceDivNm": "본사", "chgDt": "202608160900"}],
                "20260816",
                "coordinator-fenced",
                "2026-08-16 12:00:00+09:00",
                paths=paths,
            )
