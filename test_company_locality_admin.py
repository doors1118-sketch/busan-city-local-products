import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from company_locality import active_local_biznos, apply_company_changes, ensure_locality_schema, status_at
from company_locality_admin import (
    pause_locality_writes,
    recover_locality_transition,
    resolve_company_conflict,
    resume_locality_writes,
)
from maintenance_lock import LocalityPaths, WriteFenceError, configure_locality_paths, guarded_write_session


NOW = "2026-08-16 12:00:00+09:00"


def item(region):
    return {
        "bizno": "1234567890",
        "corpNm": "Supplier",
        "rgnNm": region,
        "hdoffceDivNm": "본사",
        "chgDt": "202608160900",
    }


class CompanyLocalityAdminTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        root = Path(self.tempdir.name)
        self.paths = LocalityPaths.for_in_memory_tests(
            root / "maintenance.lock", root / "transition.json", root / "marker", root / "pointer.json"
        )
        configure_locality_paths(self.paths)
        self.paths.pointer_path.write_text('{"active_generation_id":null}', encoding="ascii")
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
        self.conn.execute("INSERT INTO company_master VALUES ('1234567890', '')")
        ensure_locality_schema(self.conn, paths=self.paths)
        apply_company_changes(self.conn, [item("경남")], "20260816", "first", NOW)
        apply_company_changes(self.conn, [item("부산")], "20260816", "conflict", NOW)
        self.conflict_id = self.conn.execute(
            "SELECT id FROM company_locality_event WHERE disposition = 'quarantined_conflict'"
        ).fetchone()[0]

    def tearDown(self):
        self.conn.close()
        self.tempdir.cleanup()

    def test_resolution_is_audited_and_unblocks_selected_status(self):
        resolution = resolve_company_conflict(
            self.conn,
            [self.conflict_id],
            "active_local",
            "2026-08-16 09:00:00+09:00",
            "operator@example.test",
            "reviewed source evidence",
            {"ticket": "LOC-1"},
        )
        self.assertEqual(resolution.selected_status, "active_local")
        self.assertEqual(status_at(self.conn, "1234567890", "2026-08-16 09:00:01"), "active_local")
        self.assertEqual(
            self.conn.execute(
                "SELECT operator, reason FROM company_locality_resolution WHERE id = ?", (resolution.id,)
            ).fetchone(),
            ("operator@example.test", "reviewed source evidence"),
        )
        self.assertIn("1234567890", active_local_biznos(self.conn))

    def test_resolution_does_not_overwrite_a_later_confirmed_status(self):
        apply_company_changes(self.conn, [{**item("경남"), "chgDt": "202608161100"}], "20260816", "later", NOW)
        resolve_company_conflict(
            self.conn,
            [self.conflict_id],
            "active_local",
            "2026-08-16 09:00:00+09:00",
            "operator@example.test",
            "reviewed source evidence",
            {"ticket": "LOC-2"},
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT status, source_effective_at FROM company_locality_status WHERE bizno = ?", ("1234567890",)
            ).fetchone(),
            ("moved_out", "2026-08-16 11:00:00+09:00"),
        )

    def test_replaying_the_same_resolution_is_deterministic(self):
        first = resolve_company_conflict(
            self.conn, [self.conflict_id], "active_local", "2026-08-16 09:00:00+09:00", "op", "reason", {"id": 1}
        )
        second = resolve_company_conflict(
            self.conn, [self.conflict_id], "active_local", "2026-08-16 09:00:00+09:00", "op", "reason", {"id": 1}
        )
        self.assertEqual(second.id, first.id)
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) FROM company_locality_resolution").fetchone()[0], 1
        )

    def test_conflicting_second_resolution_for_one_event_is_rejected(self):
        resolve_company_conflict(
            self.conn, [self.conflict_id], "active_local", "2026-08-16 09:00:00+09:00", "op", "reason", {"id": 1}
        )
        with self.assertRaises(ValueError):
            resolve_company_conflict(
                self.conn, [self.conflict_id], "moved_out", "2026-08-16 09:00:00+09:00", "op2", "other", {"id": 2}
            )

    def test_two_connections_replay_one_resolution_idempotently(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "company.db"
            procurement_path = Path(directory) / "procurement.db"
            paths = LocalityPaths(
                path,
                procurement_path,
                Path(directory) / "coordination" / "maintenance.lock",
                Path(directory) / "coordination" / "transition.json",
                Path(directory) / "coordination" / "marker",
                Path(directory) / "coordination" / "pointer.json",
            )
            configure_locality_paths(paths)
            paths.pointer_path.parent.mkdir(parents=True)
            paths.pointer_path.write_text('{"active_generation_id":null}', encoding="ascii")
            setup = sqlite3.connect(path)
            setup.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
            setup.execute("INSERT INTO company_master VALUES ('1234567890', '')")
            ensure_locality_schema(setup, paths=paths)
            procurement = sqlite3.connect(procurement_path)
            procurement.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
            ensure_locality_schema(procurement, paths=paths)
            procurement.close()
            apply_company_changes(setup, [item("경남")], "20260816", "first", NOW)
            apply_company_changes(setup, [item("부산")], "20260816", "conflict", NOW)
            conflict_id = setup.execute(
                "SELECT id FROM company_locality_event WHERE disposition = 'quarantined_conflict'"
            ).fetchone()[0]
            setup.close()
            barrier = threading.Barrier(2)
            resolution_ids = []

            def replay():
                conn = sqlite3.connect(path)
                try:
                    barrier.wait()
                    resolution_ids.append(
                        resolve_company_conflict(
                            conn, [conflict_id], "active_local", "2026-08-16 09:00:00+09:00", "op", "reason", {"id": 1}
                        ).id
                    )
                finally:
                    conn.close()

            first = threading.Thread(target=replay)
            second = threading.Thread(target=replay)
            first.start()
            second.start()
            first.join()
            second.join()
            check = sqlite3.connect(path)
            self.assertEqual(resolution_ids[0], resolution_ids[1])
            self.assertEqual(check.execute("SELECT COUNT(*) FROM company_locality_resolution").fetchone()[0], 1)
            check.close()

    def test_concurrent_different_resolutions_for_one_event_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "company.db"
            procurement_path = Path(directory) / "procurement.db"
            paths = LocalityPaths(
                path,
                procurement_path,
                Path(directory) / "coordination" / "maintenance.lock",
                Path(directory) / "coordination" / "transition.json",
                Path(directory) / "coordination" / "marker",
                Path(directory) / "coordination" / "pointer.json",
            )
            configure_locality_paths(paths)
            paths.pointer_path.parent.mkdir(parents=True)
            paths.pointer_path.write_text('{"active_generation_id":null}', encoding="ascii")
            setup = sqlite3.connect(path)
            setup.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
            setup.execute("INSERT INTO company_master VALUES ('1234567890', '')")
            ensure_locality_schema(setup, paths=paths)
            procurement = sqlite3.connect(procurement_path)
            procurement.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
            ensure_locality_schema(procurement, paths=paths)
            procurement.close()
            apply_company_changes(setup, [item("경남")], "20260816", "first", NOW)
            apply_company_changes(setup, [item("부산")], "20260816", "conflict", NOW)
            conflict_id = setup.execute(
                "SELECT id FROM company_locality_event WHERE disposition = 'quarantined_conflict'"
            ).fetchone()[0]
            setup.close()
            barrier = threading.Barrier(2)
            outcomes = []

            def resolve(selected_status):
                conn = sqlite3.connect(path)
                try:
                    barrier.wait()
                    resolve_company_conflict(
                        conn, [conflict_id], selected_status, "2026-08-16 09:00:00+09:00",
                        "op", "reason", {"selected_status": selected_status},
                    )
                    outcomes.append("resolved")
                except ValueError:
                    outcomes.append("rejected")
                finally:
                    conn.close()

            first = threading.Thread(target=resolve, args=("active_local",))
            second = threading.Thread(target=resolve, args=("moved_out",))
            first.start()
            second.start()
            first.join()
            second.join()
            check = sqlite3.connect(path)
            self.assertCountEqual(outcomes, ["resolved", "rejected"])
            self.assertEqual(check.execute("SELECT COUNT(*) FROM company_locality_resolution").fetchone()[0], 1)
            self.assertEqual(check.execute("SELECT COUNT(*) FROM company_locality_resolution_event").fetchone()[0], 1)
            check.close()

    def test_schema_backfills_existing_resolution_event_bindings(self):
        resolve_company_conflict(
            self.conn, [self.conflict_id], "active_local", "2026-08-16 09:00:00+09:00", "op", "reason", {"id": 1}
        )
        from maintenance_lock import maintenance_write_permission

        with maintenance_write_permission(self.conn):
            self.conn.execute("DELETE FROM company_locality_resolution_event")
        ensure_locality_schema(self.conn)
        self.assertEqual(
            self.conn.execute(
                "SELECT resolution_id FROM company_locality_resolution_event WHERE event_id = ?", (self.conflict_id,)
            ).fetchone()[0],
            1,
        )
        with self.assertRaises(ValueError):
            resolve_company_conflict(
                self.conn, [self.conflict_id], "moved_out", "2026-08-16 09:00:00+09:00", "op2", "other", {"id": 2}
            )


class LocalityWriteTransitionTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.company_path = Path(self.tempdir.name) / "company.db"
        self.procurement_path = Path(self.tempdir.name) / "procurement.db"
        self.lock_path = Path(self.tempdir.name) / "maintenance.lock"
        self.journal_path = Path(self.tempdir.name) / "transition.json"
        self.marker_path = Path(self.tempdir.name) / "locality_writes_paused"
        self.pointer_path = Path(self.tempdir.name) / "active_locality_generation.json"
        self.paths = LocalityPaths(
            self.company_path, self.procurement_path, self.lock_path, self.journal_path, self.marker_path, self.pointer_path
        )
        configure_locality_paths(self.paths)
        for database_path in (self.company_path, self.procurement_path):
            connection = sqlite3.connect(database_path)
            connection.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
            ensure_locality_schema(connection, paths=self.paths)
            connection.close()
        self.pointer_path.write_text(json.dumps({"active_generation_id": None}), encoding="ascii")

    def tearDown(self):
        self.tempdir.cleanup()

    def _transition_arguments(self):
        return {
            "process_inspector": lambda _path: [],
            "operator": "operator",
            "reason": "maintenance",
        }

    def _factory(self, path):
        return sqlite3.connect(path)

    def _activation_rows(self):
        rows = []
        for path in (self.company_path, self.procurement_path):
            connection = sqlite3.connect(path)
            rows.append(connection.execute("SELECT writes_enabled FROM locality_activation_state").fetchone()[0])
            connection.close()
        return rows

    def _call_arguments(self):
        return {
            "paths": self.paths,
            "connection_factory": self._factory,
            "process_inspector": lambda _path: [],
            "operator": "operator",
            "reason": "maintenance",
        }

    def test_pause_resume_uses_both_activation_rows_and_marker(self):
        pause_locality_writes(**self._call_arguments())
        self.assertTrue(self.marker_path.exists())
        self.assertEqual(self._activation_rows(), [0, 0])
        for path in (self.company_path, self.procurement_path):
            connection = sqlite3.connect(path)
            with self.assertRaises(WriteFenceError):
                with guarded_write_session(connection, paths=self.paths):
                    connection.execute("SELECT 1")
            connection.close()
        resume_locality_writes(**self._call_arguments())
        self.assertFalse(self.marker_path.exists())
        self.assertEqual(self._activation_rows(), [1, 1])

    def test_failed_pause_keeps_marker_until_explicit_recovery(self):
        with self.assertRaises(RuntimeError):
            pause_locality_writes(
                fail_at="after_procurement_commit", **self._call_arguments()
            )
        self.assertTrue(self.marker_path.exists())
        alternate = LocalityPaths.for_in_memory_tests(
            Path(self.tempdir.name) / "alt" / "maintenance.lock",
            Path(self.tempdir.name) / "alt" / "transition.json",
            Path(self.tempdir.name) / "alt" / "marker",
            Path(self.tempdir.name) / "alt" / "pointer.json",
        )
        company_conn = sqlite3.connect(self.company_path)
        try:
            with self.assertRaises(WriteFenceError):
                apply_company_changes(
                    company_conn, [item("경남")], "20260816", "partial-transition-alternate-paths", NOW, paths=alternate
                )
        finally:
            company_conn.close()
        recover_locality_transition(**self._call_arguments())
        self.assertTrue(self.marker_path.exists())
        self.assertEqual(self._activation_rows(), [0, 0])

    def test_marker_last_crash_remains_recoverable_and_fenced(self):
        pause_locality_writes(**self._call_arguments())
        with self.assertRaises(RuntimeError):
            resume_locality_writes(
                fail_at="before_marker_removal", **self._call_arguments()
            )
        self.assertTrue(self.marker_path.exists())
        self.assertFalse(self.journal_path.exists())
        recover_locality_transition(**self._call_arguments())
        self.assertTrue(self.marker_path.exists())
        self.assertTrue(self.journal_path.exists())
        for path in (self.company_path, self.procurement_path):
            connection = sqlite3.connect(path)
            with self.assertRaises(WriteFenceError):
                with guarded_write_session(connection, paths=self.paths):
                    connection.execute("SELECT 1")
            connection.close()

    def test_transition_quiesces_before_factory_opens_database_handles(self):
        paths = LocalityPaths(
            company_db_path=self.company_path,
            procurement_db_path=self.procurement_path,
            maintenance_path=self.lock_path,
            journal_path=self.journal_path,
            marker_path=self.marker_path,
            pointer_path=self.pointer_path,
        )
        opened = []

        def factory(path):
            opened.append(Path(path))
            return sqlite3.connect(path)

        def inspector(path):
            return ["coordinator"] if opened else []

        pause_locality_writes(
            paths, factory,
            process_inspector=inspector,
            operator="operator",
            reason="maintenance",
        )
        self.assertEqual(opened, [self.company_path, self.procurement_path])
