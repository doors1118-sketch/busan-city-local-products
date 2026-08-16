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
from maintenance_lock import WriteFenceError, guarded_write_session


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
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
        self.conn.execute("INSERT INTO company_master VALUES ('1234567890', '')")
        ensure_locality_schema(self.conn)
        apply_company_changes(self.conn, [item("경남")], "20260816", "first", NOW)
        apply_company_changes(self.conn, [item("부산")], "20260816", "conflict", NOW)
        self.conflict_id = self.conn.execute(
            "SELECT id FROM company_locality_event WHERE disposition = 'quarantined_conflict'"
        ).fetchone()[0]

    def tearDown(self):
        self.conn.close()

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

    def test_two_connections_replay_one_resolution_idempotently(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "company.db"
            setup = sqlite3.connect(path)
            setup.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
            setup.execute("INSERT INTO company_master VALUES ('1234567890', '')")
            ensure_locality_schema(setup)
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


class LocalityWriteTransitionTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.company_path = Path(self.tempdir.name) / "company.db"
        self.procurement_path = Path(self.tempdir.name) / "procurement.db"
        self.lock_path = Path(self.tempdir.name) / "maintenance.lock"
        self.journal_path = Path(self.tempdir.name) / "transition.json"
        self.marker_path = Path(self.tempdir.name) / "locality_writes_paused"
        self.pointer_path = Path(self.tempdir.name) / "active_locality_generation.json"
        self.company = sqlite3.connect(self.company_path)
        self.procurement = sqlite3.connect(self.procurement_path)
        for connection in (self.company, self.procurement):
            connection.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
            ensure_locality_schema(connection)
        self.pointer_path.write_text(json.dumps({"active_generation_id": None}), encoding="ascii")

    def tearDown(self):
        self.company.close()
        self.procurement.close()
        self.tempdir.cleanup()

    def _transition_arguments(self):
        return {
            "maintenance_path": self.lock_path,
            "journal_path": self.journal_path,
            "marker_path": self.marker_path,
            "pointer_path": self.pointer_path,
            "process_inspector": lambda _path: [],
            "operator": "operator",
            "reason": "maintenance",
        }

    def test_pause_resume_uses_both_activation_rows_and_marker(self):
        pause_locality_writes(self.company, self.procurement, **self._transition_arguments())
        self.assertTrue(self.marker_path.exists())
        for connection in (self.company, self.procurement):
            self.assertEqual(
                connection.execute("SELECT writes_enabled FROM locality_activation_state").fetchone()[0], 0
            )
            with self.assertRaises(WriteFenceError):
                with guarded_write_session(connection, marker_path=self.marker_path):
                    connection.execute("SELECT 1")
        resume_locality_writes(self.company, self.procurement, **self._transition_arguments())
        self.assertFalse(self.marker_path.exists())
        for connection in (self.company, self.procurement):
            self.assertEqual(
                connection.execute("SELECT writes_enabled FROM locality_activation_state").fetchone()[0], 1
            )

    def test_failed_pause_keeps_marker_until_explicit_recovery(self):
        with self.assertRaises(RuntimeError):
            pause_locality_writes(
                self.company, self.procurement, fail_at="after_procurement_commit", **self._transition_arguments()
            )
        self.assertTrue(self.marker_path.exists())
        recover_locality_transition(self.company, self.procurement, **self._transition_arguments())
        self.assertTrue(self.marker_path.exists())
        for connection in (self.company, self.procurement):
            self.assertEqual(
                connection.execute("SELECT writes_enabled FROM locality_activation_state").fetchone()[0], 0
            )

    def test_marker_last_crash_remains_recoverable_and_fenced(self):
        pause_locality_writes(self.company, self.procurement, **self._transition_arguments())
        with self.assertRaises(RuntimeError):
            resume_locality_writes(
                self.company, self.procurement, fail_at="before_marker_removal", **self._transition_arguments()
            )
        self.assertTrue(self.marker_path.exists())
        self.assertFalse(self.journal_path.exists())
        recover_locality_transition(self.company, self.procurement, **self._transition_arguments())
        self.assertTrue(self.marker_path.exists())
        self.assertTrue(self.journal_path.exists())
        for connection in (self.company, self.procurement):
            with self.assertRaises(WriteFenceError):
                with guarded_write_session(connection, marker_path=self.marker_path):
                    connection.execute("SELECT 1")
