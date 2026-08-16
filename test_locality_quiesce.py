import sqlite3
import tempfile
import unittest
from pathlib import Path

from locality_quiesce import QuiescenceError, assert_databases_quiesced, dual_exclusive_transition


class LocalityQuiesceTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.company_path = Path(self.tempdir.name) / "company.db"
        self.proc_path = Path(self.tempdir.name) / "procurement.db"
        self.company = sqlite3.connect(self.company_path)
        self.procurement = sqlite3.connect(self.proc_path)
        for conn in (self.company, self.procurement):
            conn.execute("CREATE TABLE state (value TEXT)")
            conn.commit()

    def tearDown(self):
        self.company.close()
        self.procurement.close()
        self.tempdir.cleanup()

    def test_open_handle_aborts_before_any_transition_state_change(self):
        def inspector(path):
            return ["worker-42"] if Path(path) == self.company_path else []

        with self.assertRaises(QuiescenceError):
            assert_databases_quiesced([self.company_path, self.proc_path], inspector)
        self.assertEqual(self.company.execute("SELECT COUNT(*) FROM state").fetchone()[0], 0)
        self.assertEqual(self.procurement.execute("SELECT COUNT(*) FROM state").fetchone()[0], 0)

    def test_inspector_checks_database_wal_and_shm_paths(self):
        checked = []

        def inspector(path):
            checked.append(Path(path).name)
            return []

        assert_databases_quiesced([self.company_path], inspector)
        self.assertEqual(set(checked), {"company.db", "company.db-wal", "company.db-shm"})

    def test_dual_exclusive_transition_commits_both_databases(self):
        with dual_exclusive_transition(self.company, self.procurement):
            self.company.execute("INSERT INTO state VALUES ('company')")
            self.procurement.execute("INSERT INTO state VALUES ('procurement')")
        self.assertEqual(self.company.execute("SELECT value FROM state").fetchone()[0], "company")
        self.assertEqual(self.procurement.execute("SELECT value FROM state").fetchone()[0], "procurement")

    def test_dual_exclusive_transition_rolls_back_both_databases_on_error(self):
        with self.assertRaises(RuntimeError):
            with dual_exclusive_transition(self.company, self.procurement):
                self.company.execute("INSERT INTO state VALUES ('company')")
                raise RuntimeError("abort")
        self.assertEqual(self.company.execute("SELECT COUNT(*) FROM state").fetchone()[0], 0)
        self.assertEqual(self.procurement.execute("SELECT COUNT(*) FROM state").fetchone()[0], 0)
