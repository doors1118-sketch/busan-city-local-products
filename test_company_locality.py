import sqlite3
import tempfile
import unittest
from pathlib import Path

from company_locality import (
    active_local_biznos,
    apply_company_changes,
    ensure_locality_schema,
    status_at,
)
from maintenance_lock import LocalityPaths
from maintenance_lock import configure_locality_paths


NOW = "2026-08-16 12:00:00+09:00"


def source_item(bizno, region, division, changed_at, **extra):
    item = {
        "bizno": bizno,
        "corpNm": "Example Supplier",
        "rgnNm": region,
        "hdoffceDivNm": division,
        "chgDt": changed_at,
    }
    item.update(extra)
    return item


class CompanyLocalityTransitionTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        root = Path(self.tempdir.name)
        self.paths = LocalityPaths(None, None, root / "maintenance.lock", root / "transition.json", root / "marker", root / "pointer.json")
        configure_locality_paths(self.paths)
        self.paths.pointer_path.write_text('{"active_generation_id":null}', encoding="ascii")
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute(
            """
            CREATE TABLE company_master (
                bizno TEXT PRIMARY KEY,
                corpNm TEXT,
                rgnNm TEXT,
                hdoffceDivNm TEXT,
                chgDt TEXT,
                adrs TEXT
            )
            """
        )
        self.conn.execute(
            "INSERT INTO company_master VALUES (?, ?, ?, ?, ?, ?)",
            ("1234567890", "Bootstrap", "부산광역시", "본사", "", "부산광역시"),
        )
        self.conn.commit()
        ensure_locality_schema(self.conn, paths=self.paths)

    def tearDown(self):
        self.conn.close()
        self.tempdir.cleanup()

    def test_existing_master_rows_bootstrap_as_current_local(self):
        row = self.conn.execute(
            "SELECT status, source_effective_at FROM company_locality_status WHERE bizno = ?",
            ("1234567890",),
        ).fetchone()
        self.assertEqual(row, ("active_local", "1900-01-01 00:00:00+09:00"))
        self.assertEqual(active_local_biznos(self.conn), {"1234567890"})

    def test_outbound_supplier_is_retained_but_inactivated(self):
        apply_company_changes(
            self.conn,
            [source_item("1234567890", "경남", "본사", "202608160900")],
            "20260816",
            "job-1",
            NOW,
        )
        row = self.conn.execute(
            "SELECT status, inactive_reason FROM company_locality_status WHERE bizno = ?",
            ("1234567890",),
        ).fetchone()
        self.assertEqual(row, ("moved_out", "region_changed"))
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM company_master WHERE bizno = ?", ("1234567890",)
            ).fetchone()[0],
            1,
        )

    def test_branch_change_inactivates_existing_supplier(self):
        apply_company_changes(
            self.conn,
            [source_item("1234567890", "부산광역시", "지사", "202608160900")],
            "20260816",
            "job-branch",
            NOW,
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT status, inactive_reason FROM company_locality_status WHERE bizno = ?",
                ("1234567890",),
            ).fetchone(),
            ("branch_changed", "head_office_changed"),
        )

    def test_inbound_supplier_becomes_active_from_source_change_time(self):
        apply_company_changes(
            self.conn,
            [source_item("2222222222", "부산", "본사", "202608161015")],
            "20260816",
            "job-2",
            NOW,
        )
        self.assertEqual(
            status_at(self.conn, "2222222222", "2026-08-16 10:15:01"), "active_local"
        )
        self.assertIsNone(status_at(self.conn, "2222222222", "2026-08-15 23:59:59"))

    def test_reentry_after_outbound_reactivates_supplier(self):
        apply_company_changes(
            self.conn,
            [source_item("1234567890", "경남", "본사", "202608160900")],
            "20260816",
            "job-out",
            NOW,
        )
        apply_company_changes(
            self.conn,
            [source_item("1234567890", "부산", "본사", "202608161100")],
            "20260816",
            "job-in",
            NOW,
        )
        self.assertEqual(status_at(self.conn, "1234567890", "2026-08-16 10:00:00"), "moved_out")
        self.assertEqual(status_at(self.conn, "1234567890", "2026-08-16 11:00:00"), "active_local")

    def test_replaying_source_row_does_not_append_a_second_event(self):
        item = source_item("1234567890", "경남", "본사", "202608160900")
        apply_company_changes(self.conn, [item], "20260816", "job-1", NOW)
        second = apply_company_changes(self.conn, [item], "20260816", "job-2", NOW)
        self.assertEqual(second.duplicates, 1)
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM company_locality_event WHERE bizno = ? AND source_chg_dt = ?",
                ("1234567890", "202608160900"),
            ).fetchone()[0],
            1,
        )

    def test_retrograde_event_is_audited_without_replacing_current_state(self):
        apply_company_changes(
            self.conn,
            [source_item("1234567890", "경남", "본사", "202608161100")],
            "20260816",
            "job-new",
            NOW,
        )
        apply_company_changes(
            self.conn,
            [source_item("1234567890", "부산", "본사", "202608160900")],
            "20260816",
            "job-old",
            NOW,
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT status FROM company_locality_status WHERE bizno = ?", ("1234567890",)
            ).fetchone()[0],
            "moved_out",
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT disposition FROM company_locality_event WHERE job_id = ?", ("job-old",)
            ).fetchone()[0],
            "quarantined_retrograde",
        )

    def test_equal_time_divergent_locality_is_quarantined_and_blocks_lookup(self):
        apply_company_changes(
            self.conn,
            [source_item("1234567890", "경남", "본사", "202608160900")],
            "20260816",
            "job-first",
            NOW,
        )
        apply_company_changes(
            self.conn,
            [source_item("1234567890", "부산", "본사", "202608160900")],
            "20260816",
            "job-conflict",
            NOW,
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT disposition FROM company_locality_event WHERE job_id = ?", ("job-conflict",)
            ).fetchone()[0],
            "quarantined_conflict",
        )
        self.assertIsNone(status_at(self.conn, "1234567890", "2026-08-16 10:00:01"))

    def test_equal_time_descriptive_change_does_not_block_lookup(self):
        apply_company_changes(
            self.conn,
            [source_item("1234567890", "경남", "본사", "202608160900", corpNm="First")],
            "20260816",
            "job-first",
            NOW,
        )
        apply_company_changes(
            self.conn,
            [source_item("1234567890", "경남", "본사", "202608160900", corpNm="Renamed")],
            "20260816",
            "job-descriptive",
            NOW,
        )
        self.assertEqual(status_at(self.conn, "1234567890", "2026-08-16 10:00:01"), "moved_out")
        self.assertEqual(
            self.conn.execute("SELECT corpNm FROM company_master WHERE bizno = ?", ("1234567890",)).fetchone()[0],
            "Renamed",
        )

    def test_same_day_date_only_boundary_is_unknown(self):
        apply_company_changes(
            self.conn,
            [source_item("2222222222", "부산", "본사", "202608161015")],
            "20260816",
            "job-3",
            NOW,
        )
        self.assertIsNone(status_at(self.conn, "2222222222", "2026-08-16"))

    def test_complete_batch_orders_each_supplier_by_effective_time(self):
        apply_company_changes(
            self.conn,
            [
                source_item("3333333333", "경남", "본사", "202608161100"),
                source_item("3333333333", "부산", "본사", "202608160900"),
            ],
            "20260816",
            "unordered-batch",
            NOW,
        )
        self.assertEqual(status_at(self.conn, "3333333333", "2026-08-16 10:00:00"), "active_local")
        self.assertEqual(status_at(self.conn, "3333333333", "2026-08-16 11:00:00"), "moved_out")

    def test_late_historical_equal_time_divergence_is_a_conflict_not_retrograde(self):
        apply_company_changes(
            self.conn,
            [source_item("1234567890", "부산", "본사", "202608160900")],
            "20260816",
            "history",
            NOW,
        )
        apply_company_changes(
            self.conn,
            [source_item("1234567890", "경남", "본사", "202608161100")],
            "20260816",
            "newer",
            NOW,
        )
        apply_company_changes(
            self.conn,
            [source_item("1234567890", "부산", "지사", "202608160900")],
            "20260816",
            "late-history",
            NOW,
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT disposition FROM company_locality_event WHERE job_id = ?", ("late-history",)
            ).fetchone()[0],
            "quarantined_conflict",
        )
        self.assertIsNone(status_at(self.conn, "1234567890", "2026-08-16 09:00:01"))

    def test_status_lookup_is_safe_on_a_read_only_connection(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "company.db"
            writable = sqlite3.connect(path)
            writable.execute("CREATE TABLE company_master (bizno TEXT PRIMARY KEY, chgDt TEXT)")
            writable.execute("INSERT INTO company_master VALUES ('1234567890', '')")
            ensure_locality_schema(writable)
            apply_company_changes(
                writable,
                [source_item("1234567890", "경남", "본사", "202608160900")],
                "20260816",
                "readonly",
                NOW,
            )
            writable.close()
            readonly = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
            try:
                self.assertEqual(status_at(readonly, "1234567890", "2026-08-16 09:00:01"), "moved_out")
                self.assertEqual(active_local_biznos(readonly), set())
            finally:
                readonly.close()
