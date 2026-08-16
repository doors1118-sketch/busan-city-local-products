import sqlite3
import unittest

from company_locality import apply_company_changes, ensure_locality_schema, status_at
from company_locality_admin import resolve_company_conflict


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
