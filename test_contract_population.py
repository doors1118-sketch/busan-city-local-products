import sqlite3
import unittest

from contract_population import (
    CanonicalContractCollision,
    MissingContractIdentity,
    canonical_contract_from_row,
    content_fingerprint,
    iter_canonical_contracts,
)


def corp_entry(bizno, share):
    return f"[a^b^c^Supplier^e^f^{share}^h^i^{bizno}]"


def create_sector_tables(conn):
    for table in ("cnstwk_cntrct", "servc_cntrct", "thng_cntrct"):
        conn.execute(
            f"""
            CREATE TABLE {table} (
                untyCntrctNo TEXT,
                dcsnCntrctNo TEXT,
                dminsttCd TEXT,
                cntrctInsttCd TEXT,
                thtmCntrctAmt,
                totCntrctAmt,
                corpList TEXT,
                cntrctCnclsDate TEXT,
                cntrctDate TEXT,
                rgstDt TEXT,
                updDt TEXT
            )
            """
        )
    conn.execute(
        """
        CREATE TABLE shopping_cntrct (
            dlvrReqNo TEXT,
            prdctSno TEXT,
            dlvrReqChgOrd,
            dminsttCd TEXT,
            prdctAmt,
            cntrctCorpBizno TEXT,
            dlvrReqRcptDate TEXT,
            rgstDt TEXT,
            updDt TEXT
        )
        """
    )


class CanonicalContractPopulationTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        create_sector_tables(self.conn)

    def tearDown(self):
        self.conn.close()

    def insert_non_shopping(
        self,
        table="cnstwk_cntrct",
        *,
        unty="U-1",
        dcsn="DEC-000100",
        agency="A-1",
        amount=100,
        corps=None,
        contract_date="2026-08-01",
        registered="2026-08-01 09:00:00",
        updated="2026-08-01 10:00:00",
    ):
        self.conn.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, '', ?, 0, ?, ?, '', ?, ?)",
            (
                unty,
                dcsn,
                agency,
                amount,
                corps or corp_entry("123-45-67890", "100"),
                contract_date,
                registered,
                updated,
            ),
        )

    def test_non_shopping_uses_sector_scoped_decision_family_and_latest_revision(self):
        self.insert_non_shopping(dcsn="DEC-000100", amount=100)
        self.insert_non_shopping(dcsn="DEC-000101", amount=120, updated="2026-08-02 10:00:00")
        self.insert_non_shopping(dcsn="OTHER-900100", amount=300, updated="2026-08-03 10:00:00")
        self.insert_non_shopping(table="servc_cntrct", dcsn="DEC-000100", amount=400)
        self.insert_non_shopping(table="thng_cntrct", dcsn="DEC-000100", amount=500)

        construction = list(iter_canonical_contracts(self.conn, "공사"))
        service = list(iter_canonical_contracts(self.conn, "용역"))
        goods = list(iter_canonical_contracts(self.conn, "물품"))

        self.assertEqual(
            [(row.sector, row.contract_key, row.contract_revision, row.amount_won) for row in construction],
            [("공사", "DEC0001", "01", 120), ("공사", "OTHER9001", "00", 300)],
        )
        self.assertEqual(
            [(row.sector, row.contract_key, row.contract_revision, row.amount_won) for row in service],
            [("용역", "DEC0001", "00", 400)],
        )
        self.assertEqual(
            [(row.sector, row.contract_key, row.contract_revision, row.amount_won) for row in goods],
            [("물품", "DEC0001", "00", 500)],
        )

    def test_reused_unified_number_does_not_merge_distinct_decision_families(self):
        self.insert_non_shopping(unty="REUSED", dcsn="FIRST-100100", amount=100)
        self.insert_non_shopping(unty="REUSED", dcsn="SECOND-200100", amount=200)

        rows = list(iter_canonical_contracts(self.conn, "공사"))

        self.assertEqual(len(rows), 2)
        self.assertEqual({row.amount_won for row in rows}, {100, 200})

    def test_duplicate_supplier_entries_are_aggregated_after_bizno_normalization(self):
        corps = corp_entry("123-45-67890", "25") + corp_entry("1234567890", "35") + corp_entry("9999999999", "40")
        self.insert_non_shopping(corps=corps)

        row = next(iter_canonical_contracts(self.conn, "공사"))

        self.assertEqual([(supplier.bizno, supplier.share_pct) for supplier in row.suppliers], [
            ("1234567890", 60.0),
            ("9999999999", 40.0),
        ])

    def test_same_identity_with_divergent_content_is_rejected_regardless_of_source_order(self):
        self.insert_non_shopping(amount=100, updated="2026-08-02 10:00:00")
        self.insert_non_shopping(amount=101, updated="2026-08-03 10:00:00")

        with self.assertRaisesRegex(CanonicalContractCollision, "DEC0001.*00"):
            list(iter_canonical_contracts(self.conn, "공사"))

    def test_exact_duplicate_rows_are_order_independent(self):
        self.insert_non_shopping(updated="2026-08-03 10:00:00")
        self.insert_non_shopping(updated="2026-08-02 10:00:00")

        first = list(iter_canonical_contracts(self.conn, "공사"))
        self.conn.execute("DELETE FROM cnstwk_cntrct")
        self.insert_non_shopping(updated="2026-08-02 10:00:00")
        self.insert_non_shopping(updated="2026-08-03 10:00:00")
        second = list(iter_canonical_contracts(self.conn, "공사"))

        self.assertEqual(first, second)

    def test_missing_non_shopping_identity_is_a_hard_failure(self):
        self.insert_non_shopping(unty="", dcsn="")

        with self.assertRaisesRegex(MissingContractIdentity, "공사"):
            list(iter_canonical_contracts(self.conn, "공사"))

    def test_present_decision_number_with_no_family_does_not_fall_back_to_unified_number(self):
        self.insert_non_shopping(unty="VALID-UNTY", dcsn="00")

        with self.assertRaisesRegex(MissingContractIdentity, "decision"):
            list(iter_canonical_contracts(self.conn, "공사"))

    def test_null_primary_agency_and_date_fields_use_documented_fallbacks(self):
        row = canonical_contract_from_row(
            {
                "untyCntrctNo": "U-1",
                "dcsnCntrctNo": "DEC-000100",
                "dminsttCd": float("nan"),
                "cntrctInsttCd": "A-2",
                "thtmCntrctAmt": 100,
                "corpList": corp_entry("1234567890", "100"),
                "cntrctCnclsDate": float("nan"),
                "cntrctDate": "2026-08-01",
            },
            "공사",
        )

        self.assertEqual(row.agency, "A2")
        self.assertEqual(row.contract_date, "2026-08-01")

    def test_shopping_change_order_numeric_and_string_forms_share_one_revision(self):
        rows = [
            ("DLVR-1", "0007", "01", "A1", "100", "123-45-67890", "2026-08-01", "2026-08-01", "2026-08-01"),
            ("DLVR-1", "0007", 2.0, "A1", 120, "1234567890", "20260802", "2026-08-02", "2026-08-02"),
            ("DLVR-1", "0007", "2", "A1", "120.0", "123-45-67890", "2026-08-02", "2026-08-02", "2026-08-03"),
        ]
        self.conn.executemany("INSERT INTO shopping_cntrct VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

        contracts = list(iter_canonical_contracts(self.conn, "쇼핑몰"))

        self.assertEqual(len(contracts), 1)
        self.assertEqual((contracts[0].contract_key, contracts[0].contract_revision), ("DLVR1:0007", "2"))
        self.assertEqual(contracts[0].amount_won, 120)
        self.assertEqual(contracts[0].suppliers[0].bizno, "1234567890")

    def test_content_fingerprint_normalizes_agency_amount_date_and_supplier_order(self):
        corps_a = corp_entry("9999999999", "40") + corp_entry("123-45-67890", "60")
        corps_b = corp_entry("1234567890", "60.0") + corp_entry("999-99-99999", "40.0")
        self.insert_non_shopping(agency=" A-1 ", amount="100.0", corps=corps_a, contract_date="20260801")
        first = next(iter_canonical_contracts(self.conn, "공사"))
        self.conn.execute("DELETE FROM cnstwk_cntrct")
        self.insert_non_shopping(agency="A-1", amount=100, corps=corps_b, contract_date="2026-08-01")
        second = next(iter_canonical_contracts(self.conn, "공사"))

        self.assertEqual(content_fingerprint(first), content_fingerprint(second))

    def test_date_range_uses_each_sectors_governing_date(self):
        self.insert_non_shopping(contract_date="2026-07-31", dcsn="OLD-000100")
        self.insert_non_shopping(contract_date="2026-08-01", dcsn="NEW-000100")
        self.conn.execute(
            "INSERT INTO shopping_cntrct VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("DLVR-1", "1", 0, "A1", 50, "1234567890", "2026-08-01", "", ""),
        )

        construction = list(iter_canonical_contracts(self.conn, "공사", ("2026-08-01", "2026-08-31")))
        shopping = list(iter_canonical_contracts(self.conn, "쇼핑몰", ("2026-08-01", "2026-08-31")))

        self.assertEqual([row.contract_key for row in construction], ["NEW0001"])
        self.assertEqual([row.contract_key for row in shopping], ["DLVR1:1"])


if __name__ == "__main__":
    unittest.main()
