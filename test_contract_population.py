import sqlite3
import unittest

from contract_population import (
    CanonicalContractError,
    CanonicalContractCollision,
    MissingContractIdentity,
    MissingGoverningDate,
    MissingSupplierIdentity,
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

    def test_null_primary_agency_uses_contract_agency_fallback(self):
        row = canonical_contract_from_row(
            {
                "untyCntrctNo": "U-1",
                "dcsnCntrctNo": "DEC-000100",
                "dminsttCd": float("nan"),
                "cntrctInsttCd": "A-2",
                "thtmCntrctAmt": 100,
                "corpList": corp_entry("1234567890", "100"),
                "cntrctCnclsDate": "2026-08-01",
            },
            "공사",
        )

        self.assertEqual(row.agency, "A2")
        self.assertEqual(row.contract_date, "2026-08-01")

    def test_nonshopping_missing_conclusion_date_does_not_substitute_contract_event_date(self):
        with self.assertRaisesRegex(MissingGoverningDate, "cntrctCnclsDate|governing"):
            canonical_contract_from_row(
                {
                    "untyCntrctNo": "U-1",
                    "dcsnCntrctNo": "DEC-000100",
                    "dminsttCd": "A1",
                    "thtmCntrctAmt": 100,
                    "corpList": corp_entry("1234567890", "100"),
                    "cntrctCnclsDate": float("nan"),
                    "cntrctDate": "2026-08-01",
                },
                "공사",
            )

    def test_shopping_missing_receipt_date_is_a_hard_failure(self):
        with self.assertRaisesRegex(MissingGoverningDate, "dlvrReqRcptDate|governing"):
            canonical_contract_from_row(
                {
                    "dlvrReqNo": "SHOP-1",
                    "prdctSno": "1",
                    "dlvrReqChgOrd": 0,
                    "dminsttCd": "A1",
                    "prdctAmt": 100,
                    "cntrctCorpBizno": "1234567890",
                    "dlvrReqRcptDate": "",
                },
                "쇼핑몰",
            )

    def test_demand_agency_list_precedes_contracting_agency_when_primary_is_missing(self):
        row = canonical_contract_from_row(
            {
                "untyCntrctNo": "U-1",
                "dcsnCntrctNo": "DEC-000100",
                "dminsttCd": "",
                "dminsttList": "[Demand Agency^D-200]",
                "cntrctInsttCd": "C-999",
                "thtmCntrctAmt": 100,
                "corpList": corp_entry("1234567890", "100"),
                "cntrctCnclsDate": "2026-08-01",
            },
            "용역",
        )

        self.assertEqual(row.agency, "D200")

    def test_representative_central_and_self_procured_rows_cover_all_four_sectors(self):
        fixtures = (
            (
                "공사",
                {
                    "untyCntrctNo": "C-UNTY",
                    "dcsnCntrctNo": "C-DEC-000100",
                    "dminsttCd": "CENTRAL-1",
                    "dminsttList": "[Central Demand^CENTRAL-1]",
                    "cntrctInsttCd": "PPS",
                    "thtmCntrctAmt": "1000",
                    "totCntrctAmt": "1000",
                    "corpList": corp_entry("123-45-67890", "100"),
                    "cntrctCnclsDate": "20260815",
                    "rgstDt": "20260815090000",
                },
                ("공사", "CDEC0001", "00", "CENTRAL1", "2026-08-15"),
            ),
            (
                "용역",
                {
                    "untyCntrctNo": "S-UNTY",
                    "dcsnCntrctNo": "S-DEC-000100",
                    "dminsttCd": "",
                    "dminsttList": "[Self Demand^SELF-2]",
                    "cntrctInsttCd": "SELF-CONTRACT",
                    "thtmCntrctAmt": 2000,
                    "corpList": corp_entry("234-56-78901", "100"),
                    "cntrctCnclsDate": "2026-08-15",
                    "updDt": "2026-08-15 09:00:00",
                },
                ("용역", "SDEC0001", "00", "SELF2", "2026-08-15"),
            ),
            (
                "물품",
                {
                    "untyCntrctNo": "G-UNTY",
                    "dcsnCntrctNo": "G-DEC-000100",
                    "dminsttCd": "GOODS-3",
                    "cntrctInsttCd": "PPS",
                    "thtmCntrctAmt": 3000,
                    "corpList": corp_entry("345-67-89012", "100"),
                    "cntrctCnclsDate": "2026-08-15T00:00:00+09:00",
                },
                ("물품", "GDEC0001", "00", "GOODS3", "2026-08-15T00:00:00+09:00"),
            ),
            (
                "쇼핑몰",
                {
                    "dlvrReqNo": "SHOP-400",
                    "prdctSno": "0001",
                    "dlvrReqChgOrd": "2.0",
                    "dminsttCd": "SHOP-4",
                    "prdctAmt": "4000.0",
                    "cntrctCorpBizno": "456-78-90123",
                    "dlvrReqRcptDate": "2026-08-15T00:00:00+09:00",
                },
                ("쇼핑몰", "SHOP400:0001", "2", "SHOP4", "2026-08-15T00:00:00+09:00"),
            ),
        )

        for sector, source, expected in fixtures:
            with self.subTest(sector=sector):
                row = canonical_contract_from_row(source, sector)
                self.assertEqual(
                    (row.sector, row.contract_key, row.contract_revision, row.agency, row.contract_date),
                    expected,
                )

    def test_mixed_valid_and_malformed_supplier_chunks_fail_the_whole_row(self):
        malformed = corp_entry("1234567890", "50") + "[too^short]"

        with self.assertRaisesRegex(MissingSupplierIdentity, "malformed"):
            self.insert_non_shopping(corps=malformed)
            list(iter_canonical_contracts(self.conn, "공사"))

    def test_supplier_business_number_must_normalize_to_exactly_ten_digits(self):
        with self.assertRaisesRegex(MissingSupplierIdentity, "business number"):
            self.insert_non_shopping(corps=corp_entry("12-34", "100"))
            list(iter_canonical_contracts(self.conn, "공사"))

    def test_missing_supplier_share_fails_the_whole_row(self):
        with self.assertRaisesRegex(CanonicalContractError, "share"):
            self.insert_non_shopping(corps=corp_entry("1234567890", ""))
            list(iter_canonical_contracts(self.conn, "공사"))

    def test_equivalent_kst_timestamp_encodings_and_rounding_boundaries_match(self):
        base = {
            "untyCntrctNo": "U-1",
            "dcsnCntrctNo": "DEC-000100",
            "dminsttCd": "A1",
            "thtmCntrctAmt": "100.5",
            "corpList": corp_entry("1234567890", "33.3333335"),
        }
        compact = canonical_contract_from_row(
            {**base, "cntrctCnclsDate": "202608161015"}, "공사"
        )
        utc = canonical_contract_from_row(
            {**base, "cntrctCnclsDate": "2026-08-16T01:15:00Z"}, "공사"
        )

        self.assertEqual(compact.contract_date, "2026-08-16T10:15:00+09:00")
        self.assertEqual(compact.amount_won, 101)
        self.assertEqual(content_fingerprint(compact), content_fingerprint(utc))

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
