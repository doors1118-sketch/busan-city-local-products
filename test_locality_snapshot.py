import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from company_locality import apply_company_changes, ensure_locality_schema
from contract_population import (
    CanonicalContract,
    CanonicalSupplier,
    content_fingerprint,
    iter_canonical_contracts,
)
from core_calc import process_contract_row
from locality_snapshot import (
    MissingHistoricalSnapshot,
    SnapshotContentMismatch,
    SnapshotResolver,
    UnknownLocality,
    create_baseline_manifest as _create_baseline_manifest,
    ensure_snapshot_schema,
    verify_baseline_manifest as _verify_baseline_manifest,
)
from maintenance_lock import (
    LocalityPaths,
    configure_locality_paths,
    maintenance_write_permission,
    read_control_revision,
    read_data_generation,
)


NOW = "2026-08-16 12:00:00+09:00"
CUTOVER = "2026-08-16 10:00:00+09:00"
BASELINE_ID = "baseline-v1"
REQUIRED_SECTORS = ("공사", "용역", "물품", "쇼핑몰")
ELIGIBLE_AGENCY_CODES = frozenset({"A1"})


def create_baseline_manifest(conn, rows, baseline_id):
    return _create_baseline_manifest(
        conn,
        rows,
        baseline_id,
        eligible_agency_codes=ELIGIBLE_AGENCY_CODES,
    )


def verify_baseline_manifest(conn, baseline_id):
    return _verify_baseline_manifest(
        conn,
        baseline_id,
        eligible_agency_codes=ELIGIBLE_AGENCY_CODES,
    )


def corp_entry(bizno, share):
    return f"[a^b^c^Supplier^e^f^{share}^h^i^{bizno}]"


def create_contract_source_tables(conn):
    for table in ("cnstwk_cntrct", "servc_cntrct", "thng_cntrct"):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                untyCntrctNo TEXT,
                dcsnCntrctNo TEXT,
                dminsttCd TEXT,
                dminsttList TEXT,
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
        CREATE TABLE IF NOT EXISTS shopping_cntrct (
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


def insert_required_sector_source(conn, sectors=REQUIRED_SECTORS):
    rows = {
        "공사": ("cnstwk_cntrct", "CONST-000100", 101),
        "용역": ("servc_cntrct", "SERVC-000100", 202),
        "물품": ("thng_cntrct", "GOODS-000100", 303),
    }
    for sector, (table, decision, amount) in rows.items():
        if sector not in sectors:
            continue
        conn.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"UNTY-{sector}",
                decision,
                "A1",
                "",
                "C1",
                amount,
                0,
                corp_entry("1234567890", "100"),
                "2026-08-15",
                "2026-08-14",
                "2026-08-15 09:00:00",
                "2026-08-15 10:00:00",
            ),
        )
    if "쇼핑몰" in sectors:
        conn.execute(
            "INSERT INTO shopping_cntrct VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("SHOP-1", "1", 0, "A1", 404, "1234567890", "2026-08-15", "", ""),
        )


def current_canonical_source(conn):
    return [
        row
        for sector in REQUIRED_SECTORS
        for row in iter_canonical_contracts(
            conn,
            sector,
            eligible_agency_codes=ELIGIBLE_AGENCY_CODES,
        )
    ]


def source_item(bizno, region, division, changed_at):
    return {
        "bizno": bizno,
        "corpNm": "Supplier",
        "rgnNm": region,
        "hdoffceDivNm": division,
        "chgDt": changed_at,
    }


def contract(
    key,
    revision,
    date,
    suppliers=(CanonicalSupplier("1234567890", 100.0),),
    amount=100,
    sector="공사",
):
    return CanonicalContract(sector, key, revision, "A1", amount, date, tuple(suppliers))


class SnapshotTestCase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        root = Path(self.tempdir.name)
        self.paths = LocalityPaths.for_in_memory_tests(
            root / "maintenance.lock",
            root / "transition.json",
            root / "marker",
            root / "pointer.json",
        )
        configure_locality_paths(self.paths)
        self.paths.pointer_path.write_text('{"active_generation_id":null}', encoding="ascii")
        self.proc_conn = sqlite3.connect(":memory:")
        self.company_conn = sqlite3.connect(":memory:")
        self.company_conn.execute(
            "CREATE TABLE company_master (bizno TEXT PRIMARY KEY, corpNm TEXT, rgnNm TEXT, hdoffceDivNm TEXT, chgDt TEXT)"
        )
        self.company_conn.executemany(
            "INSERT INTO company_master VALUES (?, 'Supplier', '부산', '본사', '')",
            [("1234567890",), ("4444444444",)],
        )
        self.company_conn.commit()
        ensure_locality_schema(self.company_conn, paths=self.paths)
        ensure_snapshot_schema(self.proc_conn)
        self.old_contract = contract("FAMILY1", "00", "2026-08-15")
        self.new_contract = contract("FAMILY2", "00", "2026-08-16 11:00:00+09:00")

    def tearDown(self):
        self.proc_conn.close()
        self.company_conn.close()
        self.tempdir.cleanup()

    def resolver(self, mode="snapshot", **kwargs):
        return SnapshotResolver(
            self.proc_conn,
            self.company_conn,
            mode=mode,
            now=NOW,
            cutover_at=CUTOVER,
            generation_id=kwargs.pop("generation_id", "generation-1"),
            eligible_agency_codes=ELIGIBLE_AGENCY_CODES,
            **kwargs,
        )

    def complete_source_baseline(self, historical_bizno="1234567890", is_busan=True):
        create_contract_source_tables(self.proc_conn)
        insert_required_sector_source(self.proc_conn)
        self.proc_conn.execute(
            "UPDATE cnstwk_cntrct SET corpList=?",
            (corp_entry(historical_bizno, "100"),),
        )
        rows = current_canonical_source(self.proc_conn)
        builder = self.resolver(mode="shadow")
        for row in rows:
            for supplier in row.suppliers:
                decision = is_busan if row.sector == "공사" else True
                builder.seed(
                    row,
                    supplier.bizno,
                    supplier.share_pct,
                    decision,
                    "legacy_baseline_v1",
                    BASELINE_ID,
                )
        builder.flush()
        manifest = create_baseline_manifest(self.proc_conn, rows, BASELINE_ID)
        self.assertEqual(manifest.status, "complete")
        return next(row for row in rows if row.sector == "공사")


class SnapshotResolverTests(SnapshotTestCase):

    def test_snapshot_schema_uses_required_primary_key_and_without_rowid(self):
        sql = self.proc_conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='contract_supplier_locality'"
        ).fetchone()[0]
        columns = {row[1]: row for row in self.proc_conn.execute("PRAGMA table_info(contract_supplier_locality)")}

        self.assertIn("WITHOUT ROWID", sql.upper())
        self.assertEqual([columns[name][5] for name in ("sector", "contract_key", "contract_revision", "bizno")], [1, 2, 3, 4])
        self.assertEqual(columns["is_busan"][3], 0)

    def test_outbound_move_does_not_change_frozen_historical_contract(self):
        resolver = self.resolver()
        historical = self.complete_source_baseline()
        apply_company_changes(
            self.company_conn,
            [source_item("1234567890", "경남", "본사", "202608160900")],
            "20260816",
            "job-outbound",
            NOW,
        )

        self.assertTrue(resolver.resolve(historical, "1234567890", 100.0, False))
        self.assertEqual(resolver.flush(), 0)

    def test_pre_inbound_contract_remains_non_local_but_post_inbound_contract_is_local(self):
        resolver = self.resolver()
        historical = self.complete_source_baseline("2222222222", False)
        apply_company_changes(
            self.company_conn,
            [source_item("2222222222", "부산", "본사", "20260816101500")],
            "20260816",
            "job-inbound",
            NOW,
        )

        self.assertFalse(resolver.resolve(historical, "2222222222", 100.0, True))
        self.assertTrue(resolver.resolve(self.new_contract, "2222222222", 100.0, True))
        self.assertEqual(resolver.flush(), 1)

    def test_snapshot_rejects_historical_row_after_canonical_source_drift(self):
        historical = self.complete_source_baseline()
        self.proc_conn.execute("UPDATE cnstwk_cntrct SET thtmCntrctAmt=999")

        with self.assertRaisesRegex(MissingHistoricalSnapshot, "coverage|baseline"):
            self.resolver().resolve(historical, "1234567890", 100.0, True)

    def test_repeated_historical_resolves_validate_all_sectors_once(self):
        historical = self.complete_source_baseline()
        resolver = self.resolver()

        with patch(
            "locality_snapshot.iter_canonical_contracts",
            wraps=iter_canonical_contracts,
        ) as canonical_scan:
            self.assertTrue(
                resolver.resolve(historical, "1234567890", 100.0, True)
            )
            self.assertTrue(
                resolver.resolve(historical, "1234567890", 100.0, True)
            )

        self.assertEqual(canonical_scan.call_count, 4)

    def test_source_generation_drift_invalidates_cached_validation_without_rescan(self):
        historical = self.complete_source_baseline()
        resolver = self.resolver()

        with patch(
            "locality_snapshot.iter_canonical_contracts",
            wraps=iter_canonical_contracts,
        ) as canonical_scan:
            resolver.resolve(historical, "1234567890", 100.0, True)
            with maintenance_write_permission(self.proc_conn):
                self.proc_conn.execute(
                    "UPDATE locality_generation_clock "
                    "SET data_generation=data_generation+1 WHERE singleton_id=1"
                )
            with self.assertRaisesRegex(MissingHistoricalSnapshot, "generation|drift"):
                resolver.resolve(historical, "1234567890", 100.0, True)

        self.assertEqual(canonical_scan.call_count, 4)

    def test_manifest_drift_invalidates_cached_validation_without_rescan(self):
        historical = self.complete_source_baseline()
        resolver = self.resolver()

        with patch(
            "locality_snapshot.iter_canonical_contracts",
            wraps=iter_canonical_contracts,
        ) as canonical_scan:
            resolver.resolve(historical, "1234567890", 100.0, True)
            with maintenance_write_permission(self.proc_conn):
                self.proc_conn.execute(
                    "UPDATE locality_baseline_manifest "
                    "SET manifest_fingerprint='changed' WHERE baseline_id=?",
                    (BASELINE_ID,),
                )
            with self.assertRaisesRegex(MissingHistoricalSnapshot, "manifest|metadata|drift"):
                resolver.resolve(historical, "1234567890", 100.0, True)

        self.assertEqual(canonical_scan.call_count, 4)

    def test_snapshot_rejects_invalid_complete_manifest_metadata(self):
        historical = self.complete_source_baseline()
        mutations = (
            ("locality_baseline_manifest", "classifier_version", "wrong-classifier"),
            ("locality_baseline_manifest", "iterator_version", "wrong-iterator"),
            ("locality_baseline_manifest", "cutover_at", "not-a-time"),
            ("contract_supplier_locality", "introduced_generation_id", "wrong-generation"),
        )
        for table, column, value in mutations:
            with self.subTest(column=column):
                with maintenance_write_permission(self.proc_conn):
                    original = self.proc_conn.execute(
                        f"SELECT {column} FROM {table} LIMIT 1"
                    ).fetchone()[0]
                    self.proc_conn.execute(f"UPDATE {table} SET {column}=?", (value,))
                with self.assertRaisesRegex(MissingHistoricalSnapshot, "metadata"):
                    self.resolver().resolve(historical, "1234567890", 100.0, True)
                with maintenance_write_permission(self.proc_conn):
                    self.proc_conn.execute(f"UPDATE {table} SET {column}=?", (original,))

    def test_compact_kst_cutover_is_equivalent_to_offset_timestamp(self):
        at_cutover = contract("CUTOVER1", "00", "2026-08-16T10:15:00+09:00")
        resolver = SnapshotResolver(
            self.proc_conn,
            self.company_conn,
            mode="snapshot",
            now=NOW,
            cutover_at="202608161015",
            generation_id="generation-1",
        )

        with self.assertRaisesRegex(MissingHistoricalSnapshot, "pre-cutover"):
            resolver.resolve(at_cutover, "1234567890", 100.0, True)

    def test_pre_cutover_manifest_miss_is_a_hard_failure(self):
        with self.assertRaisesRegex(MissingHistoricalSnapshot, "pre-cutover"):
            self.resolver().resolve(self.old_contract, "3333333333", 100.0, True)

    def test_snapshot_rejects_pre_cutover_shadow_row_without_baseline_owner(self):
        shadow = self.resolver(mode="shadow")
        self.assertTrue(shadow.resolve(self.old_contract, "1234567890", 100.0, True))
        shadow.flush()

        with self.assertRaisesRegex(MissingHistoricalSnapshot, "baseline|pre-cutover"):
            self.resolver().resolve(self.old_contract, "1234567890", 100.0, True)

    def test_snapshot_rejects_pre_cutover_row_owned_by_building_manifest(self):
        builder = self.resolver(mode="shadow")
        builder.seed(
            self.old_contract,
            "1234567890",
            100.0,
            True,
            "legacy_baseline_v1",
            BASELINE_ID,
        )
        builder.flush()

        with self.assertRaisesRegex(MissingHistoricalSnapshot, "complete"):
            self.resolver().resolve(self.old_contract, "1234567890", 100.0, True)

    def test_snapshot_rejects_pre_cutover_row_owned_by_failed_manifest(self):
        builder = self.resolver(mode="shadow")
        builder.seed(
            self.old_contract,
            "1234567890",
            100.0,
            True,
            "legacy_baseline_v1",
            BASELINE_ID,
        )
        builder.flush()
        with maintenance_write_permission(self.proc_conn):
            self.proc_conn.execute(
                "UPDATE locality_baseline_manifest SET status='failed' WHERE baseline_id=?",
                (BASELINE_ID,),
            )

        with self.assertRaisesRegex(MissingHistoricalSnapshot, "complete"):
            self.resolver().resolve(self.old_contract, "1234567890", 100.0, True)

    def test_snapshot_rejects_complete_manifest_that_does_not_own_row_membership(self):
        builder = self.resolver(mode="shadow")
        builder.seed(
            self.old_contract,
            "1234567890",
            100.0,
            True,
            "legacy_baseline_v1",
            BASELINE_ID,
        )
        builder.flush()
        with maintenance_write_permission(self.proc_conn):
            self.proc_conn.execute(
                "UPDATE locality_baseline_manifest SET status='complete' WHERE baseline_id=?",
                (BASELINE_ID,),
            )

        with self.assertRaisesRegex(MissingHistoricalSnapshot, "member|ownership|coverage"):
            self.resolver().resolve(self.old_contract, "1234567890", 100.0, True)

    def test_new_revision_inherits_unchanged_supplier_decision(self):
        resolver = self.resolver()
        first_revision = contract("FAMILY3", "00", "2026-08-15", amount=100)
        next_revision = contract("FAMILY3", "01", "2026-08-16 11:00:00+09:00", amount=120)
        resolver.seed(first_revision, "1234567890", 100.0, True, "legacy_baseline_v1", BASELINE_ID)
        resolver.flush()
        apply_company_changes(
            self.company_conn,
            [source_item("1234567890", "경남", "본사", "202608160900")],
            "20260816",
            "job-outbound",
            NOW,
        )

        self.assertTrue(resolver.resolve(next_revision, "1234567890", 100.0, False))
        resolver.flush()
        basis = self.proc_conn.execute(
            "SELECT basis FROM contract_supplier_locality WHERE contract_key='FAMILY3' AND contract_revision='01'"
        ).fetchone()[0]
        self.assertEqual(basis, "inherited_revision")

    def test_new_supplier_on_revision_uses_status_at_governing_date(self):
        resolver = self.resolver()
        first_revision = contract("FAMILY4", "00", "2026-08-15")
        suppliers = (
            CanonicalSupplier("1234567890", 50.0),
            CanonicalSupplier("2222222222", 50.0),
        )
        next_revision = contract("FAMILY4", "01", "2026-08-16 11:00:00+09:00", suppliers=suppliers)
        resolver.seed(first_revision, "1234567890", 100.0, True, "legacy_baseline_v1", BASELINE_ID)
        resolver.flush()
        apply_company_changes(
            self.company_conn,
            [source_item("2222222222", "부산", "본사", "202608161015")],
            "20260816",
            "job-inbound",
            NOW,
        )

        self.assertTrue(resolver.resolve(next_revision, "2222222222", 50.0, False))

    def test_reintroduced_supplier_does_not_inherit_across_immediate_revision_gap(self):
        resolver = self.resolver()
        revision_00 = contract("FAMILYGAP", "00", "2026-08-15")
        revision_01 = contract(
            "FAMILYGAP",
            "01",
            "2026-08-16 10:30:00+09:00",
            suppliers=(CanonicalSupplier("4444444444", 100.0),),
        )
        revision_02 = contract(
            "FAMILYGAP",
            "02",
            "2026-08-16 11:00:00+09:00",
        )
        resolver.seed(revision_00, "1234567890", 100.0, True, "legacy_baseline_v1", None)
        resolver.seed(revision_01, "4444444444", 100.0, True, "status_history", None)
        resolver.flush()
        apply_company_changes(
            self.company_conn,
            [source_item("1234567890", "경남", "본사", "20260816090000")],
            "20260816",
            "job-gap-outbound",
            NOW,
        )

        self.assertFalse(resolver.resolve(revision_02, "1234567890", 100.0, True))
        resolver.flush()
        self.assertEqual(
            self.proc_conn.execute(
                "SELECT basis FROM contract_supplier_locality WHERE contract_key='FAMILYGAP' AND contract_revision='02'"
            ).fetchone()[0],
            "status_history",
        )

    def test_revision_inheritance_rejects_divergent_immediate_revision_correction(self):
        prior = contract("FAMILYCORR", "01", "2026-08-16 10:30:00+09:00")
        current = contract("FAMILYCORR", "02", "2026-08-16 11:00:00+09:00")
        persisted = self.resolver(mode="shadow")
        persisted.seed(prior, "1234567890", 100.0, True, "status_history", None)
        persisted.flush()
        resolver = self.resolver()
        resolver.seed(prior, "1234567890", 100.0, False, "status_history", None)

        with self.assertRaisesRegex(SnapshotContentMismatch, "divergent"):
            resolver.resolve(current, "1234567890", 100.0, True)

    def test_post_cutover_supplier_without_lifecycle_history_keeps_legacy_policy_decision(self):
        resolver = self.resolver()
        no_history = contract(
            "FAMILY6",
            "00",
            "2026-08-16 11:00:00+09:00",
            suppliers=(CanonicalSupplier("6010000000", 100.0),),
        )

        self.assertTrue(resolver.resolve(no_history, "6010000000", 100.0, True))
        resolver.flush()
        self.assertEqual(
            self.proc_conn.execute(
                "SELECT basis FROM contract_supplier_locality WHERE contract_key='FAMILY6'"
            ).fetchone()[0],
            "legacy_policy_at_contract",
        )

    def test_post_cutover_contract_before_confirmed_inbound_is_non_local(self):
        apply_company_changes(
            self.company_conn,
            [source_item("2222222222", "부산", "본사", "20260816101500")],
            "20260816",
            "job-inbound",
            NOW,
        )
        before_inbound = contract(
            "FAMILY7",
            "00",
            "2026-08-16 10:05:00+09:00",
            suppliers=(CanonicalSupplier("2222222222", 100.0),),
        )
        resolver = self.resolver()

        self.assertFalse(resolver.resolve(before_inbound, "2222222222", 100.0, True))
        resolver.flush()
        self.assertEqual(
            self.proc_conn.execute(
                "SELECT basis FROM contract_supplier_locality WHERE contract_key='FAMILY7'"
            ).fetchone()[0],
            "pre_inbound_status_history",
        )

    def test_date_only_contract_on_transition_day_is_unknown(self):
        apply_company_changes(
            self.company_conn,
            [source_item("1234567890", "경남", "본사", "202608161100")],
            "20260816",
            "job-boundary",
            NOW,
        )
        ambiguous = contract("FAMILY5", "00", "2026-08-16")
        resolver = SnapshotResolver(
            self.proc_conn,
            self.company_conn,
            mode="snapshot",
            now=NOW,
            cutover_at="2026-08-15 23:59:59+09:00",
            generation_id="generation-1",
        )

        with self.assertRaisesRegex(UnknownLocality, "same-day|unknown"):
            resolver.resolve(ambiguous, "1234567890", 100.0, True)

    def test_snapshot_rejects_content_change_for_an_existing_identity(self):
        resolver = self.resolver()
        resolver.seed(self.old_contract, "1234567890", 100.0, True, "legacy_baseline_v1", BASELINE_ID)
        resolver.flush()
        corrected = contract("FAMILY1", "00", "2026-08-15", amount=101)

        with self.assertRaisesRegex(SnapshotContentMismatch, "fingerprint"):
            resolver.resolve(corrected, "1234567890", 100.0, True)

    def test_legacy_mode_returns_legacy_without_staging_rows(self):
        resolver = self.resolver(mode="legacy")

        self.assertFalse(resolver.resolve(self.new_contract, "1234567890", 100.0, False))
        self.assertEqual(resolver.flush(), 0)
        self.assertEqual(self.proc_conn.execute("SELECT COUNT(*) FROM contract_supplier_locality").fetchone()[0], 0)

    def test_shadow_mode_records_candidate_but_returns_legacy(self):
        resolver = self.resolver(mode="shadow")

        self.assertFalse(resolver.resolve(self.new_contract, "1234567890", 100.0, False))
        self.assertEqual(resolver.flush(), 1)
        self.assertEqual(
            self.proc_conn.execute("SELECT is_busan, basis FROM contract_supplier_locality").fetchone(),
            (1, "status_history_shadow"),
        )

    def test_shadow_mode_returns_legacy_even_when_a_frozen_snapshot_exists(self):
        resolver = self.resolver(mode="shadow")
        resolver.seed(self.old_contract, "1234567890", 100.0, True, "legacy_baseline_v1", BASELINE_ID)

        self.assertFalse(resolver.resolve(self.old_contract, "1234567890", 100.0, False))

    def test_existing_snapshot_rejects_supplier_share_drift(self):
        resolver = self.resolver()
        resolver.seed(self.old_contract, "1234567890", 100.0, True, "legacy_baseline_v1", BASELINE_ID)
        resolver.flush()

        with self.assertRaisesRegex(SnapshotContentMismatch, "share"):
            resolver.resolve(self.old_contract, "1234567890", 99.0, True)

    def test_snapshot_flush_is_derived_output_and_does_not_advance_data_clock(self):
        resolver = self.resolver()
        before = (read_data_generation(self.proc_conn), read_control_revision(self.proc_conn))
        self.assertTrue(resolver.resolve(self.new_contract, "1234567890", 100.0, True))
        resolver.flush()
        after = (read_data_generation(self.proc_conn), read_control_revision(self.proc_conn))

        self.assertEqual(after[0], before[0])
        self.assertGreater(after[1], before[1])
        with self.assertRaisesRegex(sqlite3.IntegrityError, "guarded session"):
            self.proc_conn.execute("DELETE FROM contract_supplier_locality")


class BaselineManifestTests(SnapshotTestCase):
    def build_source_baseline(self, *, basis="legacy_baseline_v1", sectors=REQUIRED_SECTORS):
        create_contract_source_tables(self.proc_conn)
        insert_required_sector_source(self.proc_conn, sectors)
        rows = current_canonical_source(self.proc_conn)
        resolver = self.resolver(mode="shadow")
        for row in rows:
            for supplier in row.suppliers:
                resolver.seed(
                    row,
                    supplier.bizno,
                    supplier.share_pct,
                    True,
                    basis,
                    BASELINE_ID,
                )
        resolver.flush()
        return rows, create_baseline_manifest(self.proc_conn, rows, BASELINE_ID)

    def test_manifest_verification_rejects_source_amount_correction(self):
        self.build_source_baseline()
        self.proc_conn.execute("UPDATE cnstwk_cntrct SET thtmCntrctAmt=999")

        coverage = verify_baseline_manifest(self.proc_conn, BASELINE_ID)

        self.assertFalse(coverage.complete)

    def test_historical_baseline_fingerprints_first_eligible_agency_candidate(self):
        create_contract_source_tables(self.proc_conn)
        insert_required_sector_source(self.proc_conn)
        self.proc_conn.execute(
            "UPDATE cnstwk_cntrct "
            "SET dminsttCd='UNKNOWN', dminsttList='[1^A1^Agency^x]'"
        )
        rows = current_canonical_source(self.proc_conn)
        construction = next(row for row in rows if row.sector == "공사")
        resolver = self.resolver(mode="shadow")
        for row in rows:
            for supplier in row.suppliers:
                resolver.seed(
                    row,
                    supplier.bizno,
                    supplier.share_pct,
                    True,
                    "legacy_baseline_v1",
                    BASELINE_ID,
                )
        resolver.flush()

        manifest = create_baseline_manifest(self.proc_conn, rows, BASELINE_ID)

        self.assertEqual(construction.agency, "A1")
        self.assertEqual(manifest.status, "complete")
        self.assertTrue(verify_baseline_manifest(self.proc_conn, BASELINE_ID).complete)

    def test_historical_baseline_without_eligible_agencies_fails_closed(self):
        create_contract_source_tables(self.proc_conn)
        insert_required_sector_source(self.proc_conn)
        rows = [
            row
            for sector in REQUIRED_SECTORS
            for row in iter_canonical_contracts(self.proc_conn, sector)
        ]
        resolver = self.resolver(mode="shadow")
        for row in rows:
            for supplier in row.suppliers:
                resolver.seed(
                    row,
                    supplier.bizno,
                    supplier.share_pct,
                    True,
                    "legacy_baseline_v1",
                    BASELINE_ID,
                )
        resolver.flush()

        manifest = _create_baseline_manifest(self.proc_conn, rows, BASELINE_ID)

        self.assertEqual(manifest.status, "failed")

    def test_same_day_post_cutover_new_family_does_not_change_historical_baseline(self):
        self.build_source_baseline()
        self.proc_conn.execute(
            "INSERT INTO cnstwk_cntrct VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "UNTY-LATE",
                "LATE-000100",
                "A1",
                "",
                "C1",
                999,
                0,
                corp_entry("1234567890", "100"),
                "2026-08-16T11:00:00+09:00",
                "2026-08-16",
                "",
                "",
            ),
        )

        self.assertTrue(verify_baseline_manifest(self.proc_conn, BASELINE_ID).complete)

    def test_same_day_post_cutover_revision_does_not_displace_historical_revision(self):
        self.build_source_baseline()
        self.proc_conn.execute(
            "INSERT INTO cnstwk_cntrct VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "UNTY-공사",
                "CONST-000101",
                "A1",
                "",
                "C1",
                999,
                0,
                corp_entry("1234567890", "100"),
                "2026-08-16T11:00:00+09:00",
                "2026-08-16",
                "",
                "",
            ),
        )

        self.assertTrue(verify_baseline_manifest(self.proc_conn, BASELINE_ID).complete)

    def test_manifest_verification_rejects_exact_source_dimension_drift(self):
        self.build_source_baseline()
        mutations = (
            ("dcsnCntrctNo", "CONST-000102", "CONST-000100"),
            ("dminsttCd", "A2", "A1"),
            ("corpList", corp_entry("1234567890", "80"), corp_entry("1234567890", "100")),
            ("corpList", corp_entry("9999999999", "100"), corp_entry("1234567890", "100")),
        )
        for column, changed, original in mutations:
            with self.subTest(column=column, changed=changed):
                self.proc_conn.execute(
                    f"UPDATE cnstwk_cntrct SET {column}=?", (changed,)
                )
                self.assertFalse(
                    verify_baseline_manifest(self.proc_conn, BASELINE_ID).complete
                )
                self.proc_conn.execute(
                    f"UPDATE cnstwk_cntrct SET {column}=?", (original,)
                )
                self.assertTrue(
                    verify_baseline_manifest(self.proc_conn, BASELINE_ID).complete
                )

    def test_manifest_verification_rejects_new_source_identity(self):
        self.build_source_baseline()
        self.proc_conn.execute(
            "INSERT INTO thng_cntrct VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "UNTY-NEW",
                "GOODS-900100",
                "A1",
                "",
                "C1",
                505,
                0,
                corp_entry("1234567890", "100"),
                "2026-08-15",
                "2026-08-14",
                "",
                "",
            ),
        )

        coverage = verify_baseline_manifest(self.proc_conn, BASELINE_ID)

        self.assertFalse(coverage.complete)

    def test_manifest_verification_rejects_deleted_source_identity(self):
        self.build_source_baseline()
        self.proc_conn.execute("DELETE FROM servc_cntrct")

        coverage = verify_baseline_manifest(self.proc_conn, BASELINE_ID)

        self.assertFalse(coverage.complete)

    def test_manifest_rejects_empty_canonical_population(self):
        create_contract_source_tables(self.proc_conn)

        manifest = create_baseline_manifest(self.proc_conn, [], BASELINE_ID)
        coverage = verify_baseline_manifest(self.proc_conn, BASELINE_ID)

        self.assertEqual(manifest.status, "failed")
        self.assertFalse(coverage.complete)

    def test_manifest_rejects_partial_required_sector_population(self):
        rows, manifest = self.build_source_baseline(sectors=("공사",))
        coverage = verify_baseline_manifest(self.proc_conn, BASELINE_ID)

        self.assertEqual({row.sector for row in rows}, {"공사"})
        self.assertEqual(manifest.status, "failed")
        self.assertFalse(coverage.complete)

    def test_manifest_verification_rejects_nonbaseline_basis_and_counts_fallback_amount(self):
        self.build_source_baseline(basis="legacy_shadow_candidate")

        coverage = verify_baseline_manifest(self.proc_conn, BASELINE_ID)

        self.assertFalse(coverage.complete)
        self.assertGreater(coverage.fallback_amount_won, 0)

    def test_manifest_verifies_every_contract_supplier_share_amount_and_fingerprint(self):
        rows, manifest = self.build_source_baseline()
        coverage = verify_baseline_manifest(self.proc_conn, BASELINE_ID)

        self.assertEqual(manifest.status, "complete")
        self.assertEqual(len(rows), 4)
        self.assertEqual((manifest.expected_contracts, manifest.expected_suppliers), (4, 4))
        self.assertEqual(manifest.expected_share_micros, 400_000_000)
        self.assertEqual(manifest.expected_amount_won, 1010)
        self.assertEqual(manifest.cutover_at, CUTOVER)
        self.assertEqual(manifest.cache_generation_id, "generation-1")
        self.assertTrue(coverage.complete)
        self.assertEqual(coverage.coverage_pct, 100.0)
        self.assertEqual(coverage.unknown_amount_won, 0)
        self.assertEqual(coverage.fingerprint_mismatches, 0)

    def test_manifest_verification_fails_closed_on_share_or_fingerprint_drift(self):
        row = contract("BASE3", "00", "2026-08-15")
        resolver = self.resolver()
        resolver.seed(row, "1234567890", 100.0, True, "legacy_baseline_v1", BASELINE_ID)
        resolver.flush()
        create_baseline_manifest(self.proc_conn, [row], BASELINE_ID)
        with maintenance_write_permission(self.proc_conn):
            self.proc_conn.execute(
                "UPDATE contract_supplier_locality SET share_pct=99, content_fingerprint='drift'"
            )
        coverage = verify_baseline_manifest(self.proc_conn, BASELINE_ID)

        self.assertFalse(coverage.complete)
        self.assertLess(coverage.coverage_pct, 100.0)
        self.assertEqual(coverage.fingerprint_mismatches, 1)

    def test_manifest_verification_counts_unknown_historical_amount(self):
        row = contract("BASE4", "00", "2026-08-15", amount=125)
        resolver = self.resolver()
        resolver.seed(row, "1234567890", 100.0, None, "legacy_baseline_v1", BASELINE_ID)
        resolver.flush()
        manifest = create_baseline_manifest(self.proc_conn, [row], BASELINE_ID)
        coverage = verify_baseline_manifest(self.proc_conn, BASELINE_ID)

        self.assertEqual(manifest.status, "failed")
        self.assertFalse(coverage.complete)
        self.assertEqual(coverage.unknown_amount_won, 125)

    def test_manifest_fingerprint_tampering_fails_verification(self):
        row = contract("BASE5", "00", "2026-08-15")
        resolver = self.resolver()
        resolver.seed(row, "1234567890", 100.0, True, "legacy_baseline_v1", BASELINE_ID)
        resolver.flush()
        create_baseline_manifest(self.proc_conn, [row], BASELINE_ID)
        with maintenance_write_permission(self.proc_conn):
            self.proc_conn.execute(
                "UPDATE locality_baseline_manifest SET manifest_fingerprint='tampered' WHERE baseline_id=?",
                (BASELINE_ID,),
            )

        coverage = verify_baseline_manifest(self.proc_conn, BASELINE_ID)

        self.assertFalse(coverage.complete)
        self.assertFalse(coverage.manifest_fingerprint_matches)


class CoreCalculationResolverTests(unittest.TestCase):
    def test_no_resolver_and_legacy_resolver_preserve_exact_local_amount(self):
        row = {
            "untyCntrctNo": "U-1",
            "dcsnCntrctNo": "DEC-000100",
            "dminsttCd": "A1",
            "thtmCntrctAmt": 100,
            "totCntrctAmt": 0,
            "corpList": "[a^b^c^Supplier^e^f^25^h^i^123-45-67890][a^b^c^Supplier^e^f^75^h^i^9999999999]",
            "cntrctCnclsDate": "2026-08-15",
        }
        inst = {"A1": {"cate_lrg": "group"}}

        without_resolver = process_contract_row(row, inst, {"1234567890"})

        class LegacyResolver:
            def resolve(self, resolved_row, bizno, share_pct, legacy_is_local):
                self.sector = resolved_row["sector"]
                return legacy_is_local

        resolver = LegacyResolver()
        with_resolver = process_contract_row(
            row,
            inst,
            {"1234567890"},
            locality_resolver=resolver,
            sector="공사",
        )

        self.assertEqual(without_resolver, ("A1", 100.0, 25.0))
        self.assertEqual(with_resolver, without_resolver)
        self.assertEqual(resolver.sector, "공사")

    def test_resolver_receives_aggregated_duplicate_supplier_share(self):
        row = {
            "dminsttCd": "A1",
            "thtmCntrctAmt": 100,
            "corpList": "[a^b^c^Supplier^e^f^20^h^i^123-45-67890][a^b^c^Supplier^e^f^30^h^i^1234567890]",
        }
        calls = []

        class Resolver:
            def resolve(self, resolved_row, bizno, share_pct, legacy_is_local):
                calls.append((bizno, share_pct, legacy_is_local))
                return legacy_is_local

        process_contract_row(
            row,
            {"A1": {"cate_lrg": "group"}},
            {"1234567890"},
            locality_resolver=Resolver(),
            sector="공사",
        )

        self.assertEqual(calls, [("1234567890", 50.0, True)])

    def test_legacy_resolver_preserves_duplicate_tiny_share_float_order(self):
        row = {
            "dminsttCd": "A1",
            "thtmCntrctAmt": 1,
            "corpList": "".join(corp_entry("1234567890", "0.01") for _ in range(3)),
        }
        calls = []

        class LegacyResolver:
            def resolve(self, resolved_row, bizno, share_pct, legacy_is_local):
                calls.append((bizno, share_pct, legacy_is_local))
                return legacy_is_local

        without_resolver = process_contract_row(
            row, {"A1": {"cate_lrg": "group"}}, {"1234567890"}
        )
        with_resolver = process_contract_row(
            row,
            {"A1": {"cate_lrg": "group"}},
            {"1234567890"},
            locality_resolver=LegacyResolver(),
            sector="공사",
        )

        self.assertEqual(without_resolver[2], 0.00030000000000000003)
        self.assertEqual(with_resolver, without_resolver)
        self.assertEqual(calls, [("1234567890", 0.03, True)])


if __name__ == "__main__":
    unittest.main()
