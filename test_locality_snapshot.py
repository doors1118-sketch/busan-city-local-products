import sqlite3
import tempfile
import unittest
from pathlib import Path

from company_locality import apply_company_changes, ensure_locality_schema
from contract_population import CanonicalContract, CanonicalSupplier, content_fingerprint
from core_calc import process_contract_row
from locality_snapshot import (
    MissingHistoricalSnapshot,
    SnapshotContentMismatch,
    SnapshotResolver,
    UnknownLocality,
    create_baseline_manifest,
    ensure_snapshot_schema,
    verify_baseline_manifest,
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
            **kwargs,
        )


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
        resolver.seed(self.old_contract, "1234567890", 100.0, True, "legacy_baseline_v1", BASELINE_ID)
        apply_company_changes(
            self.company_conn,
            [source_item("1234567890", "경남", "본사", "202608160900")],
            "20260816",
            "job-outbound",
            NOW,
        )

        self.assertTrue(resolver.resolve(self.old_contract, "1234567890", 100.0, False))
        self.assertEqual(resolver.flush(), 1)

    def test_pre_inbound_contract_remains_non_local_but_post_inbound_contract_is_local(self):
        resolver = self.resolver()
        resolver.seed(self.old_contract, "2222222222", 100.0, False, "legacy_baseline_v1", BASELINE_ID)
        apply_company_changes(
            self.company_conn,
            [source_item("2222222222", "부산", "본사", "20260816101500")],
            "20260816",
            "job-inbound",
            NOW,
        )

        self.assertFalse(resolver.resolve(self.old_contract, "2222222222", 100.0, True))
        self.assertTrue(resolver.resolve(self.new_contract, "2222222222", 100.0, True))
        self.assertEqual(resolver.flush(), 2)

    def test_pre_cutover_manifest_miss_is_a_hard_failure(self):
        with self.assertRaisesRegex(MissingHistoricalSnapshot, "pre-cutover"):
            self.resolver().resolve(self.old_contract, "3333333333", 100.0, True)

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

    def test_flush_uses_guarded_data_clock_and_direct_snapshot_write_fails(self):
        resolver = self.resolver()
        before = (read_data_generation(self.proc_conn), read_control_revision(self.proc_conn))
        self.assertTrue(resolver.resolve(self.new_contract, "1234567890", 100.0, True))
        resolver.flush()
        after = (read_data_generation(self.proc_conn), read_control_revision(self.proc_conn))

        self.assertGreater(after[0], before[0])
        self.assertEqual(after[1], before[1])
        with self.assertRaisesRegex(sqlite3.IntegrityError, "guarded session"):
            self.proc_conn.execute("DELETE FROM contract_supplier_locality")


class BaselineManifestTests(SnapshotTestCase):
    def test_manifest_verifies_every_contract_supplier_share_amount_and_fingerprint(self):
        first = contract("BASE1", "00", "2026-08-14", amount=101)
        second = contract(
            "BASE2",
            "00",
            "2026-08-15",
            suppliers=(CanonicalSupplier("1234567890", 60.0), CanonicalSupplier("4444444444", 40.0)),
            amount=202,
        )
        resolver = self.resolver()
        resolver.seed(first, "1234567890", 100.0, True, "legacy_baseline_v1", BASELINE_ID)
        resolver.seed(second, "1234567890", 60.0, True, "legacy_baseline_v1", BASELINE_ID)
        resolver.seed(second, "4444444444", 40.0, True, "legacy_baseline_v1", BASELINE_ID)
        resolver.flush()

        manifest = create_baseline_manifest(self.proc_conn, [second, first], BASELINE_ID)
        coverage = verify_baseline_manifest(self.proc_conn, BASELINE_ID)

        self.assertEqual(manifest.status, "complete")
        self.assertEqual((manifest.expected_contracts, manifest.expected_suppliers), (2, 3))
        self.assertEqual(manifest.expected_share_micros, 200_000_000)
        self.assertEqual(manifest.expected_amount_won, 303)
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


if __name__ == "__main__":
    unittest.main()
