import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import locality_snapshot as locality_snapshot_module
from company_locality import apply_company_changes, ensure_locality_schema
from contract_population import (
    CanonicalContract,
    CanonicalSupplier,
    canonical_contract_from_row,
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
    refresh_source_schema,
    verify_baseline_manifest as _verify_baseline_manifest,
)
from maintenance_lock import (
    LocalityPaths,
    configure_locality_paths,
    guarded_write_session,
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
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    if {
        "cnstwk_cntrct",
        "servc_cntrct",
        "thng_cntrct",
        "shopping_cntrct",
    }.issubset(tables):
        refresh_source_schema(
            conn,
            operator="test-suite",
            reason=f"protect sources for {baseline_id}",
        )
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
        eligible_agency_codes = kwargs.pop(
            "eligible_agency_codes", ELIGIBLE_AGENCY_CODES
        )
        return SnapshotResolver(
            self.proc_conn,
            self.company_conn,
            mode=mode,
            now=NOW,
            cutover_at=CUTOVER,
            generation_id=kwargs.pop("generation_id", "generation-1"),
            selected_baseline_id=kwargs.pop("selected_baseline_id", BASELINE_ID),
            eligible_agency_codes=eligible_agency_codes,
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

    def assert_warm_cache_mutation_is_rejected(self, mutation):
        historical = self.complete_source_baseline()
        resolver = self.resolver()
        with patch(
            "locality_snapshot.iter_canonical_contracts",
            wraps=iter_canonical_contracts,
        ) as canonical_scan:
            resolver.resolve(historical, "1234567890", 100.0, True)
            with self.assertRaisesRegex(sqlite3.IntegrityError, "complete baseline"):
                with guarded_write_session(self.proc_conn):
                    mutation()
            self.assertTrue(
                resolver.resolve(historical, "1234567890", 100.0, True)
            )
        self.assertEqual(canonical_scan.call_count, 0)

    def source_refresh_state(self):
        return {
            "schema": self.proc_conn.execute(
                "SELECT type, name, tbl_name, sql FROM sqlite_master "
                "WHERE type IN ('table','index','view','trigger') "
                "AND name NOT LIKE 'sqlite_%' ORDER BY type, name"
            ).fetchall(),
            "construction": self.proc_conn.execute(
                "SELECT * FROM cnstwk_cntrct ORDER BY rowid"
            ).fetchall(),
            "schema_state": self.proc_conn.execute(
                "SELECT * FROM locality_source_schema_state ORDER BY singleton_id"
            ).fetchall(),
            "audit": self.proc_conn.execute(
                "SELECT * FROM locality_source_schema_audit ORDER BY id"
            ).fetchall(),
            "sequence": self.proc_conn.execute(
                "SELECT * FROM sqlite_sequence ORDER BY name"
            ).fetchall(),
            "clock": self.proc_conn.execute(
                "SELECT * FROM locality_generation_clock ORDER BY singleton_id"
            ).fetchall(),
        }

    def test_snapshot_schema_uses_required_primary_key_and_without_rowid(self):
        sql = self.proc_conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='contract_supplier_locality'"
        ).fetchone()[0]
        columns = {row[1]: row for row in self.proc_conn.execute("PRAGMA table_info(contract_supplier_locality)")}

        self.assertIn("WITHOUT ROWID", sql.upper())
        self.assertEqual(
            [
                columns[name][5]
                for name in (
                    "baseline_id",
                    "sector",
                    "contract_key",
                    "contract_revision",
                    "bizno",
                )
            ],
            [1, 2, 3, 4, 5],
        )
        self.assertEqual(columns["is_busan"][3], 0)

    def test_replacement_baseline_versions_same_population_and_selects_explicit_decision(self):
        historical_v1 = self.complete_source_baseline()
        frozen_v1 = {
            table: self.proc_conn.execute(
                f"SELECT * FROM {table} WHERE baseline_id=? ORDER BY 1,2,3,4",
                (BASELINE_ID,),
            ).fetchall()
            for table in (
                "locality_baseline_manifest",
                "locality_baseline_contract",
                "locality_baseline_supplier",
                "locality_baseline_verification",
                "contract_supplier_locality",
            )
        }
        with guarded_write_session(self.proc_conn):
            self.proc_conn.execute(
                "UPDATE cnstwk_cntrct SET thtmCntrctAmt=111"
            )
        rows_v2 = current_canonical_source(self.proc_conn)
        historical_v2 = next(row for row in rows_v2 if row.sector == "공사")
        builder = self.resolver(
            mode="shadow",
            generation_id="generation-2",
            selected_baseline_id="baseline-v2",
        )
        for row in rows_v2:
            for supplier in row.suppliers:
                builder.seed(
                    row,
                    supplier.bizno,
                    supplier.share_pct,
                    row.sector != "공사",
                    "legacy_baseline_v1",
                    "baseline-v2",
                )
        self.assertEqual(builder.flush(), 4)

        manifest_v2 = create_baseline_manifest(
            self.proc_conn, rows_v2, "baseline-v2"
        )

        self.assertEqual(manifest_v2.status, "complete")
        self.assertEqual(historical_v1.identity, historical_v2.identity)
        self.assertNotEqual(
            content_fingerprint(historical_v1), content_fingerprint(historical_v2)
        )
        for table, rows in frozen_v1.items():
            self.assertEqual(
                self.proc_conn.execute(
                    f"SELECT * FROM {table} WHERE baseline_id=? ORDER BY 1,2,3,4",
                    (BASELINE_ID,),
                ).fetchall(),
                rows,
            )
        resolver_v1 = self.resolver(
            generation_id="generation-1",
            selected_baseline_id=BASELINE_ID,
        )
        resolver_v2 = self.resolver(
            generation_id="generation-2",
            selected_baseline_id="baseline-v2",
        )
        self.assertTrue(
            resolver_v1.resolve(historical_v1, "1234567890", 100.0, True)
        )
        self.assertFalse(
            resolver_v2.resolve(historical_v2, "1234567890", 100.0, True)
        )
        self.assertEqual(
            self.proc_conn.execute(
                "SELECT COUNT(*) FROM contract_supplier_locality "
                "WHERE sector='공사' AND contract_key=? AND contract_revision=? "
                "AND bizno='1234567890'",
                historical_v2.identity[1:],
            ).fetchone()[0],
            2,
        )

    def test_snapshot_requires_explicit_baseline_when_versions_are_ambiguous(self):
        historical = self.complete_source_baseline()
        rows = current_canonical_source(self.proc_conn)
        builder = self.resolver(
            mode="shadow",
            generation_id="generation-2",
            selected_baseline_id="baseline-v2",
        )
        for row in rows:
            for supplier in row.suppliers:
                builder.seed(
                    row,
                    supplier.bizno,
                    supplier.share_pct,
                    False,
                    "legacy_baseline_v1",
                    "baseline-v2",
                )
        builder.flush()
        create_baseline_manifest(self.proc_conn, rows, "baseline-v2")

        resolver = self.resolver(
            generation_id="generation-2", selected_baseline_id=None
        )
        with self.assertRaisesRegex(MissingHistoricalSnapshot, "selected baseline"):
            resolver.resolve(historical, "1234567890", 100.0, True)

    def test_post_cutover_decisions_are_versioned_by_generation(self):
        self.complete_source_baseline()
        generation_1 = self.resolver(generation_id="generation-1")
        self.assertTrue(
            generation_1.resolve(
                self.new_contract, "1234567890", 100.0, True
            )
        )
        self.assertEqual(generation_1.flush(), 1)
        apply_company_changes(
            self.company_conn,
            [source_item("1234567890", "경남", "본사", "202608161030")],
            "20260816",
            "job-post-cutover-outbound",
            NOW,
        )

        generation_2 = self.resolver(generation_id="generation-2")
        self.assertFalse(
            generation_2.resolve(
                self.new_contract, "1234567890", 100.0, True
            )
        )
        self.assertEqual(generation_2.flush(), 1)
        self.assertEqual(
            self.proc_conn.execute(
                "SELECT baseline_id FROM contract_supplier_locality "
                "WHERE sector=? AND contract_key=? AND contract_revision=? "
                "AND bizno='1234567890' ORDER BY baseline_id",
                self.new_contract.identity,
            ).fetchall(),
            [
                ("@generation:generation-1",),
                ("@generation:generation-2",),
            ],
        )

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

    def test_explicit_historical_baseline_uses_its_owned_proof_after_source_correction(self):
        historical = self.complete_source_baseline()
        with guarded_write_session(self.proc_conn):
            self.proc_conn.execute(
                "UPDATE cnstwk_cntrct SET thtmCntrctAmt=999"
            )

        self.assertTrue(
            self.resolver().resolve(historical, "1234567890", 100.0, True)
        )

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

        self.assertEqual(canonical_scan.call_count, 0)

    def test_warm_resolver_checks_only_pinned_schema_version_not_sqlite_master(self):
        historical = self.complete_source_baseline()
        resolver = self.resolver()
        resolver.resolve(historical, "1234567890", 100.0, True)
        statements = []
        self.proc_conn.set_trace_callback(statements.append)
        try:
            self.assertTrue(
                resolver.resolve(historical, "1234567890", 100.0, True)
            )
        finally:
            self.proc_conn.set_trace_callback(None)

        self.assertFalse(
            any("sqlite_master" in statement.lower() for statement in statements)
        )
        self.assertTrue(
            any("pragma schema_version" in statement.lower() for statement in statements)
        )

    def test_warm_cache_rejects_complete_manifest_header_mutation(self):
        self.assert_warm_cache_mutation_is_rejected(
            lambda: self.proc_conn.execute(
                "UPDATE locality_baseline_manifest SET status='failed' WHERE baseline_id=?",
                (BASELINE_ID,),
            )
        )

    def test_warm_cache_rejects_complete_manifest_header_delete(self):
        self.assert_warm_cache_mutation_is_rejected(
            lambda: self.proc_conn.execute(
                "DELETE FROM locality_baseline_manifest WHERE baseline_id=?",
                (BASELINE_ID,),
            )
        )

    def test_warm_cache_rejects_complete_baseline_child_insert(self):
        self.assert_warm_cache_mutation_is_rejected(
            lambda: self.proc_conn.execute(
                "INSERT INTO locality_baseline_contract VALUES (?, ?, ?, ?, ?, ?)",
                (BASELINE_ID, "공사", "FORGED", "00", 1, "forged"),
            )
        )

    def test_warm_cache_rejects_complete_baseline_child_update(self):
        self.assert_warm_cache_mutation_is_rejected(
            lambda: self.proc_conn.execute(
                "UPDATE locality_baseline_contract SET amount_won=amount_won+1 "
                "WHERE baseline_id=? AND sector='공사'",
                (BASELINE_ID,),
            )
        )

    def test_warm_cache_rejects_complete_baseline_child_delete(self):
        self.assert_warm_cache_mutation_is_rejected(
            lambda: self.proc_conn.execute(
                "DELETE FROM locality_baseline_supplier "
                "WHERE baseline_id=? AND sector='공사'",
                (BASELINE_ID,),
            )
        )

    def test_warm_cache_rejects_complete_snapshot_reownership(self):
        self.assert_warm_cache_mutation_is_rejected(
            lambda: self.proc_conn.execute(
                "UPDATE contract_supplier_locality SET baseline_id=NULL "
                "WHERE baseline_id=? AND sector='공사'",
                (BASELINE_ID,),
            )
        )

    def test_warm_cache_rejects_complete_snapshot_insert(self):
        self.assert_warm_cache_mutation_is_rejected(
            lambda: self.proc_conn.execute(
                """
                INSERT INTO contract_supplier_locality
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "공사",
                    "FORGED",
                    "00",
                    "9999999999",
                    100.0,
                    1,
                    "legacy_baseline_v1",
                    "2026-08-15",
                    NOW,
                    "supplier_locality_snapshot_v1",
                    "forged",
                    BASELINE_ID,
                    "generation-1",
                ),
            )
        )

    def test_warm_cache_rejects_complete_snapshot_update(self):
        self.assert_warm_cache_mutation_is_rejected(
            lambda: self.proc_conn.execute(
                "UPDATE contract_supplier_locality SET is_busan=0 "
                "WHERE baseline_id=? AND sector='공사'",
                (BASELINE_ID,),
            )
        )

    def test_warm_cache_rejects_complete_snapshot_delete(self):
        self.assert_warm_cache_mutation_is_rejected(
            lambda: self.proc_conn.execute(
                "DELETE FROM contract_supplier_locality "
                "WHERE baseline_id=? AND sector='공사'",
                (BASELINE_ID,),
            )
        )

    def test_complete_baseline_verification_evidence_is_immutable(self):
        self.assert_warm_cache_mutation_is_rejected(
            lambda: self.proc_conn.execute(
                "UPDATE locality_baseline_verification "
                "SET proof_fingerprint='tampered' WHERE baseline_id=?",
                (BASELINE_ID,),
            )
        )

    def test_source_generation_drift_invalidates_cached_validation_without_rescan(self):
        historical = self.complete_source_baseline()
        ensure_snapshot_schema(self.proc_conn)
        resolver = self.resolver()

        with patch(
            "locality_snapshot.iter_canonical_contracts",
            wraps=iter_canonical_contracts,
        ) as canonical_scan:
            resolver.resolve(historical, "1234567890", 100.0, True)
            before = read_data_generation(self.proc_conn)
            with guarded_write_session(self.proc_conn):
                self.proc_conn.execute(
                    "UPDATE cnstwk_cntrct SET thtmCntrctAmt=999"
                )
            self.assertEqual(read_data_generation(self.proc_conn), before + 1)
            with self.assertRaisesRegex(MissingHistoricalSnapshot, "generation|drift"):
                resolver.resolve(historical, "1234567890", 100.0, True)

        self.assertEqual(canonical_scan.call_count, 0)

    def test_warm_resolver_rejects_caller_read_transaction_after_concurrent_refresh(self):
        root = Path(self.tempdir.name)
        procurement_path = root / "wal-procurement.sqlite3"
        company_path = root / "wal-company.sqlite3"
        wal_paths = LocalityPaths(
            company_path,
            procurement_path,
            root / "wal-maintenance.lock",
            root / "wal-transition.json",
            root / "wal-marker",
            root / "wal-pointer.json",
        )
        configure_locality_paths(wal_paths)
        wal_paths.pointer_path.write_text(
            '{"active_generation_id":null}', encoding="ascii"
        )
        company_connection = sqlite3.connect(company_path)
        company_connection.execute(
            "CREATE TABLE company_master "
            "(bizno TEXT PRIMARY KEY, corpNm TEXT, rgnNm TEXT, "
            "hdoffceDivNm TEXT, chgDt TEXT)"
        )
        company_connection.execute(
            "INSERT INTO company_master VALUES "
            "('1234567890', 'Supplier', '부산', '본사', '')"
        )
        company_connection.commit()
        ensure_locality_schema(company_connection, paths=wal_paths)
        connection_a = sqlite3.connect(procurement_path)
        connection_b = sqlite3.connect(procurement_path)
        try:
            ensure_snapshot_schema(connection_a)
            create_contract_source_tables(connection_a)
            insert_required_sector_source(connection_a)
            connection_a.commit()
            refresh_source_schema(
                connection_a,
                operator="operator-wal-setup",
                reason="protect WAL concurrency fixture",
            )
            rows = current_canonical_source(connection_a)
            builder = SnapshotResolver(
                connection_a,
                company_connection,
                mode="shadow",
                now=NOW,
                cutover_at=CUTOVER,
                generation_id="generation-1",
                selected_baseline_id=BASELINE_ID,
                eligible_agency_codes=ELIGIBLE_AGENCY_CODES,
            )
            for row in rows:
                for supplier in row.suppliers:
                    builder.seed(
                        row,
                        supplier.bizno,
                        supplier.share_pct,
                        True,
                        "legacy_baseline_v1",
                        BASELINE_ID,
                    )
            builder.flush()
            manifest = create_baseline_manifest(
                connection_a, rows, BASELINE_ID
            )
            self.assertEqual(manifest.status, "complete")
            historical = next(row for row in rows if row.sector == "공사")
            resolver = SnapshotResolver(
                connection_a,
                company_connection,
                mode="snapshot",
                now=NOW,
                cutover_at=CUTOVER,
                generation_id="generation-1",
                selected_baseline_id=BASELINE_ID,
                eligible_agency_codes=ELIGIBLE_AGENCY_CODES,
            )
            self.assertTrue(
                resolver.resolve(historical, "1234567890", 100.0, True)
            )

            connection_a.execute("BEGIN")
            stale_generation = read_data_generation(connection_a)
            refresh_source_schema(
                connection_b,
                operator="operator-wal-refresh",
                reason="replace source while reader holds WAL snapshot",
                replace=lambda plan: plan.execute(
                    "UPDATE cnstwk_cntrct SET thtmCntrctAmt=707"
                ),
            )
            self.assertEqual(
                read_data_generation(connection_b), stale_generation + 1
            )
            with self.assertRaisesRegex(
                MissingHistoricalSnapshot, "caller-owned procurement transaction"
            ):
                resolver.resolve(historical, "1234567890", 100.0, True)

            connection_a.rollback()
            with self.assertRaisesRegex(
                MissingHistoricalSnapshot, "source schema|generation|drift"
            ):
                resolver.resolve(historical, "1234567890", 100.0, True)
        finally:
            if connection_a.in_transaction:
                connection_a.rollback()
            connection_b.close()
            connection_a.close()
            company_connection.close()
            configure_locality_paths(self.paths)

    def test_warm_resolver_rejects_drop_recreated_source_table_before_rescan(self):
        historical = self.complete_source_baseline()
        resolver = self.resolver()

        with patch(
            "locality_snapshot.iter_canonical_contracts",
            wraps=iter_canonical_contracts,
        ) as canonical_scan:
            self.assertTrue(
                resolver.resolve(historical, "1234567890", 100.0, True)
            )
            generation = read_data_generation(self.proc_conn)
            original_sql = self.proc_conn.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type='table' AND name='cnstwk_cntrct'"
            ).fetchone()[0]
            original_row = self.proc_conn.execute(
                "SELECT * FROM cnstwk_cntrct"
            ).fetchone()
            self.proc_conn.execute("DROP TABLE cnstwk_cntrct")
            self.proc_conn.execute(original_sql)
            self.proc_conn.execute(
                "INSERT INTO cnstwk_cntrct VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                original_row,
            )
            self.proc_conn.commit()

            self.assertEqual(read_data_generation(self.proc_conn), generation)
            with self.assertRaisesRegex(
                MissingHistoricalSnapshot, "source schema|protection"
            ):
                resolver.resolve(historical, "1234567890", 100.0, True)

        self.assertEqual(canonical_scan.call_count, 0)

    def test_missing_source_guard_blocks_resolver_activation(self):
        self.complete_source_baseline()
        self.proc_conn.execute(
            "DROP TRIGGER locality_cnstwk_cntrct_update_guard"
        )

        with self.assertRaisesRegex(
            MissingHistoricalSnapshot, "source schema|protection|trigger"
        ):
            self.resolver()

    def test_late_schema_creation_invalidates_warm_resolver_without_rescan(self):
        historical = self.complete_source_baseline()
        resolver = self.resolver()

        with patch(
            "locality_snapshot.iter_canonical_contracts",
            wraps=iter_canonical_contracts,
        ) as canonical_scan:
            self.assertTrue(
                resolver.resolve(historical, "1234567890", 100.0, True)
            )
            generation = read_data_generation(self.proc_conn)
            self.proc_conn.execute("CREATE TABLE late_schema_probe (id INTEGER)")

            self.assertEqual(read_data_generation(self.proc_conn), generation)
            with self.assertRaisesRegex(
                MissingHistoricalSnapshot, "source schema|protection"
            ):
                resolver.resolve(historical, "1234567890", 100.0, True)

        self.assertEqual(canonical_scan.call_count, 0)

    def test_late_created_source_table_requires_audited_refresh(self):
        create_contract_source_tables(self.proc_conn)
        insert_required_sector_source(self.proc_conn)

        manifest = _create_baseline_manifest(
            self.proc_conn,
            current_canonical_source(self.proc_conn),
            BASELINE_ID,
            eligible_agency_codes=ELIGIBLE_AGENCY_CODES,
        )
        self.assertEqual(manifest.status, "failed")
        self.assertIsNone(
            self.proc_conn.execute(
                "SELECT 1 FROM locality_baseline_verification WHERE baseline_id=?",
                (BASELINE_ID,),
            ).fetchone()
        )

        before = read_data_generation(self.proc_conn)
        refresh_source_schema(
            self.proc_conn,
            operator="operator-1",
            reason="approve late-created canonical source tables",
        )

        self.assertEqual(read_data_generation(self.proc_conn), before + 1)
        self.assertEqual(
            self.proc_conn.execute(
                "SELECT operator, reason FROM locality_source_schema_audit "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone(),
            ("operator-1", "approve late-created canonical source tables"),
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "guarded session"):
            self.proc_conn.execute(
                "UPDATE cnstwk_cntrct SET thtmCntrctAmt=999"
            )

    def test_source_schema_refresh_replacement_rolls_back_atomically(self):
        historical = self.complete_source_baseline()
        generation = read_data_generation(self.proc_conn)
        audit_count = self.proc_conn.execute(
            "SELECT COUNT(*) FROM locality_source_schema_audit"
        ).fetchone()[0]
        original_sql = self.proc_conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='table' AND name='cnstwk_cntrct'"
        ).fetchone()[0]

        def broken_replacement(conn):
            conn.execute("DROP TABLE cnstwk_cntrct")
            conn.execute(original_sql)
            raise RuntimeError("replacement failed")

        with self.assertRaisesRegex(RuntimeError, "replacement failed"):
            refresh_source_schema(
                self.proc_conn,
                operator="operator-2",
                reason="replace construction source",
                replace=broken_replacement,
            )

        self.assertEqual(read_data_generation(self.proc_conn), generation)
        self.assertEqual(
            self.proc_conn.execute(
                "SELECT COUNT(*) FROM locality_source_schema_audit"
            ).fetchone()[0],
            audit_count,
        )
        self.assertEqual(
            self.proc_conn.execute(
                "SELECT thtmCntrctAmt FROM cnstwk_cntrct"
            ).fetchone()[0],
            historical.amount_won,
        )
        self.assertTrue(
            self.resolver().resolve(historical, "1234567890", 100.0, True)
        )

    def test_source_schema_plan_rejects_escaped_rename_destination_atomically(self):
        self.complete_source_baseline()
        before = self.source_refresh_state()
        original_sql = self.proc_conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='table' AND name='cnstwk_cntrct'"
        ).fetchone()[0]
        original_row = self.proc_conn.execute(
            "SELECT * FROM cnstwk_cntrct"
        ).fetchone()

        def escaped_replacement(plan):
            plan.execute(
                "ALTER TABLE cnstwk_cntrct RENAME TO escaped_source_copy"
            )
            plan.execute(original_sql)
            plan.execute(
                "INSERT INTO cnstwk_cntrct "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                original_row,
            )

        with self.assertRaisesRegex(RuntimeError, "destination is not approved"):
            refresh_source_schema(
                self.proc_conn,
                operator="operator-escaped-rename",
                reason="reject unapproved rename residue",
                replace=escaped_replacement,
            )

        self.assertEqual(self.source_refresh_state(), before)
        self.assertIsNone(
            self.proc_conn.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type='table' AND name='escaped_source_copy'"
            ).fetchone()
        )

    def test_source_schema_refresh_rejects_indirect_view_schema_rewrite(self):
        create_contract_source_tables(self.proc_conn)
        self.proc_conn.execute(
            "CREATE VIEW construction_amounts AS "
            "SELECT thtmCntrctAmt FROM cnstwk_cntrct"
        )
        self.complete_source_baseline()
        before = self.source_refresh_state()
        original_sql = self.proc_conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='table' AND name='cnstwk_cntrct'"
        ).fetchone()[0]
        original_row = self.proc_conn.execute(
            "SELECT * FROM cnstwk_cntrct"
        ).fetchone()

        def view_rewriting_plan(plan):
            plan.execute(
                "ALTER TABLE cnstwk_cntrct RENAME TO busan_award_servc"
            )
            plan.execute(original_sql)
            plan.execute(
                "INSERT INTO cnstwk_cntrct "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                original_row,
            )

        with self.assertRaisesRegex(RuntimeError, "schema effect: view"):
            refresh_source_schema(
                self.proc_conn,
                operator="operator-view-rewrite",
                reason="reject indirect unrelated schema rewrite",
                replace=view_rewriting_plan,
            )

        self.assertEqual(self.source_refresh_state(), before)

    def test_source_schema_refresh_rejects_extra_source_trigger_before_plan(self):
        self.complete_source_baseline()
        self.proc_conn.execute(
            "CREATE TRIGGER source_clock_escape AFTER UPDATE ON cnstwk_cntrct "
            "BEGIN UPDATE locality_generation_clock "
            "SET data_generation=data_generation+100 WHERE singleton_id=1; END"
        )
        self.proc_conn.commit()
        before = self.source_refresh_state()

        with self.assertRaisesRegex(RuntimeError, "trigger surface"):
            refresh_source_schema(
                self.proc_conn,
                operator="operator-extra-trigger",
                reason="reject indirect source side effects",
                replace=lambda plan: plan.execute(
                    "UPDATE cnstwk_cntrct SET thtmCntrctAmt=707"
                ),
            )

        self.assertEqual(self.source_refresh_state(), before)

    def test_source_schema_plan_creates_approved_evidence_index_and_pins_it(self):
        self.proc_conn.execute(
            "CREATE TABLE busan_award_servc "
            "(bidwinnrBizno TEXT, bidNtceNo TEXT)"
        )
        self.complete_source_baseline()
        generation = read_data_generation(self.proc_conn)
        control_revision = read_control_revision(self.proc_conn)
        table_signature = self.proc_conn.execute(
            "SELECT table_signature FROM locality_source_schema_state "
            "WHERE singleton_id=1"
        ).fetchone()[0]

        refresh_source_schema(
            self.proc_conn,
            operator="operator-award-index",
            reason="install deployed award lookup index",
            replace=lambda plan: plan.execute(
                "CREATE INDEX IF NOT EXISTS idx_award_bizno "
                "ON busan_award_servc (bidwinnrBizno)"
            ),
        )

        self.assertEqual(
            self.proc_conn.execute(
                "SELECT tbl_name FROM sqlite_master "
                "WHERE type='index' AND name='idx_award_bizno'"
            ).fetchone(),
            ("busan_award_servc",),
        )
        self.assertNotEqual(
            self.proc_conn.execute(
                "SELECT table_signature FROM locality_source_schema_state "
                "WHERE singleton_id=1"
            ).fetchone()[0],
            table_signature,
        )
        self.assertEqual(read_data_generation(self.proc_conn), generation + 1)
        self.assertEqual(read_control_revision(self.proc_conn), control_revision + 2)

    def test_source_schema_plan_rejects_index_on_missing_column_without_residue(self):
        self.proc_conn.execute(
            "CREATE TABLE busan_award_servc "
            "(bidwinnrBizno TEXT, bidNtceNo TEXT)"
        )
        self.complete_source_baseline()
        before = self.source_refresh_state()

        with self.assertRaisesRegex(RuntimeError, "index column is not approved"):
            refresh_source_schema(
                self.proc_conn,
                operator="operator-bad-index-column",
                reason="reject an index expression outside the owner schema",
                replace=lambda plan: plan.execute(
                    "CREATE INDEX idx_award_bad "
                    "ON busan_award_servc (missing_column)"
                ),
            )

        self.assertEqual(self.source_refresh_state(), before)
        self.assertIsNone(
            self.proc_conn.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type='index' AND name='idx_award_bad'"
            ).fetchone()
        )

    def test_source_schema_plan_rejects_index_on_unapproved_owner(self):
        self.proc_conn.execute(
            "CREATE TABLE outside_source (data_generation INTEGER)"
        )
        self.complete_source_baseline()
        before = self.source_refresh_state()

        with self.assertRaisesRegex(RuntimeError, "target is not approved"):
            refresh_source_schema(
                self.proc_conn,
                operator="operator-bad-index-owner",
                reason="reject index outside source ownership",
                replace=lambda plan: plan.execute(
                    "CREATE INDEX idx_control_escape "
                    "ON outside_source (data_generation)"
                ),
            )

        self.assertEqual(self.source_refresh_state(), before)

    def test_source_schema_editor_rejects_commit_rollback_and_transaction_sql(self):
        self.complete_source_baseline()
        forbidden = (
            "COMMIT",
            "ROLLBACK",
            "BEGIN IMMEDIATE",
            "SAVEPOINT callback_scope",
            "RELEASE callback_scope",
            "PRAGMA journal_mode=WAL",
            "ATTACH DATABASE ':memory:' AS escaped",
            "DETACH DATABASE escaped",
            "VACUUM",
            "UPDATE locality_generation_clock SET data_generation=999",
            "DELETE FROM locality_source_schema_audit",
            "SELECT * FROM cnstwk_cntrct",
            "WITH source AS (SELECT 1) UPDATE cnstwk_cntrct SET thtmCntrctAmt=999",
            "CREATE TABLE attacker (id INTEGER)",
            "CREATE TRIGGER attacker AFTER UPDATE ON cnstwk_cntrct BEGIN SELECT 1; END",
            "UPDATE cnstwk_cntrct SET thtmCntrctAmt=999; COMMIT",
        )
        for statement in forbidden:
            with self.subTest(statement=statement):
                before = self.source_refresh_state()

                def transaction_attempt(editor):
                    editor.execute(
                        "UPDATE cnstwk_cntrct SET thtmCntrctAmt=999"
                    )
                    editor.execute(statement)

                with self.assertRaisesRegex(
                    RuntimeError,
                    "rejects transaction control|unsafe SQL|operation kind|target is not approved",
                ):
                    refresh_source_schema(
                        self.proc_conn,
                        operator="operator-adversarial",
                        reason="reject transaction-changing SQL",
                        replace=transaction_attempt,
                    )
                self.assertEqual(self.source_refresh_state(), before)

        for method_name in ("commit", "rollback"):
            with self.subTest(method=method_name):
                before = self.source_refresh_state()

                def method_attempt(editor):
                    self.assertNotIsInstance(editor, sqlite3.Connection)
                    for forbidden_attribute in (
                        "connection",
                        "cursor",
                        "commit",
                        "rollback",
                        "executescript",
                        "__enter__",
                        "__exit__",
                    ):
                        self.assertFalse(hasattr(editor, forbidden_attribute))
                    editor.execute(
                        "UPDATE cnstwk_cntrct SET thtmCntrctAmt=999"
                    )
                    getattr(editor, method_name)()

                with self.assertRaises(AttributeError):
                    refresh_source_schema(
                        self.proc_conn,
                        operator="operator-adversarial",
                        reason="reject connection transaction method",
                        replace=method_attempt,
                    )
                self.assertEqual(self.source_refresh_state(), before)

    def test_source_schema_plan_exposes_no_live_connection_or_cursor_surface(self):
        self.complete_source_baseline()
        retained = []
        mutable_row = [707]

        def replacement(plan):
            retained.append(plan)
            self.assertIsNone(
                plan.executemany(
                    "UPDATE cnstwk_cntrct SET thtmCntrctAmt=?",
                    (mutable_row,),
                )
            )
            mutable_row[0] = 999

        refresh_source_schema(
            self.proc_conn,
            operator="operator-plan-surface",
            reason="prove declarative plan isolation",
            replace=replacement,
        )

        plan = retained[0]
        self.assertEqual(
            self.proc_conn.execute(
                "SELECT thtmCntrctAmt FROM cnstwk_cntrct"
            ).fetchone()[0],
            707,
        )
        self.assertFalse(hasattr(locality_snapshot_module, "_SOURCE_EDITOR_CONNECTIONS"))
        self.assertFalse(hasattr(locality_snapshot_module, "_source_editor_connection"))
        self.assertFalse(hasattr(plan, "__dict__"))
        for forbidden_attribute in (
            "connection",
            "cursor",
            "commit",
            "rollback",
            "executescript",
            "__enter__",
            "__exit__",
        ):
            self.assertFalse(hasattr(plan, forbidden_attribute))
        with self.assertRaisesRegex(RuntimeError, "already sealed"):
            plan.execute("UPDATE cnstwk_cntrct SET thtmCntrctAmt=808")

    def test_source_schema_plan_rejects_mutable_parameter_values(self):
        self.complete_source_baseline()

        def replacement(plan):
            plan.execute(
                "UPDATE cnstwk_cntrct SET thtmCntrctAmt=?",
                ([999],),
            )

        with self.assertRaisesRegex(RuntimeError, "immutable SQLite values"):
            refresh_source_schema(
                self.proc_conn,
                operator="operator-plan-parameters",
                reason="reject mutable replacement parameters",
                replace=replacement,
            )

    def test_source_schema_refresh_preserves_existing_authorizer_after_success(self):
        self.complete_source_baseline()
        self.proc_conn.execute("CREATE TABLE authorizer_probe (id INTEGER)")
        self.proc_conn.execute("INSERT INTO authorizer_probe VALUES (1)")
        self.proc_conn.commit()
        denials = []

        def authorizer(action, argument_one, argument_two, database, trigger):
            if action == sqlite3.SQLITE_DELETE and argument_one == "authorizer_probe":
                denials.append((action, argument_one, argument_two, database, trigger))
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        self.proc_conn.set_authorizer(authorizer)
        try:
            refresh_source_schema(
                self.proc_conn,
                operator="operator-authorizer-success",
                reason="preserve caller authorizer on success",
                replace=lambda plan: plan.execute(
                    "UPDATE cnstwk_cntrct SET thtmCntrctAmt=707"
                ),
            )
            with self.assertRaises(sqlite3.DatabaseError):
                self.proc_conn.execute("DELETE FROM authorizer_probe")
            self.assertEqual(len(denials), 1)
        finally:
            self.proc_conn.set_authorizer(None)

    def test_source_schema_refresh_preserves_existing_authorizer_after_failure(self):
        self.complete_source_baseline()
        before = self.source_refresh_state()
        denials = []

        def authorizer(action, argument_one, argument_two, database, trigger):
            if action == sqlite3.SQLITE_UPDATE and argument_one == "cnstwk_cntrct":
                denials.append((action, argument_one, argument_two, database, trigger))
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        self.proc_conn.set_authorizer(authorizer)
        try:
            with self.assertRaises(sqlite3.DatabaseError):
                refresh_source_schema(
                    self.proc_conn,
                    operator="operator-authorizer-failure",
                    reason="preserve caller authorizer on failure",
                    replace=lambda plan: plan.execute(
                        "UPDATE cnstwk_cntrct SET thtmCntrctAmt=707"
                    ),
                )
            self.assertEqual(self.source_refresh_state(), before)
            with self.assertRaises(sqlite3.DatabaseError):
                self.proc_conn.execute(
                    "UPDATE cnstwk_cntrct SET thtmCntrctAmt=808"
                )
            self.assertEqual(len(denials), 2)
        finally:
            self.proc_conn.set_authorizer(None)

    def test_source_schema_refresh_failure_boundaries_restore_exact_prior_state(self):
        self.complete_source_baseline()
        stages = (
            "after_replacement",
            "after_trigger_reinstall",
            "after_state_update",
            "after_audit_insert",
            "before_return",
        )
        for stage in stages:
            with self.subTest(stage=stage):
                before = self.source_refresh_state()

                def replacement(editor):
                    editor.execute(
                        "UPDATE cnstwk_cntrct SET thtmCntrctAmt=707"
                    )

                with self.assertRaisesRegex(
                    RuntimeError, "injected source schema refresh failure"
                ):
                    refresh_source_schema(
                        self.proc_conn,
                        operator="operator-failure-injection",
                        reason=f"exercise {stage}",
                        replace=replacement,
                        fail_after=stage,
                    )
                self.assertEqual(self.source_refresh_state(), before)

    def test_source_schema_refresh_replaces_and_reprotects_atomically(self):
        historical = self.complete_source_baseline()
        resolver = self.resolver()
        self.assertTrue(
            resolver.resolve(historical, "1234567890", 100.0, True)
        )
        generation = read_data_generation(self.proc_conn)
        original_sql = self.proc_conn.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='table' AND name='cnstwk_cntrct'"
        ).fetchone()[0]
        replacement_row = list(
            self.proc_conn.execute("SELECT * FROM cnstwk_cntrct").fetchone()
        )
        replacement_row[5] = 707

        def approved_replacement(conn):
            conn.execute("DROP TABLE cnstwk_cntrct")
            conn.execute(original_sql)
            conn.execute(
                "INSERT INTO cnstwk_cntrct VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                replacement_row,
            )

        refresh_source_schema(
            self.proc_conn,
            operator="operator-3",
            reason="approved construction source replacement",
            replace=approved_replacement,
        )

        self.assertEqual(read_data_generation(self.proc_conn), generation + 1)
        self.assertEqual(
            self.proc_conn.execute(
                "SELECT operator, reason FROM locality_source_schema_audit "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone(),
            ("operator-3", "approved construction source replacement"),
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "guarded session"):
            self.proc_conn.execute(
                "UPDATE cnstwk_cntrct SET thtmCntrctAmt=808"
            )
        self.proc_conn.rollback()
        with self.assertRaisesRegex(
            MissingHistoricalSnapshot, "source schema|generation|drift"
        ):
            resolver.resolve(historical, "1234567890", 100.0, True)

    def test_direct_canonical_source_write_is_fenced(self):
        self.complete_source_baseline()
        ensure_snapshot_schema(self.proc_conn)

        with self.assertRaisesRegex(sqlite3.IntegrityError, "guarded session"):
            self.proc_conn.execute(
                "UPDATE cnstwk_cntrct SET thtmCntrctAmt=999"
            )

    def test_rolled_back_canonical_source_write_does_not_advance_generation(self):
        self.complete_source_baseline()
        ensure_snapshot_schema(self.proc_conn)
        before = read_data_generation(self.proc_conn)

        with self.assertRaisesRegex(RuntimeError, "rollback"):
            with guarded_write_session(self.proc_conn):
                self.proc_conn.execute(
                    "UPDATE cnstwk_cntrct SET thtmCntrctAmt=999"
                )
                raise RuntimeError("rollback")

        self.assertEqual(read_data_generation(self.proc_conn), before)
        self.assertEqual(
            self.proc_conn.execute(
                "SELECT thtmCntrctAmt FROM cnstwk_cntrct"
            ).fetchone()[0],
            101,
        )

    def test_present_evidence_table_is_guarded_and_data_clocked(self):
        self.proc_conn.execute(
            "CREATE TABLE bid_notices_raw (bidNtceNo TEXT PRIMARY KEY, rgnLmtInfo TEXT)"
        )
        ensure_snapshot_schema(self.proc_conn)
        before = read_data_generation(self.proc_conn)

        with self.assertRaisesRegex(sqlite3.IntegrityError, "guarded session"):
            self.proc_conn.execute(
                "INSERT INTO bid_notices_raw VALUES ('N-1', 'Busan')"
            )
        with guarded_write_session(self.proc_conn):
            self.proc_conn.execute(
                "INSERT INTO bid_notices_raw VALUES ('N-1', 'Busan')"
            )

        self.assertEqual(read_data_generation(self.proc_conn), before + 1)

    def test_compact_kst_cutover_is_equivalent_to_offset_timestamp(self):
        at_cutover = contract("CUTOVER1", "00", "2026-08-16T10:15:00+09:00")
        resolver = SnapshotResolver(
            self.proc_conn,
            self.company_conn,
            mode="snapshot",
            now=NOW,
            cutover_at="202608161015",
            generation_id="generation-1",
            eligible_agency_codes=ELIGIBLE_AGENCY_CODES,
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
        self.proc_conn.commit()

        with self.assertRaisesRegex(MissingHistoricalSnapshot, "complete"):
            self.resolver().resolve(self.old_contract, "1234567890", 100.0, True)

    def test_snapshot_rejects_complete_manifest_that_does_not_own_row_membership(self):
        create_contract_source_tables(self.proc_conn)
        insert_required_sector_source(self.proc_conn)
        self.proc_conn.commit()
        refresh_source_schema(
            self.proc_conn,
            operator="test-suite",
            reason="prepare membership rejection source surface",
        )
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
        self.proc_conn.commit()

        with self.assertRaisesRegex(
            MissingHistoricalSnapshot, "member|ownership|coverage|verification|evidence"
        ):
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
            eligible_agency_codes=ELIGIBLE_AGENCY_CODES,
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

    def test_legacy_mode_returns_without_reading_caller_transaction(self):
        resolver = self.resolver(mode="legacy")
        self.proc_conn.execute("BEGIN")

        self.assertTrue(
            resolver.resolve(self.new_contract, "1234567890", 100.0, True)
        )
        self.proc_conn.rollback()

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
        with guarded_write_session(self.proc_conn):
            self.proc_conn.execute(
                "UPDATE cnstwk_cntrct SET thtmCntrctAmt=999"
            )

        coverage = verify_baseline_manifest(self.proc_conn, BASELINE_ID)

        self.assertFalse(coverage.complete)

    def test_complete_baseline_cannot_be_rebuilt_with_the_same_id(self):
        rows, manifest = self.build_source_baseline()
        self.assertEqual(manifest.status, "complete")

        with self.assertRaisesRegex(sqlite3.IntegrityError, "complete baseline"):
            create_baseline_manifest(self.proc_conn, rows, BASELINE_ID)

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
        with guarded_write_session(self.proc_conn):
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
        with guarded_write_session(self.proc_conn):
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
                with guarded_write_session(self.proc_conn):
                    self.proc_conn.execute(
                        f"UPDATE cnstwk_cntrct SET {column}=?", (changed,)
                    )
                self.assertFalse(
                    verify_baseline_manifest(self.proc_conn, BASELINE_ID).complete
                )
                with guarded_write_session(self.proc_conn):
                    self.proc_conn.execute(
                        f"UPDATE cnstwk_cntrct SET {column}=?", (original,)
                    )
                self.assertTrue(
                    verify_baseline_manifest(self.proc_conn, BASELINE_ID).complete
                )

    def test_manifest_verification_rejects_new_source_identity(self):
        self.build_source_baseline()
        with guarded_write_session(self.proc_conn):
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
        with guarded_write_session(self.proc_conn):
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
    def test_shadow_and_snapshot_resolvers_require_eligible_agency_codes(self):
        proc_conn = sqlite3.connect(":memory:")
        company_conn = sqlite3.connect(":memory:")
        try:
            for mode in ("shadow", "snapshot"):
                with self.subTest(mode=mode):
                    with self.assertRaisesRegex(ValueError, "eligible agency"):
                        SnapshotResolver(
                            proc_conn,
                            company_conn,
                            mode=mode,
                            cutover_at=CUTOVER,
                        )
            legacy = SnapshotResolver(
                proc_conn,
                company_conn,
                mode="legacy",
                cutover_at=CUTOVER,
            )
            self.assertFalse(legacy.resolve({}, "1234567890", 100.0, False))
        finally:
            proc_conn.close()
            company_conn.close()

    def test_integrated_resolver_persists_fingerprint_for_calculated_agency(self):
        case = SnapshotTestCase(methodName="runTest")
        case.setUp()
        try:
            source = {
                "untyCntrctNo": "U-1",
                "dcsnCntrctNo": "DEC-000100",
                "dminsttCd": "UNKNOWN",
                "dminsttList": "[1^VALID^Agency^x]",
                "cntrctInsttCd": "FALLBACK",
                "thtmCntrctAmt": 100,
                "totCntrctAmt": 0,
                "corpList": corp_entry("1234567890", "100"),
                "cntrctCnclsDate": "2026-08-16T11:00:00+09:00",
            }
            agencies = {"VALID": {"cate_lrg": "group"}}
            resolver = case.resolver(
                mode="shadow", eligible_agency_codes=agencies
            )

            result = process_contract_row(
                source,
                agencies,
                {"1234567890"},
                locality_resolver=resolver,
                sector="용역",
            )
            resolver.flush()
            stored = case.proc_conn.execute(
                "SELECT content_fingerprint FROM contract_supplier_locality"
            ).fetchone()[0]
            canonical = canonical_contract_from_row(
                source,
                "용역",
                eligible_agency_codes=agencies,
            )

            self.assertEqual(result[0], "VALID")
            self.assertEqual(canonical.agency, "VALID")
            self.assertEqual(stored, content_fingerprint(canonical))
        finally:
            case.tearDown()

    def test_integrated_resolver_rejects_mismatched_eligible_agency_set(self):
        case = SnapshotTestCase(methodName="runTest")
        case.setUp()
        try:
            source = {
                "untyCntrctNo": "U-1",
                "dcsnCntrctNo": "DEC-000100",
                "dminsttCd": "UNKNOWN",
                "dminsttList": "[1^VALID^Agency^x]",
                "thtmCntrctAmt": 100,
                "corpList": corp_entry("1234567890", "100"),
                "cntrctCnclsDate": "2026-08-16T11:00:00+09:00",
            }
            agencies = {"VALID": {"cate_lrg": "group"}}
            resolver = case.resolver(
                mode="shadow", eligible_agency_codes={"UNKNOWN"}
            )

            with self.assertRaisesRegex(SnapshotContentMismatch, "eligible agency"):
                process_contract_row(
                    source,
                    agencies,
                    {"1234567890"},
                    locality_resolver=resolver,
                    sector="용역",
                )

            self.assertEqual(resolver.flush(), 0)
        finally:
            case.tearDown()

    def test_resolver_freezes_the_exact_eligible_agency_set(self):
        case = SnapshotTestCase(methodName="runTest")
        case.setUp()
        try:
            eligible = {"A1"}
            resolver = case.resolver(
                mode="shadow", eligible_agency_codes=eligible
            )
            eligible.add("A2")

            resolver.require_eligible_agency_codes({"A1"})
            with self.assertRaisesRegex(SnapshotContentMismatch, "eligible agency"):
                resolver.require_eligible_agency_codes(eligible)
        finally:
            case.tearDown()

    def test_resolver_rejects_prebuilt_contract_with_ineligible_agency(self):
        case = SnapshotTestCase(methodName="runTest")
        case.setUp()
        try:
            resolver = case.resolver(
                mode="shadow", eligible_agency_codes={"VALID"}
            )
            ineligible = CanonicalContract(
                "용역",
                "DEC0001",
                "00",
                "UNKNOWN",
                100,
                "2026-08-16T11:00:00+09:00",
                (CanonicalSupplier("1234567890", 100.0),),
            )

            with self.assertRaisesRegex(SnapshotContentMismatch, "eligible agency"):
                resolver.resolve(ineligible, "1234567890", 100.0, True)

            self.assertEqual(resolver.flush(), 0)
        finally:
            case.tearDown()

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
