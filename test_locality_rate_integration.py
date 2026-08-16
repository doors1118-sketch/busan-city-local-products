import ast
from dataclasses import FrozenInstanceError
import importlib.util
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from contract_population import (
    CanonicalContractCollision,
    MissingAgencyIdentity,
    MissingSupplierIdentity,
)
from company_locality import apply_company_changes, ensure_locality_schema
from core_calc import process_contract_row
from locality_snapshot import MissingHistoricalSnapshot, ensure_snapshot_schema
from maintenance_lock import LocalityPaths, configure_locality_paths


CUTOVER = "2026-08-16 10:00:00+09:00"
SECTORS = ("공사", "용역", "물품", "쇼핑몰")


class CountingEnvironment(dict):
    def __init__(self, values):
        super().__init__(values)
        self.reads = {}

    def get(self, key, default=None):
        self.reads[key] = self.reads.get(key, 0) + 1
        return super().get(key, default)


class LocalityRateIntegrationTests(unittest.TestCase):
    def configure_test_paths(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        paths = LocalityPaths.for_in_memory_tests(
            root / "maintenance.lock",
            root / "transition.json",
            root / "marker",
            root / "pointer.json",
        )
        paths.pointer_path.write_text('{"active_generation_id":null}', encoding="ascii")
        configure_locality_paths(paths)
        return paths

    @staticmethod
    def corp_entry(bizno, share):
        return f"[a^b^c^Supplier^e^f^{share}^h^i^{bizno}]"

    def test_shared_rate_resolver_module_exists(self):
        self.assertIsNotNone(
            importlib.util.find_spec("locality_rate_resolver"),
            "Task 5A requires the shared rate resolver factory",
        )

    def test_mode_is_read_once_and_only_exact_modes_are_accepted(self):
        from locality_rate_resolver import read_locality_config

        environment = CountingEnvironment(
            {
                "LOCALITY_MODE": "shadow",
                "LOCALITY_CUTOVER_AT": CUTOVER,
                "LOCALITY_GENERATION_ID": "generation-5a",
                "LOCALITY_BASELINE_ID": "baseline-v1",
            }
        )
        config = read_locality_config(environment)
        self.assertEqual(config.mode, "shadow")
        self.assertEqual(environment.reads["LOCALITY_MODE"], 1)

        for invalid in ("", "SHADOW", "current", "snapshot "):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(ValueError, "legacy, shadow, or snapshot"):
                    read_locality_config(
                        CountingEnvironment(
                            {
                                "LOCALITY_MODE": invalid,
                                "LOCALITY_CUTOVER_AT": CUTOVER,
                                "LOCALITY_GENERATION_ID": "generation-5a",
                            }
                        )
                    )

    def test_four_sector_resolvers_share_one_generation_context(self):
        from locality_rate_resolver import (
            LocalityRateConfig,
            build_locality_resolvers,
        )

        procurement = sqlite3.connect(":memory:")
        company = sqlite3.connect(":memory:")
        self.addCleanup(procurement.close)
        self.addCleanup(company.close)
        config = LocalityRateConfig(
            mode="legacy",
            cutover_at=CUTOVER,
            generation_id="generation-5a",
            baseline_id=None,
        )

        resolvers = build_locality_resolvers(
            procurement,
            company,
            config,
            eligible_agency_codes={"A1"},
        )

        self.assertEqual(tuple(resolvers), SECTORS)
        for sector in SECTORS:
            resolver = resolvers[sector]
            self.assertEqual(resolver.sector, sector)
            self.assertIs(resolver.procurement_conn, procurement)
            self.assertIs(resolver.company_conn, company)
            self.assertEqual(resolver.generation_id, "generation-5a")
            self.assertEqual(resolver.cutover_at, CUTOVER)

    def test_shared_canonical_selector_keeps_distinct_decision_families(self):
        from core_calc import select_canonical_contract_rows

        rows = pd.DataFrame(
            [
                self.contract_row("REUSED", "FIRST000100", 100, "first"),
                self.contract_row("REUSED", "SECOND00100", 200, "second"),
                self.contract_row("", "THIRD000100", 300, "third"),
                self.contract_row("", "FOURTH00100", 400, "fourth"),
            ]
        )

        selected = select_canonical_contract_rows(
            rows,
            "공사",
            eligible_agency_codes={"A1"},
        )

        self.assertEqual(selected["display_name"].tolist(), ["first", "second", "third", "fourth"])
        self.assertEqual(selected["thtmCntrctAmt"].tolist(), [100, 200, 300, 400])

    def test_shared_canonical_selector_uses_latest_revision_and_preserves_metadata(self):
        from core_calc import select_canonical_contract_rows

        rows = pd.DataFrame(
            [
                self.contract_row(
                    "U-OLD", "REVISION0100", 100, "old display",
                    updated="2026-08-14 09:00:00",
                ),
                self.contract_row(
                    "U-NEW", "REVISION0101", 120, "latest display",
                    updated="2026-08-15 09:00:00",
                ),
            ]
        )

        selected = select_canonical_contract_rows(
            rows,
            "공사",
            eligible_agency_codes={"A1"},
        )

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected.iloc[0]["display_name"], "latest display")
        self.assertEqual(selected.iloc[0]["untyCntrctNo"], "U-NEW")

    def test_shared_canonical_selector_rejects_divergent_exact_identity(self):
        from core_calc import select_canonical_contract_rows

        rows = pd.DataFrame(
            [
                self.contract_row("U-1", "COLLIDE00100", 100, "first"),
                self.contract_row("U-2", "COLLIDE00100", 101, "second"),
            ]
        )

        with self.assertRaises(CanonicalContractCollision):
            select_canonical_contract_rows(
                rows,
                "공사",
                eligible_agency_codes={"A1"},
            )

    def test_resolver_set_rejects_every_mixed_child_context_dimension(self):
        from locality_rate_resolver import (
            LocalityRateConfig,
            LocalityResolverSet,
            build_locality_resolver,
            build_locality_resolvers,
        )

        procurement = sqlite3.connect(":memory:")
        other_procurement = sqlite3.connect(":memory:")
        company = sqlite3.connect(":memory:")
        other_company = sqlite3.connect(":memory:")
        for connection in (procurement, other_procurement, company, other_company):
            self.addCleanup(connection.close)
        config = LocalityRateConfig("legacy", CUTOVER, "generation-good", "baseline-good")
        good = build_locality_resolvers(
            procurement,
            company,
            config,
            eligible_agency_codes={"A1"},
        )

        cases = {
            "sector": {"sector": "용역"},
            "mode": {"mode": "shadow"},
            "cutover": {"cutover_at": "2026-08-17 10:00:00+09:00"},
            "generation": {"generation_id": "generation-other"},
            "baseline": {"selected_baseline_id": "baseline-other"},
            "procurement connection": {"procurement_conn": other_procurement},
            "company connection": {"company_conn": other_company},
            "read-only": {"read_only": True},
        }
        defaults = {
            "procurement_conn": procurement,
            "company_conn": company,
            "mode": config.mode,
            "sector": "물품",
            "cutover_at": config.cutover_at,
            "generation_id": config.generation_id,
            "selected_baseline_id": config.baseline_id,
            "eligible_agency_codes": {"A1"},
            "read_only": False,
        }
        for label, override in cases.items():
            with self.subTest(label=label):
                child = build_locality_resolver(**(defaults | override))
                mixed = dict(good)
                mixed["물품"] = child
                with self.assertRaisesRegex(ValueError, label):
                    LocalityResolverSet(config, mixed)

    def test_resolver_set_exposes_immutable_pinned_shared_context(self):
        from locality_rate_resolver import LocalityRateConfig, build_locality_resolvers

        procurement = sqlite3.connect(":memory:")
        company = sqlite3.connect(":memory:")
        self.addCleanup(procurement.close)
        self.addCleanup(company.close)
        config = LocalityRateConfig("legacy", CUTOVER, "generation-5a", None)
        resolvers = build_locality_resolvers(
            procurement,
            company,
            config,
            eligible_agency_codes={"A1"},
            read_only=True,
        )

        context = resolvers.shared_context
        self.assertIs(context.procurement_conn, procurement)
        self.assertIs(context.company_conn, company)
        self.assertTrue(context.read_only)
        self.assertEqual(context.generation_id, config.generation_id)
        with self.assertRaises(FrozenInstanceError):
            context.generation_id = "changed"
        with self.assertRaises(TypeError):
            resolvers._resolvers["공사"] = resolvers["용역"]
        with self.assertRaises(AttributeError):
            resolvers["공사"].generation_id = "changed"
        with self.assertRaises(AttributeError):
            resolvers["공사"]._cutover = None
        with self.assertRaises(AttributeError):
            resolvers.config = LocalityRateConfig("legacy", CUTOVER, "changed", None)

    def test_resolver_factory_closes_both_connections_when_initialization_fails(self):
        from locality_rate_resolver import LocalityRateConfig, open_locality_resolvers

        procurement = Mock()
        company = Mock()
        config = LocalityRateConfig("legacy", CUTOVER, "legacy", None)
        with patch(
            "locality_rate_resolver._connect",
            side_effect=[procurement, company],
        ):
            with self.assertRaisesRegex(ValueError, "eligible agency"):
                with open_locality_resolvers(
                    "procurement.db",
                    "company.db",
                    config,
                    eligible_agency_codes=set(),
                ):
                    self.fail("resolver initialization unexpectedly succeeded")

        procurement.close.assert_called_once_with()
        company.close.assert_called_once_with()

    def test_read_only_resolver_rejects_candidate_staging(self):
        from locality_rate_resolver import (
            LocalityRateConfig,
            build_locality_resolvers,
        )

        procurement = sqlite3.connect(":memory:")
        company = sqlite3.connect(":memory:")
        self.addCleanup(procurement.close)
        self.addCleanup(company.close)
        self.configure_test_paths()
        ensure_snapshot_schema(procurement)
        config = LocalityRateConfig(
            mode="shadow",
            cutover_at=CUTOVER,
            generation_id="generation-5a",
            baseline_id=None,
        )
        resolver = build_locality_resolvers(
            procurement,
            company,
            config,
            eligible_agency_codes={"A1"},
            read_only=True,
        )["공사"]
        row = {
            "sector": "공사",
            "dcsnCntrctNo": "CONST-100",
            "dminsttCd": "A1",
            "thtmCntrctAmt": 100,
            "corpList": "[a^b^c^Supplier^e^f^100^h^i^1234567890]",
            "cntrctCnclsDate": "2026-08-15",
        }

        with self.assertRaisesRegex(MissingHistoricalSnapshot, "read-only"):
            resolver.resolve(row, "1234567890", 100.0, True)
        with self.assertRaisesRegex(MissingHistoricalSnapshot, "read-only"):
            resolver.flush()

    def test_malformed_supplier_list_fails_before_rate_calculation(self):
        from locality_rate_resolver import LocalityRateConfig, build_locality_resolvers

        procurement = sqlite3.connect(":memory:")
        company = sqlite3.connect(":memory:")
        self.addCleanup(procurement.close)
        self.addCleanup(company.close)
        config = LocalityRateConfig(
            mode="legacy",
            cutover_at=CUTOVER,
            generation_id="legacy",
            baseline_id=None,
        )
        resolver = build_locality_resolvers(
            procurement,
            company,
            config,
            eligible_agency_codes={"A1"},
        )["공사"]
        malformed = {
            "dcsnCntrctNo": "CONST-100",
            "dminsttCd": "A1",
            "thtmCntrctAmt": 100,
            "corpList": "not-a-supplier-list",
            "cntrctCnclsDate": "2026-08-15",
        }
        agencies = {"A1": {"cate_lrg": "부산광역시 및 소속기관"}}

        with self.assertRaises(MissingSupplierIdentity):
            process_contract_row(
                malformed,
                agencies,
                set(),
                sector="공사",
                locality_resolver=resolver,
            )

    def test_missing_required_agency_fails_instead_of_skipping(self):
        from locality_rate_resolver import LocalityRateConfig, build_locality_resolvers

        procurement = sqlite3.connect(":memory:")
        company = sqlite3.connect(":memory:")
        self.addCleanup(procurement.close)
        self.addCleanup(company.close)
        resolver = build_locality_resolvers(
            procurement,
            company,
            LocalityRateConfig("legacy", CUTOVER, "legacy", None),
            eligible_agency_codes={"A1"},
        )["용역"]
        row = {
            "dcsnCntrctNo": "SERVC000100",
            "thtmCntrctAmt": 100,
            "corpList": self.corp_entry("1234567890", 100),
            "cntrctCnclsDate": "2026-08-15",
        }

        with self.assertRaises(MissingAgencyIdentity):
            process_contract_row(
                row,
                {"A1": {"cate_lrg": "group"}},
                {"1234567890"},
                sector="용역",
                locality_resolver=resolver,
            )

    def test_legacy_and_shadow_match_for_all_four_canonical_sectors(self):
        from locality_rate_resolver import LocalityRateConfig, build_locality_resolvers

        self.configure_test_paths()
        procurement = sqlite3.connect(":memory:")
        company = sqlite3.connect(":memory:")
        self.addCleanup(procurement.close)
        self.addCleanup(company.close)
        ensure_snapshot_schema(procurement)
        agencies = {"A1": {"cate_lrg": "부산광역시 및 소속기관"}}
        local = "1234567890"
        outside = "9999999999"
        rows = {
            "공사": {
                "dcsnCntrctNo": "CONST000101",
                "dminsttCd": "A1",
                "thtmCntrctAmt": 100,
                "corpList": self.corp_entry(local, 25) + self.corp_entry(outside, 75),
                "cntrctCnclsDate": "2026-08-15",
            },
            "용역": {
                "dcsnCntrctNo": "SERVC000100",
                "dminsttCd": "A1",
                "thtmCntrctAmt": 200,
                "corpList": self.corp_entry(local, 100),
                "cntrctCnclsDate": "2026-08-15",
            },
            "물품": {
                "dcsnCntrctNo": "GOODS000100",
                "dminsttCd": "A1",
                "thtmCntrctAmt": 300,
                "corpList": self.corp_entry(outside, 100),
                "cntrctCnclsDate": "2026-08-15",
            },
            "쇼핑몰": {
                "dlvrReqNo": "SHOP-1",
                "dlvrReqChgOrd": 2,
                "prdctSno": "1",
                "dminsttCd": "A1",
                "prdctAmt": 400,
                "cntrctCorpBizno": local,
                "dlvrReqRcptDate": "2026-08-15",
            },
        }
        legacy_config = LocalityRateConfig("legacy", CUTOVER, "generation-5a", None)
        shadow_config = LocalityRateConfig("shadow", CUTOVER, "generation-5a", None)
        legacy = build_locality_resolvers(
            procurement,
            company,
            legacy_config,
            eligible_agency_codes=agencies,
        )
        shadow = build_locality_resolvers(
            procurement,
            company,
            shadow_config,
            eligible_agency_codes=agencies,
        )

        def calculate(resolvers):
            return [
                process_contract_row(
                    rows[sector],
                    agencies,
                    {local},
                    is_shopping=sector == "쇼핑몰",
                    sector=sector,
                    locality_resolver=resolvers[sector],
                )
                for sector in SECTORS
            ]

        legacy_output = calculate(legacy)
        shadow_output = calculate(shadow)
        self.assertEqual(legacy_output, shadow_output)
        self.assertEqual(
            legacy_output,
            [("A1", 100.0, 25.0), ("A1", 200.0, 200.0),
             ("A1", 300.0, 0), ("A1", 400.0, 400.0)],
        )
        self.assertEqual(sum(len(resolver._pending) for resolver in shadow.values()), 5)

    def test_real_legacy_resolver_preserves_duplicate_share_float_order(self):
        from locality_rate_resolver import LocalityRateConfig, build_locality_resolvers

        procurement = sqlite3.connect(":memory:")
        company = sqlite3.connect(":memory:")
        self.addCleanup(procurement.close)
        self.addCleanup(company.close)
        resolver = build_locality_resolvers(
            procurement,
            company,
            LocalityRateConfig("legacy", CUTOVER, "legacy", None),
            eligible_agency_codes={"A1"},
        )["공사"]
        row = {
            "dcsnCntrctNo": "FLOAT000100",
            "dminsttCd": "A1",
            "thtmCntrctAmt": 1,
            "corpList": "".join(
                self.corp_entry("1234567890", "0.01") for _ in range(3)
            ),
            "cntrctCnclsDate": "2026-08-15",
        }
        agencies = {"A1": {"cate_lrg": "group"}}

        without_resolver = process_contract_row(row, agencies, {"1234567890"})
        with_resolver = process_contract_row(
            row,
            agencies,
            {"1234567890"},
            sector="공사",
            locality_resolver=resolver,
        )

        self.assertEqual(with_resolver, without_resolver)
        self.assertEqual(with_resolver[2], 0.00030000000000000003)

    def test_snapshot_applies_inbound_and_outbound_only_after_effective_time(self):
        from locality_rate_resolver import LocalityRateConfig, build_locality_resolvers

        paths = self.configure_test_paths()
        procurement = sqlite3.connect(":memory:")
        company = sqlite3.connect(":memory:")
        self.addCleanup(procurement.close)
        self.addCleanup(company.close)
        company.execute(
            "CREATE TABLE company_master (bizno TEXT PRIMARY KEY, corpNm TEXT, rgnNm TEXT, hdoffceDivNm TEXT, chgDt TEXT)"
        )
        company.execute(
            "INSERT INTO company_master VALUES ('2222222222', 'Outbound', '부산', '본사', '')"
        )
        company.commit()
        ensure_locality_schema(company, paths=paths)
        ensure_snapshot_schema(procurement)
        apply_company_changes(
            company,
            [{"bizno": "1111111111", "corpNm": "Inbound", "rgnNm": "부산", "hdoffceDivNm": "본사", "chgDt": "2026-08-16 11:00:00+09:00"}],
            "2026-08-16",
            "inbound",
            "2026-08-16 12:00:00+09:00",
            paths=paths,
        )
        apply_company_changes(
            company,
            [{"bizno": "2222222222", "corpNm": "Outbound", "rgnNm": "부산", "hdoffceDivNm": "본사", "chgDt": "2026-08-16 09:00:00+09:00"}],
            "2026-08-16",
            "outbound-before",
            "2026-08-16 12:00:00+09:00",
            paths=paths,
        )
        apply_company_changes(
            company,
            [{"bizno": "2222222222", "corpNm": "Outbound", "rgnNm": "서울", "hdoffceDivNm": "본사", "chgDt": "2026-08-16 11:00:00+09:00"}],
            "2026-08-16",
            "outbound-after",
            "2026-08-16 12:00:00+09:00",
            paths=paths,
        )
        config = LocalityRateConfig(
            "snapshot",
            "2026-08-16 08:00:00+09:00",
            "generation-5a",
            "baseline-v1",
        )
        resolver = build_locality_resolvers(
            procurement,
            company,
            config,
            eligible_agency_codes={"A1"},
        )["물품"]
        agencies = {"A1": {"cate_lrg": "부산광역시 및 소속기관"}}

        def local_amount(key, bizno, at):
            row = {
                "dcsnCntrctNo": f"{key}00",
                "dminsttCd": "A1",
                "thtmCntrctAmt": 100,
                "corpList": self.corp_entry(bizno, 100),
                "cntrctCnclsDate": at,
            }
            return process_contract_row(
                row,
                agencies,
                set(),
                sector="물품",
                locality_resolver=resolver,
            )[2]

        self.assertEqual(local_amount("INBEFORE", "1111111111", "2026-08-16 10:00:00+09:00"), 0)
        self.assertEqual(local_amount("INAFTER", "1111111111", "2026-08-16 12:00:00+09:00"), 100)
        self.assertEqual(local_amount("OUTBEFORE", "2222222222", "2026-08-16 10:00:00+09:00"), 100)
        self.assertEqual(local_amount("OUTAFTER", "2222222222", "2026-08-16 12:00:00+09:00"), 0)

    def test_every_rate_call_is_sector_and_resolver_bound(self):
        root = Path(__file__).resolve().parent
        for filename in (
            "build_api_cache.py",
            "build_monthly_cache.py",
            "rate_calc_db.py",
            "export_excel.py",
        ):
            with self.subTest(filename=filename):
                tree = ast.parse((root / filename).read_text(encoding="utf-8"))
                calls = [
                    node
                    for node in ast.walk(tree)
                    if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "process_contract_row"
                ]
                self.assertTrue(calls)
                for call in calls:
                    keywords = {keyword.arg for keyword in call.keywords}
                    self.assertIn("sector", keywords)
                    self.assertIn("locality_resolver", keywords)

    def test_cache_builders_do_not_publish_live_files(self):
        root = Path(__file__).resolve().parent
        api_source = (root / "build_api_cache.py").read_text(encoding="utf-8")
        monthly_source = (root / "build_monthly_cache.py").read_text(encoding="utf-8")
        self.assertNotIn("os.replace(tmp_path, CACHE_FILE)", api_source)
        self.assertNotIn("open(MONTHLY_CACHE", monthly_source)

    def test_nonlegacy_cache_builds_require_a_shared_generation_context(self):
        import build_api_cache
        import build_monthly_cache
        from locality_rate_resolver import LocalityRateConfig

        config = LocalityRateConfig("shadow", CUTOVER, "generation-5a", None)
        for builder in (build_api_cache.build_cache, build_monthly_cache.build_monthly):
            with self.subTest(builder=builder.__module__):
                with self.assertRaisesRegex(ValueError, "shared generation resolver"):
                    builder(locality_config=config)

    def test_cache_builder_functions_remain_callable_with_shared_context(self):
        import build_api_cache
        import build_monthly_cache
        from locality_rate_resolver import LocalityRateConfig, build_locality_resolvers

        procurement = sqlite3.connect(":memory:")
        company = sqlite3.connect(":memory:")
        self.addCleanup(procurement.close)
        self.addCleanup(company.close)
        config = LocalityRateConfig("legacy", CUTOVER, "generation-5a", None)
        resolvers = build_locality_resolvers(
            procurement,
            company,
            config,
            eligible_agency_codes={"A1"},
        )
        sentinel = object()

        with patch.object(build_api_cache, "_build_cache", return_value=sentinel):
            self.assertIs(
                build_api_cache.build_cache(
                    locality_config=config,
                    locality_resolvers=resolvers,
                ),
                sentinel,
            )
        with patch.object(build_monthly_cache, "_build_monthly", return_value=sentinel):
            self.assertIs(
                build_monthly_cache.build_monthly(
                    locality_config=config,
                    locality_resolvers=resolvers,
                ),
                sentinel,
            )

    def test_required_sector_exceptions_are_not_suppressed(self):
        root = Path(__file__).resolve().parent
        for filename in ("build_api_cache.py", "build_monthly_cache.py"):
            tree = ast.parse((root / filename).read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Try):
                    continue
                has_rate_call = any(
                    isinstance(child, ast.Call)
                    and isinstance(child.func, ast.Name)
                    and child.func.id == "process_contract_row"
                    for child in ast.walk(node)
                )
                if not has_rate_call:
                    continue
                for handler in node.handlers:
                    self.assertTrue(
                        any(isinstance(child, ast.Raise) for child in ast.walk(handler)),
                        f"{filename} suppresses a required-sector calculation exception",
                    )

    def test_reports_open_explicit_read_only_resolvers(self):
        root = Path(__file__).resolve().parent
        for filename in ("rate_calc_db.py", "export_excel.py"):
            tree = ast.parse((root / filename).read_text(encoding="utf-8"))
            calls = [
                node
                for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "open_locality_resolvers"
            ]
            self.assertEqual(len(calls), 1)
            read_only = next(
                keyword.value
                for keyword in calls[0].keywords
                if keyword.arg == "read_only"
            )
            self.assertIsInstance(read_only, ast.Constant)
            self.assertIs(read_only.value, True)

    def test_reports_reject_writable_injected_resolver_sets(self):
        import export_excel
        import rate_calc_db
        from locality_rate_resolver import LocalityRateConfig, build_locality_resolvers

        procurement = sqlite3.connect(":memory:")
        company = sqlite3.connect(":memory:")
        self.addCleanup(procurement.close)
        self.addCleanup(company.close)
        config = LocalityRateConfig("legacy", CUTOVER, "generation-5a", None)
        writable = build_locality_resolvers(
            procurement,
            company,
            config,
            eligible_agency_codes={"A1"},
            read_only=False,
        )

        with patch.object(rate_calc_db, "_main") as calculate:
            with self.assertRaisesRegex(ValueError, "read-only"):
                rate_calc_db.main(locality_config=config, locality_resolvers=writable)
            calculate.assert_not_called()
        with patch.object(export_excel, "_generate_agency_excel") as generate:
            with self.assertRaisesRegex(ValueError, "read-only"):
                export_excel.generate_agency_excel(
                    "agency",
                    locality_config=config,
                    locality_resolvers=writable,
                )
            generate.assert_not_called()

    def test_reports_reject_read_only_sets_without_explicit_active_binding(self):
        import export_excel
        import rate_calc_db
        from locality_rate_resolver import LocalityRateConfig, build_locality_resolvers

        procurement = sqlite3.connect(":memory:")
        company = sqlite3.connect(":memory:")
        self.addCleanup(procurement.close)
        self.addCleanup(company.close)
        config = LocalityRateConfig("legacy", CUTOVER, "generation-5a", None)
        unbound = build_locality_resolvers(
            procurement,
            company,
            config,
            eligible_agency_codes={"A1"},
            read_only=True,
        )

        with patch.object(rate_calc_db, "_main") as calculate:
            with self.assertRaisesRegex(ValueError, "active generation/baseline"):
                rate_calc_db.main(locality_config=config, locality_resolvers=unbound)
            calculate.assert_not_called()
        with patch.object(export_excel, "_generate_agency_excel") as generate:
            with self.assertRaisesRegex(ValueError, "active generation/baseline"):
                export_excel.generate_agency_excel(
                    "agency",
                    locality_config=config,
                    locality_resolvers=unbound,
                )
            generate.assert_not_called()

    def test_reports_accept_matching_read_only_active_binding(self):
        import export_excel
        import rate_calc_db
        from locality_rate_resolver import (
            ActiveLocalityBinding,
            LocalityRateConfig,
            build_locality_resolvers,
        )

        procurement = sqlite3.connect(":memory:")
        company = sqlite3.connect(":memory:")
        self.addCleanup(procurement.close)
        self.addCleanup(company.close)
        config = LocalityRateConfig("legacy", CUTOVER, "generation-5a", None)
        resolvers = build_locality_resolvers(
            procurement,
            company,
            config,
            eligible_agency_codes={"A1"},
            read_only=True,
            active_binding=ActiveLocalityBinding(config.generation_id, config.baseline_id),
        )
        sentinel = object()

        with patch.object(rate_calc_db, "_main", return_value=sentinel):
            self.assertIs(
                rate_calc_db.main(
                    locality_config=config,
                    locality_resolvers=resolvers,
                ),
                sentinel,
            )
        with patch.object(export_excel, "_generate_agency_excel", return_value=sentinel):
            self.assertIs(
                export_excel.generate_agency_excel(
                    "agency",
                    locality_config=config,
                    locality_resolvers=resolvers,
                ),
                sentinel,
            )

    def test_direct_cache_builder_clis_require_task5b_orchestrator(self):
        root = Path(__file__).resolve().parent
        with tempfile.TemporaryDirectory() as temporary:
            for filename, cache_name in (
                ("build_api_cache.py", "api_cache.json"),
                ("build_monthly_cache.py", "monthly_cache.json"),
            ):
                with self.subTest(filename=filename):
                    completed = subprocess.run(
                        [sys.executable, str(root / filename)],
                        cwd=temporary,
                        env=os.environ.copy(),
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        timeout=30,
                    )
                    self.assertNotEqual(completed.returncode, 0)
                    self.assertRegex(completed.stderr.lower(), r"migration.*orchestrator|required.*orchestrator")
                    self.assertNotIn("완료", completed.stdout)
                    self.assertNotIn("미리보기", completed.stdout)
                    self.assertFalse((Path(temporary) / cache_name).exists())

    def test_all_rate_consumers_load_one_complete_canonical_population(self):
        root = Path(__file__).resolve().parent
        for filename in (
            "build_api_cache.py",
            "build_monthly_cache.py",
            "rate_calc_db.py",
            "export_excel.py",
        ):
            with self.subTest(filename=filename):
                source = (root / filename).read_text(encoding="utf-8")
                tree = ast.parse(source)
                calls = [
                    node
                    for node in ast.walk(tree)
                    if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "load_rate_contract_populations"
                ]
                self.assertEqual(
                    len(calls),
                    1,
                    f"{filename} must load one complete canonical population",
                )
                self.assertNotIn("dedup_by_dcsn", source)
                self.assertNotIn("select_canonical_contract_rows", source)
                if filename == "export_excel.py":
                    self.assertLess(
                        source.index("load_rate_contract_populations("),
                        source.index("if not target_cds:"),
                        "export target filtering must follow complete canonicalization",
                    )
                for node in ast.walk(tree):
                    if not isinstance(node, ast.Call):
                        continue
                    if not isinstance(node.func, ast.Attribute) or node.func.attr != "drop_duplicates":
                        continue
                    keywords = {keyword.arg: keyword.value for keyword in node.keywords}
                    subset = keywords.get("subset")
                    if not isinstance(subset, (ast.List, ast.Tuple)):
                        continue
                    fields = {
                        element.value
                        for element in subset.elts
                        if isinstance(element, ast.Constant) and isinstance(element.value, str)
                    }
                    self.assertFalse(
                        fields & {"untyCntrctNo", "dcsnCntrctNo", "dlvrReqNo", "prdctSno"},
                        f"{filename} retains consumer-specific contract selection",
                    )

    def test_complete_population_precedes_period_agency_revision_and_method_filters(self):
        from core_calc import load_rate_contract_populations

        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        self.create_rate_contract_tables(connection)
        connection.executemany(
            """
            INSERT INTO cnstwk_cntrct (
                untyCntrctNo, dcsnCntrctNo, cntrctInsttCd, dminsttCd,
                thtmCntrctAmt, totCntrctAmt, corpList, cntrctCnclsDate,
                cntrctDate, cntrctCnclsMthdNm, cnstwkNm, updDt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "MOVE-1", "MOVE000100", "A1", "A1", 100, 100,
                    self.corp_entry("1234567890", 100), "2026-08-01",
                    "2026-08-01", "수의계약", "obsolete", "2026-08-01 09:00:00",
                ),
                (
                    "MOVE-2", "MOVE000101", "A2", "A2", 120, 120,
                    self.corp_entry("1234567890", 100), "2026-08-08",
                    "2026-08-08", "일반경쟁", "current", "2026-08-08 09:00:00",
                ),
            ],
        )

        selected = load_rate_contract_populations(
            connection,
            eligible_agency_codes={"A1", "A2"},
        )["공사"]

        self.assertEqual(selected["dcsnCntrctNo"].tolist(), ["MOVE000101"])
        self.assertTrue(selected[selected["cntrctCnclsDate"] == "2026-08-01"].empty)
        self.assertTrue(selected[selected["dminsttCd"] == "A1"].empty)
        self.assertTrue(selected[selected["dcsnCntrctNo"].str.endswith("00")].empty)
        self.assertTrue(selected[selected["cntrctCnclsMthdNm"] == "수의계약"].empty)

    def test_complete_population_exposes_collision_before_filters_can_hide_it(self):
        from core_calc import load_rate_contract_populations

        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        self.create_rate_contract_tables(connection)
        connection.executemany(
            """
            INSERT INTO cnstwk_cntrct (
                untyCntrctNo, dcsnCntrctNo, cntrctInsttCd, dminsttCd,
                thtmCntrctAmt, totCntrctAmt, corpList, cntrctCnclsDate,
                cntrctDate, cntrctCnclsMthdNm, cnstwkNm, updDt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "C-1", "COLLIDE00100", "A1", "A1", 100, 100,
                    self.corp_entry("1234567890", 100), "2026-08-01",
                    "2026-08-01", "수의계약", "hidden", "2026-08-01 09:00:00",
                ),
                (
                    "C-2", "COLLIDE00100", "A1", "A1", 101, 101,
                    self.corp_entry("1234567890", 100), "2026-08-08",
                    "2026-08-08", "일반경쟁", "visible", "2026-08-08 09:00:00",
                ),
            ],
        )

        with self.assertRaises(CanonicalContractCollision):
            load_rate_contract_populations(
                connection,
                eligible_agency_codes={"A1"},
            )

    def test_rate_projection_selects_same_newest_metadata_rows_as_task4(self):
        from contract_population import iter_canonical_contracts
        from core_calc import load_rate_contract_populations

        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        self.create_rate_contract_tables(connection)
        for table, decision, display_column, source_order_column in (
            ("cnstwk_cntrct", "CONST000100", "cnstwkNm", "dcsnCntrctDt"),
            ("servc_cntrct", "SERVC000100", "cntrctNm", "updDt"),
            ("thng_cntrct", "GOODS000100", "cntrctNm", "updDt"),
        ):
            connection.executemany(
                f"""
                INSERT INTO [{table}] (
                    untyCntrctNo, dcsnCntrctNo, cntrctInsttCd, dminsttCd,
                    thtmCntrctAmt, totCntrctAmt, corpList, cntrctCnclsDate,
                    cntrctDate, [{display_column}], cnstrtsiteRgnNm,
                    [{source_order_column}]
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "U-1", decision, "A1", "A1", 100, 100,
                        self.corp_entry("1234567890", 100), "2026-08-15",
                        "2026-08-15", "stale", "서울", "2026-08-15 09:00:00",
                    ),
                    (
                        "U-2", decision, "A1", "A1", 100, 100,
                        self.corp_entry("1234567890", 100), "2026-08-15",
                        "2026-08-15", "newest", "부산", "2026-08-15 10:00:00",
                    ),
                ],
            )
        connection.executemany(
            """
            INSERT INTO shopping_cntrct (
                dlvrReqNo, dlvrReqChgOrd, prdctSno, dminsttCd, prdctAmt,
                cntrctCorpBizno, dlvrReqRcptDate, dlvrReqNm, dlvrReqChgDt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("SHOP-1", 0, "1", "A1", 100, "1234567890", "2026-08-15", "stale", "2026-08-15 09:00:00"),
                ("SHOP-1", 0, "1", "A1", 100, "1234567890", "2026-08-15", "newest", "2026-08-15 10:00:00"),
            ],
        )

        populations = load_rate_contract_populations(
            connection,
            eligible_agency_codes={"A1"},
        )
        display_columns = {
            "공사": "cnstwkNm",
            "용역": "cntrctNm",
            "물품": "cntrctNm",
            "쇼핑몰": "dlvrReqNm",
        }
        for sector, display_column in display_columns.items():
            with self.subTest(sector=sector):
                task4 = next(iter_canonical_contracts(connection, sector))
                self.assertEqual(task4.source_row[display_column], "newest")
                self.assertEqual(populations[sector].iloc[0][display_column], "newest")
                source_order_column = {
                    "공사": "dcsnCntrctDt",
                    "쇼핑몰": "dlvrReqChgDt",
                }.get(sector, "updDt")
                self.assertEqual(
                    populations[sector].iloc[0][source_order_column],
                    "2026-08-15 10:00:00",
                )

    def test_schema_projection_includes_all_available_source_order_fields(self):
        from contract_population import canonical_source_projection

        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        self.create_rate_contract_tables(connection)

        construction = canonical_source_projection(connection, "공사", ("cnstwkNm",))
        shopping = canonical_source_projection(connection, "쇼핑몰", ("dlvrReqNm",))

        for field in ("lastUpdtDt", "updDt", "chgDt", "rgstDt", "cntrctDate", "dcsnCntrctDt"):
            self.assertIn(field, construction)
        for field in (
            "lastUpdtDt", "updDt", "chgDt", "rgstDt", "cntrctDate",
            "dlvrReqDt", "dlvrReqChgDt", "dlvrReqRcptDate",
        ):
            self.assertIn(field, shopping)

    def test_rate_modules_have_no_ad_hoc_locality_membership(self):
        root = Path(__file__).resolve().parent
        for filename in (
            "build_api_cache.py",
            "build_monthly_cache.py",
            "rate_calc_db.py",
            "export_excel.py",
        ):
            source = (root / filename).read_text(encoding="utf-8")
            self.assertNotIn("BUSAN_BIZNO_PREFIXES", source, filename)
            self.assertNotIn(" in biznos", source, filename)
            self.assertNotIn(" not in biznos", source, filename)

    @classmethod
    def contract_row(cls, unty, decision, amount, display_name, *, updated="2026-08-15 09:00:00"):
        return {
            "untyCntrctNo": unty,
            "dcsnCntrctNo": decision,
            "dminsttCd": "A1",
            "cntrctInsttCd": "A1",
            "thtmCntrctAmt": amount,
            "totCntrctAmt": amount,
            "corpList": cls.corp_entry("1234567890", 100),
            "cntrctCnclsDate": "2026-08-15",
            "lastUpdtDt": updated,
            "display_name": display_name,
        }

    @staticmethod
    def create_rate_contract_tables(connection):
        for table, display_column in (
            ("cnstwk_cntrct", "cnstwkNm"),
            ("servc_cntrct", "cntrctNm"),
            ("thng_cntrct", "cntrctNm"),
        ):
            connection.execute(
                f"""
                CREATE TABLE [{table}] (
                    untyCntrctNo TEXT,
                    dcsnCntrctNo TEXT,
                    cntrctInsttCd TEXT,
                    dminsttCd TEXT,
                    dminsttList TEXT,
                    thtmCntrctAmt REAL,
                    totCntrctAmt REAL,
                    corpList TEXT,
                    cntrctCnclsDate TEXT,
                    cntrctDate TEXT,
                    cntrctCnclsMthdNm TEXT,
                    [{display_column}] TEXT,
                    cnstrtsiteRgnNm TEXT,
                    lastUpdtDt TEXT,
                    updDt TEXT,
                    chgDt TEXT,
                    rgstDt TEXT,
                    dcsnCntrctDt TEXT
                )
                """
            )
        connection.execute(
            """
            CREATE TABLE shopping_cntrct (
                dlvrReqNo TEXT,
                dlvrReqChgOrd TEXT,
                prdctSno TEXT,
                dminsttCd TEXT,
                cntrctInsttCd TEXT,
                prdctAmt REAL,
                cntrctCorpBizno TEXT,
                dlvrReqRcptDate TEXT,
                dlvrReqNm TEXT,
                lastUpdtDt TEXT,
                updDt TEXT,
                chgDt TEXT,
                rgstDt TEXT,
                cntrctDate TEXT,
                dlvrReqDt TEXT,
                dlvrReqChgDt TEXT
            )
            """
        )


if __name__ == "__main__":
    unittest.main()
