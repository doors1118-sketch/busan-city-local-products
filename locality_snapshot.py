"""Frozen supplier-locality snapshots and exact historical baseline manifests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
import sqlite3
from typing import Any, Collection, Iterable, Mapping
from zoneinfo import ZoneInfo

from company_locality import normalize_bizno, status_at
from contract_population import (
    ITERATOR_VERSION,
    CanonicalContract,
    canonical_contract_from_row,
    content_fingerprint,
    iter_canonical_contracts,
)


SEOUL = ZoneInfo("Asia/Seoul")
CLASSIFIER_VERSION = "supplier_locality_snapshot_v1"
VALID_MODES = {"legacy", "shadow", "snapshot"}
REQUIRED_SECTORS = ("공사", "용역", "물품", "쇼핑몰")


class SnapshotError(RuntimeError):
    pass


class MissingHistoricalSnapshot(SnapshotError):
    pass


class SnapshotContentMismatch(SnapshotError):
    pass


class UnknownLocality(SnapshotError):
    pass


class BaselineManifestConflict(SnapshotError):
    pass


@dataclass(frozen=True)
class BaselineManifest:
    baseline_id: str
    cutover_at: str
    classifier_version: str
    iterator_version: str
    cache_generation_id: str
    manifest_fingerprint: str
    expected_contracts: int
    expected_suppliers: int
    expected_share_micros: int
    expected_amount_won: int
    status: str
    created_at: str


@dataclass(frozen=True)
class BaselineCoverage:
    baseline_id: str
    expected_contracts: int
    matched_contracts: int
    expected_suppliers: int
    matched_suppliers: int
    expected_share_micros: int
    matched_share_micros: int
    expected_amount_won: int
    matched_amount_won: int
    missing_suppliers: int
    extra_suppliers: int
    fingerprint_mismatches: int
    unknown_suppliers: int
    unknown_amount_won: int
    coverage_pct: float
    complete: bool
    manifest_fingerprint_matches: bool
    source_population_matches: bool
    required_sectors_complete: bool
    invalid_basis_suppliers: int
    fallback_amount_won: int


@dataclass(frozen=True)
class _SnapshotCandidate:
    contract: CanonicalContract
    bizno: str
    share_pct: float
    is_busan: bool | None
    basis: str
    baseline_id: str | None


def _create_control_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS locality_activation_state (
            singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
            writes_enabled INTEGER NOT NULL CHECK(writes_enabled IN (0,1)),
            active_generation_id TEXT,
            ever_snapshot_activated INTEGER NOT NULL CHECK(ever_snapshot_activated IN (0,1)),
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS locality_generation_clock (
            singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
            data_generation INTEGER NOT NULL,
            control_revision INTEGER NOT NULL
        )
        """
    )
    conn.execute("INSERT OR IGNORE INTO locality_generation_clock VALUES (1, 0, 0)")
    conn.execute(
        "INSERT OR IGNORE INTO locality_activation_state VALUES (1, 1, NULL, 0, ?)",
        (_now(),),
    )


def _create_snapshot_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS contract_supplier_locality (
            sector TEXT NOT NULL,
            contract_key TEXT NOT NULL,
            contract_revision TEXT NOT NULL DEFAULT '',
            bizno TEXT NOT NULL,
            share_pct REAL NOT NULL,
            is_busan INTEGER,
            basis TEXT NOT NULL,
            contract_date TEXT NOT NULL DEFAULT '',
            classified_at TEXT NOT NULL,
            classifier_version TEXT NOT NULL,
            content_fingerprint TEXT NOT NULL,
            baseline_id TEXT,
            introduced_generation_id TEXT NOT NULL,
            PRIMARY KEY(sector, contract_key, contract_revision, bizno)
        ) WITHOUT ROWID
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS locality_baseline_manifest (
            baseline_id TEXT PRIMARY KEY,
            cutover_at TEXT NOT NULL,
            classifier_version TEXT NOT NULL,
            iterator_version TEXT NOT NULL,
            cache_generation_id TEXT NOT NULL,
            manifest_fingerprint TEXT NOT NULL,
            expected_contracts INTEGER NOT NULL,
            expected_suppliers INTEGER NOT NULL,
            expected_share_micros INTEGER NOT NULL,
            expected_amount_won INTEGER NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('building','complete','failed')),
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS locality_baseline_contract (
            baseline_id TEXT NOT NULL,
            sector TEXT NOT NULL,
            contract_key TEXT NOT NULL,
            contract_revision TEXT NOT NULL,
            amount_won INTEGER NOT NULL,
            content_fingerprint TEXT NOT NULL,
            PRIMARY KEY(baseline_id, sector, contract_key, contract_revision),
            FOREIGN KEY(baseline_id) REFERENCES locality_baseline_manifest(baseline_id)
        ) WITHOUT ROWID
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS locality_baseline_supplier (
            baseline_id TEXT NOT NULL,
            sector TEXT NOT NULL,
            contract_key TEXT NOT NULL,
            contract_revision TEXT NOT NULL,
            bizno TEXT NOT NULL,
            share_micros INTEGER NOT NULL,
            PRIMARY KEY(baseline_id, sector, contract_key, contract_revision, bizno),
            FOREIGN KEY(baseline_id, sector, contract_key, contract_revision)
                REFERENCES locality_baseline_contract(baseline_id, sector, contract_key, contract_revision)
        ) WITHOUT ROWID
        """
    )


def _install_snapshot_triggers(conn: sqlite3.Connection) -> None:
    data_tables: tuple[str, ...] = ()
    control_tables = (
        "contract_supplier_locality",
        "locality_baseline_manifest",
        "locality_baseline_contract",
        "locality_baseline_supplier",
    )
    for table, clock in (
        *((table, "data_generation") for table in data_tables),
        *((table, "control_revision") for table in control_tables),
    ):
        for action in ("INSERT", "UPDATE", "DELETE"):
            conn.execute(
                f"DROP TRIGGER IF EXISTS locality_{table}_{action.lower()}_clock"
            )
            conn.execute(
                f"CREATE TRIGGER locality_{table}_{action.lower()}_clock "
                f"AFTER {action} ON {table} BEGIN "
                f"UPDATE locality_generation_clock SET {clock} = {clock} + 1 WHERE singleton_id = 1; END"
            )
            conn.execute(
                f"CREATE TRIGGER IF NOT EXISTS locality_{table}_{action.lower()}_guard "
                f"BEFORE {action} ON {table} WHEN locality_guarded_write() = 0 "
                "OR (locality_guarded_write() != 2 AND COALESCE((SELECT writes_enabled "
                "FROM locality_activation_state WHERE singleton_id = 1), 0) = 0) "
                "BEGIN SELECT RAISE(ABORT, 'locality protected write requires guarded session'); END"
            )


def ensure_snapshot_schema(conn: sqlite3.Connection) -> None:
    """Install snapshot tables under the canonical maintenance lock and clocks."""
    from maintenance_lock import (
        install_write_guard,
        maintenance_lock,
        maintenance_write_permission,
        require_locality_paths,
    )

    paths = require_locality_paths()
    owns_transaction = not conn.in_transaction
    with maintenance_lock(paths.maintenance_path, 5):
        if owns_transaction:
            conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        install_write_guard(conn)
        with maintenance_write_permission(conn):
            _create_control_tables(conn)
            _create_snapshot_tables(conn)
            _install_snapshot_triggers(conn)
        if owns_transaction:
            conn.commit()


def _now() -> str:
    return datetime.now(SEOUL).isoformat(timespec="seconds")


def _share_micros(share_pct: float) -> int:
    return int(
        (Decimal(str(share_pct)) * Decimal(1_000_000)).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP
        )
    )


def _manifest_from_row(row: tuple[Any, ...]) -> BaselineManifest:
    return BaselineManifest(*row)


def _manifest_row(conn: sqlite3.Connection, baseline_id: str) -> BaselineManifest | None:
    row = conn.execute(
        """
        SELECT baseline_id, cutover_at, classifier_version, iterator_version,
               cache_generation_id, manifest_fingerprint, expected_contracts,
               expected_suppliers, expected_share_micros, expected_amount_won,
               status, created_at
        FROM locality_baseline_manifest WHERE baseline_id = ?
        """,
        (baseline_id,),
    ).fetchone()
    return _manifest_from_row(row) if row else None


def _manifest_fingerprint(contracts: list[CanonicalContract]) -> str:
    payload = []
    for contract in sorted(contracts, key=lambda row: row.identity):
        payload.append(
            {
                "identity": list(contract.identity),
                "amount_won": contract.amount_won,
                "content_fingerprint": content_fingerprint(contract),
                "suppliers": [
                    [supplier.bizno, _share_micros(supplier.share_pct)]
                    for supplier in sorted(contract.suppliers)
                ],
            }
        )
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _stored_manifest_fingerprint(
    expected_contracts: dict[tuple[str, str, str], tuple[int, str]],
    expected_suppliers: dict[tuple[str, str, str, str], int],
) -> str:
    payload = []
    for identity, (amount_won, fingerprint) in sorted(expected_contracts.items()):
        suppliers = sorted(
            [key[3], share]
            for key, share in expected_suppliers.items()
            if key[:3] == identity
        )
        payload.append(
            {
                "identity": list(identity),
                "amount_won": amount_won,
                "content_fingerprint": fingerprint,
                "suppliers": suppliers,
            }
        )
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def create_baseline_manifest(
    conn: sqlite3.Connection,
    canonical_rows: Iterable[CanonicalContract],
    baseline_id: str,
    *,
    eligible_agency_codes: Collection[str] | None = None,
) -> BaselineManifest:
    """Persist immutable canonical expectations and mark exact coverage complete or failed."""
    from maintenance_lock import guarded_write_session

    contracts = sorted(list(canonical_rows), key=lambda row: row.identity)
    if not baseline_id:
        raise ValueError("baseline_id is required")
    if len({contract.identity for contract in contracts}) != len(contracts):
        raise BaselineManifestConflict("baseline contains duplicate canonical identities")
    fingerprint = _manifest_fingerprint(contracts)
    expected_suppliers = sum(len(contract.suppliers) for contract in contracts)
    expected_share_micros = sum(
        _share_micros(supplier.share_pct)
        for contract in contracts
        for supplier in contract.suppliers
    )
    expected_amount_won = sum(contract.amount_won for contract in contracts)
    with guarded_write_session(conn):
        existing = _manifest_row(conn, baseline_id)
        if existing and existing.manifest_fingerprint not in ("", fingerprint):
            raise BaselineManifestConflict("baseline_id already names different canonical content")
        if existing is None:
            seed = conn.execute(
                """
                SELECT classified_at, classifier_version, introduced_generation_id
                FROM contract_supplier_locality WHERE baseline_id = ?
                ORDER BY classified_at DESC, introduced_generation_id DESC LIMIT 1
                """,
                (baseline_id,),
            ).fetchone()
            created_at, classifier_version, generation_id = seed or (_now(), CLASSIFIER_VERSION, "unassigned")
            conn.execute(
                "INSERT INTO locality_baseline_manifest VALUES (?, ?, ?, ?, ?, '', 0, 0, 0, 0, 'building', ?)",
                (baseline_id, created_at, classifier_version, ITERATOR_VERSION, generation_id, created_at),
            )
        conn.execute("DELETE FROM locality_baseline_supplier WHERE baseline_id = ?", (baseline_id,))
        conn.execute("DELETE FROM locality_baseline_contract WHERE baseline_id = ?", (baseline_id,))
        for contract in contracts:
            conn.execute(
                "INSERT INTO locality_baseline_contract VALUES (?, ?, ?, ?, ?, ?)",
                (baseline_id, *contract.identity, contract.amount_won, content_fingerprint(contract)),
            )
            for supplier in contract.suppliers:
                conn.execute(
                    "INSERT INTO locality_baseline_supplier VALUES (?, ?, ?, ?, ?, ?)",
                    (baseline_id, *contract.identity, supplier.bizno, _share_micros(supplier.share_pct)),
                )
        conn.execute(
            """
            UPDATE locality_baseline_manifest
            SET manifest_fingerprint=?, expected_contracts=?, expected_suppliers=?,
                expected_share_micros=?, expected_amount_won=?, status='building'
            WHERE baseline_id=?
            """,
            (
                fingerprint,
                len(contracts),
                expected_suppliers,
                expected_share_micros,
                expected_amount_won,
                baseline_id,
            ),
        )
    coverage = verify_baseline_manifest(
        conn,
        baseline_id,
        eligible_agency_codes=eligible_agency_codes,
    )
    with guarded_write_session(conn):
        conn.execute(
            "UPDATE locality_baseline_manifest SET status=? WHERE baseline_id=?",
            ("complete" if coverage.complete else "failed", baseline_id),
        )
    manifest = _manifest_row(conn, baseline_id)
    if manifest is None:
        raise RuntimeError("baseline manifest did not persist")
    return manifest


def verify_baseline_manifest(
    conn: sqlite3.Connection,
    baseline_id: str,
    *,
    eligible_agency_codes: Collection[str] | None = None,
) -> BaselineCoverage:
    manifest = _manifest_row(conn, baseline_id)
    if manifest is None:
        raise KeyError(f"unknown baseline manifest: {baseline_id}")
    expected_contract_rows = conn.execute(
        """
        SELECT sector, contract_key, contract_revision, amount_won, content_fingerprint
        FROM locality_baseline_contract WHERE baseline_id=?
        """,
        (baseline_id,),
    ).fetchall()
    expected_supplier_rows = conn.execute(
        """
        SELECT sector, contract_key, contract_revision, bizno, share_micros
        FROM locality_baseline_supplier WHERE baseline_id=?
        """,
        (baseline_id,),
    ).fetchall()
    expected_contracts = {(row[0], row[1], row[2]): (row[3], row[4]) for row in expected_contract_rows}
    expected_suppliers = {(row[0], row[1], row[2], row[3]): row[4] for row in expected_supplier_rows}
    manifest_fingerprint_matches = (
        _stored_manifest_fingerprint(expected_contracts, expected_suppliers)
        == manifest.manifest_fingerprint
    )
    actual_rows = conn.execute(
        """
        SELECT sector, contract_key, contract_revision, bizno, share_pct,
               is_busan, basis, content_fingerprint
        FROM contract_supplier_locality WHERE baseline_id=?
        """,
        (baseline_id,),
    ).fetchall()
    actual = {(row[0], row[1], row[2], row[3]): row[4:] for row in actual_rows}
    source_contracts: dict[tuple[str, str, str], tuple[int, str]] = {}
    source_suppliers: dict[tuple[str, str, str, str], int] = {}
    populated_sectors: set[str] = set()
    source_enumeration_valid = eligible_agency_codes is not None
    for sector in REQUIRED_SECTORS:
        if eligible_agency_codes is None:
            continue
        try:
            rows = list(
                iter_canonical_contracts(
                    conn,
                    sector,
                    (None, manifest.cutover_at),
                    eligible_agency_codes=eligible_agency_codes,
                )
            )
        except (sqlite3.Error, RuntimeError, ValueError):
            source_enumeration_valid = False
            continue
        if rows:
            populated_sectors.add(sector)
        for contract in rows:
            source_contracts[contract.identity] = (
                contract.amount_won,
                content_fingerprint(contract),
            )
            for supplier in contract.suppliers:
                source_suppliers[(*contract.identity, supplier.bizno)] = _share_micros(
                    supplier.share_pct
                )
    required_sectors_complete = populated_sectors == set(REQUIRED_SECTORS)
    source_population_matches = (
        source_enumeration_valid
        and required_sectors_complete
        and source_contracts == expected_contracts
        and source_suppliers == expected_suppliers
    )
    matched_suppliers = 0
    matched_share_micros = 0
    unknown_suppliers = 0
    unknown_amount = Decimal(0)
    invalid_basis_suppliers = 0
    fallback_amount = Decimal(0)
    fingerprint_contracts: set[tuple[str, str, str]] = set()
    missing_suppliers = 0
    exact_contracts: set[tuple[str, str, str]] = set()
    for supplier_key, expected_share in expected_suppliers.items():
        contract_key = supplier_key[:3]
        current = actual.get(supplier_key)
        if current is None:
            missing_suppliers += 1
            continue
        actual_share = _share_micros(current[0])
        expected_fingerprint = expected_contracts[contract_key][1]
        if current[3] != expected_fingerprint:
            fingerprint_contracts.add(contract_key)
        if actual_share == expected_share and current[3] == expected_fingerprint:
            matched_suppliers += 1
            matched_share_micros += expected_share
        if current[1] is None:
            unknown_suppliers += 1
            unknown_amount += Decimal(expected_contracts[contract_key][0]) * Decimal(expected_share) / Decimal(100_000_000)
        if current[2] != "legacy_baseline_v1":
            invalid_basis_suppliers += 1
            fallback_amount += Decimal(expected_contracts[contract_key][0]) * Decimal(expected_share) / Decimal(100_000_000)
    expected_keys = set(expected_suppliers)
    extra_suppliers = len(set(actual) - expected_keys)
    for contract_key, (amount, fingerprint) in expected_contracts.items():
        supplier_keys = {key for key in expected_keys if key[:3] == contract_key}
        if supplier_keys and all(
            key in actual
            and _share_micros(actual[key][0]) == expected_suppliers[key]
            and actual[key][3] == fingerprint
            for key in supplier_keys
        ) and not any(key[:3] == contract_key for key in set(actual) - expected_keys):
            exact_contracts.add(contract_key)
    matched_amount = sum(expected_contracts[key][0] for key in exact_contracts)
    coverage_pct = 100.0 if not expected_suppliers and not actual else (
        matched_suppliers / len(expected_suppliers) * 100.0 if expected_suppliers else 0.0
    )
    unknown_amount_won = int(unknown_amount.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    fallback_amount_won = int(fallback_amount.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    complete = (
        len(expected_contracts) == manifest.expected_contracts == len(exact_contracts)
        and len(expected_suppliers) == manifest.expected_suppliers == matched_suppliers
        and sum(expected_suppliers.values()) == manifest.expected_share_micros == matched_share_micros
        and sum(row[0] for row in expected_contracts.values()) == manifest.expected_amount_won == matched_amount
        and missing_suppliers == 0
        and extra_suppliers == 0
        and not fingerprint_contracts
        and unknown_suppliers == 0
        and unknown_amount_won == 0
        and manifest_fingerprint_matches
        and source_population_matches
        and required_sectors_complete
        and invalid_basis_suppliers == 0
        and fallback_amount_won == 0
    )
    return BaselineCoverage(
        baseline_id,
        manifest.expected_contracts,
        len(exact_contracts),
        manifest.expected_suppliers,
        matched_suppliers,
        manifest.expected_share_micros,
        matched_share_micros,
        manifest.expected_amount_won,
        matched_amount,
        missing_suppliers,
        extra_suppliers,
        len(fingerprint_contracts),
        unknown_suppliers,
        unknown_amount_won,
        coverage_pct,
        complete,
        manifest_fingerprint_matches,
        source_population_matches,
        required_sectors_complete,
        invalid_basis_suppliers,
        fallback_amount_won,
    )


def _parse_at(value: str) -> tuple[datetime | None, bool]:
    text = str(value or "").strip()
    if not text:
        return None, False
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        try:
            return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=SEOUL), True
        except ValueError:
            return None, True
    if len(text) == 8 and text.isdigit():
        try:
            return datetime.strptime(text, "%Y%m%d").replace(tzinfo=SEOUL), True
        except ValueError:
            return None, True
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        formats = []
        if text.isdigit() and len(text) == 14:
            formats.append("%Y%m%d%H%M%S")
        elif text.isdigit() and len(text) == 12:
            formats.append("%Y%m%d%H%M")
        formats.append("%Y-%m-%d %H:%M:%S")
        for format_string in formats:
            try:
                return datetime.strptime(text, format_string).replace(tzinfo=SEOUL), False
            except ValueError:
                pass
        return None, False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SEOUL)
    return parsed.astimezone(SEOUL), False


def _revision_rank(revision: str) -> tuple[int, int | str]:
    return (1, int(revision)) if revision.isdigit() else (0, revision)


class SnapshotResolver:
    def __init__(
        self,
        procurement_conn: sqlite3.Connection,
        company_conn: sqlite3.Connection,
        *,
        mode: str = "shadow",
        now: str | None = None,
        cutover_at: str,
        generation_id: str = "unassigned",
        eligible_agency_codes: Collection[str] | None = None,
    ) -> None:
        if mode not in VALID_MODES:
            raise ValueError("locality mode must be legacy, shadow, or snapshot")
        cutover, cutover_date_only = _parse_at(cutover_at)
        if cutover is None or cutover_date_only:
            raise ValueError("cutover_at must include an Asia/Seoul effective time")
        self.procurement_conn = procurement_conn
        self.company_conn = company_conn
        self.mode = mode
        self.now = now or _now()
        self.cutover_at = cutover_at
        self._cutover = cutover
        self.generation_id = generation_id
        self.eligible_agency_codes = eligible_agency_codes
        self._pending: dict[tuple[str, str, str, str], _SnapshotCandidate] = {}
        self._validated_baselines: dict[str, tuple[int, BaselineManifest]] = {}

    def _contract(self, row: CanonicalContract | Mapping[str, Any]) -> CanonicalContract:
        if isinstance(row, CanonicalContract):
            return row
        sector = row.get("sector") or row.get("_locality_sector")
        if not sector:
            raise ValueError("sector is required for locality snapshot resolution")
        return canonical_contract_from_row(
            row,
            str(sector),
            eligible_agency_codes=self.eligible_agency_codes,
        )

    @staticmethod
    def _key(contract: CanonicalContract, bizno: str) -> tuple[str, str, str, str]:
        return (*contract.identity, normalize_bizno(bizno))

    def _existing(self, contract: CanonicalContract, bizno: str) -> tuple[Any, ...] | None:
        key = self._key(contract, bizno)
        pending = self._pending.get(key)
        if pending is not None:
            return (
                pending.share_pct,
                None if pending.is_busan is None else int(pending.is_busan),
                pending.basis,
                content_fingerprint(pending.contract),
                pending.baseline_id,
                self.generation_id,
            )
        return self.procurement_conn.execute(
            """
            SELECT share_pct, is_busan, basis, content_fingerprint,
                   baseline_id, introduced_generation_id
            FROM contract_supplier_locality
            WHERE sector=? AND contract_key=? AND contract_revision=? AND bizno=?
            """,
            key,
        ).fetchone()

    def _validate_historical_snapshot(
        self,
        contract: CanonicalContract,
        bizno: str,
        share_pct: float,
        existing: tuple[Any, ...],
    ) -> None:
        baseline_id = existing[4]
        if not baseline_id:
            raise MissingHistoricalSnapshot("pre-cutover snapshot has no baseline ownership")
        manifest = _manifest_row(self.procurement_conn, baseline_id)
        if manifest is None or manifest.status != "complete":
            raise MissingHistoricalSnapshot("pre-cutover snapshot baseline is not complete")
        manifest_cutover, manifest_date_only = _parse_at(manifest.cutover_at)
        if (
            manifest.classifier_version != CLASSIFIER_VERSION
            or manifest.iterator_version != ITERATOR_VERSION
            or manifest_cutover is None
            or manifest_date_only
            or manifest_cutover != self._cutover
            or manifest.cache_generation_id != existing[5]
        ):
            raise MissingHistoricalSnapshot("pre-cutover snapshot baseline metadata is invalid")
        self._validate_baseline_once(baseline_id, manifest)
        expected_contract = self.procurement_conn.execute(
            """
            SELECT amount_won, content_fingerprint
            FROM locality_baseline_contract
            WHERE baseline_id=? AND sector=? AND contract_key=? AND contract_revision=?
            """,
            (baseline_id, *contract.identity),
        ).fetchone()
        expected_supplier = self.procurement_conn.execute(
            """
            SELECT share_micros
            FROM locality_baseline_supplier
            WHERE baseline_id=? AND sector=? AND contract_key=? AND contract_revision=? AND bizno=?
            """,
            (baseline_id, *contract.identity, bizno),
        ).fetchone()
        if (
            expected_contract is None
            or expected_supplier is None
            or expected_contract != (contract.amount_won, content_fingerprint(contract))
            or expected_supplier[0] != _share_micros(share_pct)
            or existing[2] != "legacy_baseline_v1"
        ):
            raise MissingHistoricalSnapshot("pre-cutover snapshot is not an owned baseline member")

    def _validate_baseline_once(
        self,
        baseline_id: str,
        manifest: BaselineManifest,
    ) -> None:
        from maintenance_lock import read_data_generation

        source_generation = read_data_generation(self.procurement_conn)
        cached = self._validated_baselines.get(baseline_id)
        if cached is not None:
            if cached[0] != source_generation:
                raise MissingHistoricalSnapshot(
                    "source data generation drifted during snapshot resolution"
                )
            if cached[1] != manifest:
                raise MissingHistoricalSnapshot(
                    "baseline manifest drifted during snapshot resolution"
                )
            return
        if self.eligible_agency_codes is None:
            raise MissingHistoricalSnapshot(
                "historical baseline validation requires eligible agency codes"
            )
        coverage = verify_baseline_manifest(
            self.procurement_conn,
            baseline_id,
            eligible_agency_codes=self.eligible_agency_codes,
        )
        if read_data_generation(self.procurement_conn) != source_generation:
            raise MissingHistoricalSnapshot(
                "source data generation drifted during baseline validation"
            )
        current_manifest = _manifest_row(self.procurement_conn, baseline_id)
        if current_manifest != manifest:
            raise MissingHistoricalSnapshot(
                "baseline manifest drifted during baseline validation"
            )
        if not coverage.complete:
            raise MissingHistoricalSnapshot(
                "pre-cutover snapshot baseline coverage is invalid"
            )
        self._validated_baselines[baseline_id] = (source_generation, manifest)

    def _stage(
        self,
        contract: CanonicalContract,
        bizno: str,
        share_pct: float,
        is_busan: bool | None,
        basis: str,
        baseline_id: str | None,
    ) -> None:
        normalized = normalize_bizno(bizno)
        if not normalized:
            raise ValueError("a normalized supplier business number is required")
        key = (*contract.identity, normalized)
        candidate = _SnapshotCandidate(contract, normalized, float(share_pct), is_busan, basis, baseline_id)
        prior = self._pending.get(key)
        if prior is not None and prior != candidate:
            raise SnapshotContentMismatch("staged snapshot identity has divergent content")
        self._pending[key] = candidate

    def seed(
        self,
        row: CanonicalContract | Mapping[str, Any],
        bizno: str,
        share_pct: float,
        is_busan: bool | None,
        basis: str,
        baseline_id: str,
    ) -> None:
        self._stage(self._contract(row), bizno, share_pct, is_busan, basis, baseline_id)

    def _is_pre_cutover(self, contract: CanonicalContract) -> bool:
        effective, date_only = _parse_at(contract.contract_date)
        if effective is None:
            raise UnknownLocality("contract governing date is unknown")
        if date_only:
            return effective.date() <= self._cutover.date()
        return effective <= self._cutover

    def _inherited(self, contract: CanonicalContract, bizno: str) -> bool | None | object:
        current_rank = _revision_rank(contract.contract_revision)
        rows = self.procurement_conn.execute(
            """
            SELECT contract_revision, bizno, is_busan
            FROM contract_supplier_locality
            WHERE sector=? AND contract_key=? AND contract_revision<>?
            """,
            (contract.sector, contract.contract_key, contract.contract_revision),
        ).fetchall()
        prior_revisions = {
            row[0] for row in rows if _revision_rank(row[0]) < current_rank
        }
        for key, pending in self._pending.items():
            if (
                key[0] == contract.sector
                and key[1] == contract.contract_key
                and key[2] != contract.contract_revision
                and _revision_rank(key[2]) < current_rank
            ):
                prior_revisions.add(key[2])
        if not prior_revisions:
            return _MISSING
        immediate_revision = max(prior_revisions, key=_revision_rank)
        values = [
            row[2]
            for row in rows
            if row[0] == immediate_revision and row[1] == bizno
        ]
        pending = self._pending.get(
            (contract.sector, contract.contract_key, immediate_revision, bizno)
        )
        if pending is not None:
            values.append(None if pending.is_busan is None else int(pending.is_busan))
        if not values:
            return _MISSING
        if len(set(values)) != 1:
            raise SnapshotContentMismatch(
                "immediate prior revision has divergent supplier locality"
            )
        value = values[0]
        if value is None:
            raise UnknownLocality("inherited supplier locality is unknown")
        return bool(value)

    def _classify_new(
        self,
        contract: CanonicalContract,
        bizno: str,
        legacy_is_local: bool,
    ) -> tuple[bool, str]:
        inherited = self._inherited(contract, bizno)
        if inherited is not _MISSING:
            return bool(inherited), "inherited_revision"
        status = status_at(self.company_conn, bizno, contract.contract_date)
        if status is None:
            first_history = self.company_conn.execute(
                """
                SELECT source_effective_at, new_status
                FROM company_locality_event
                WHERE bizno=? AND disposition='applied'
                ORDER BY source_effective_at, id LIMIT 1
                """,
                (bizno,),
            ).fetchone()
            if first_history is None:
                return bool(legacy_is_local), "legacy_policy_at_contract"
            contract_at, date_only = _parse_at(contract.contract_date)
            first_at, _ = _parse_at(first_history[0])
            if (
                contract_at is not None
                and first_at is not None
                and contract_at < first_at
                and first_history[1] == "active_local"
                and not (date_only and contract_at.date() == first_at.date())
            ):
                return False, "pre_inbound_status_history"
            raise UnknownLocality("supplier locality is unknown at the contract governing date; same-day boundaries require resolution")
        if status == "unverified":
            raise UnknownLocality("supplier locality is unknown at the contract governing date; same-day boundaries require resolution")
        return status == "active_local", "status_history"

    def resolve(
        self,
        row: CanonicalContract | Mapping[str, Any],
        bizno: str,
        share_pct: float,
        legacy_is_local: bool,
    ) -> bool:
        if self.mode == "legacy":
            return bool(legacy_is_local)
        contract = self._contract(row)
        normalized = normalize_bizno(bizno)
        pre_cutover = self._is_pre_cutover(contract)
        existing = self._existing(contract, normalized)
        fingerprint = content_fingerprint(contract)
        if existing is not None:
            if existing[3] != fingerprint:
                raise SnapshotContentMismatch("snapshot content fingerprint does not match canonical contract")
            if _share_micros(existing[0]) != _share_micros(share_pct):
                raise SnapshotContentMismatch("snapshot supplier share does not match canonical contract")
            if self.mode == "snapshot" and pre_cutover:
                self._validate_historical_snapshot(
                    contract, normalized, share_pct, existing
                )
            if existing[1] is None:
                raise UnknownLocality("frozen supplier locality is unknown")
            return bool(legacy_is_local) if self.mode == "shadow" else bool(existing[1])
        if self.mode == "snapshot" and pre_cutover:
            raise MissingHistoricalSnapshot(
                f"pre-cutover contract has no frozen historical snapshot: {contract.identity}/{normalized}"
            )
        if pre_cutover:
            decision, basis = bool(legacy_is_local), "legacy_shadow_candidate"
        else:
            decision, basis = self._classify_new(contract, normalized, legacy_is_local)
            if self.mode == "shadow":
                basis += "_shadow"
        self._stage(contract, normalized, share_pct, decision, basis, None)
        return bool(legacy_is_local) if self.mode == "shadow" else decision

    def flush(self) -> int:
        from maintenance_lock import guarded_write_session

        if self.mode == "legacy" or not self._pending:
            return 0
        inserted = 0
        with guarded_write_session(self.procurement_conn):
            baseline_ids = sorted(
                {candidate.baseline_id for candidate in self._pending.values() if candidate.baseline_id}
            )
            for baseline_id in baseline_ids:
                existing_manifest = _manifest_row(self.procurement_conn, baseline_id)
                if existing_manifest is None:
                    self.procurement_conn.execute(
                        "INSERT INTO locality_baseline_manifest VALUES (?, ?, ?, ?, ?, '', 0, 0, 0, 0, 'building', ?)",
                        (
                            baseline_id,
                            self.cutover_at,
                            CLASSIFIER_VERSION,
                            ITERATOR_VERSION,
                            self.generation_id,
                            self.now,
                        ),
                    )
                elif (
                    existing_manifest.cutover_at != self.cutover_at
                    or existing_manifest.classifier_version != CLASSIFIER_VERSION
                    or existing_manifest.iterator_version != ITERATOR_VERSION
                    or existing_manifest.cache_generation_id != self.generation_id
                ):
                    raise BaselineManifestConflict(
                        "baseline seed metadata differs from the existing manifest"
                    )
            for key, candidate in sorted(self._pending.items()):
                existing = self.procurement_conn.execute(
                    """
                    SELECT share_pct, is_busan, basis, content_fingerprint, baseline_id,
                           introduced_generation_id
                    FROM contract_supplier_locality
                    WHERE sector=? AND contract_key=? AND contract_revision=? AND bizno=?
                    """,
                    key,
                ).fetchone()
                expected = (
                    candidate.share_pct,
                    None if candidate.is_busan is None else int(candidate.is_busan),
                    candidate.basis,
                    content_fingerprint(candidate.contract),
                    candidate.baseline_id,
                    self.generation_id,
                )
                if existing is not None:
                    if existing != expected:
                        raise SnapshotContentMismatch("stored snapshot identity has divergent content or fingerprint")
                    continue
                self.procurement_conn.execute(
                    """
                    INSERT INTO contract_supplier_locality
                    (sector, contract_key, contract_revision, bizno, share_pct, is_busan,
                     basis, contract_date, classified_at, classifier_version,
                     content_fingerprint, baseline_id, introduced_generation_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        *key,
                        candidate.share_pct,
                        None if candidate.is_busan is None else int(candidate.is_busan),
                        candidate.basis,
                        candidate.contract.contract_date,
                        self.now,
                        CLASSIFIER_VERSION,
                        content_fingerprint(candidate.contract),
                        candidate.baseline_id,
                        self.generation_id,
                    ),
                )
                inserted += 1
        self._pending.clear()
        return inserted


_MISSING = object()
