"""Frozen supplier-locality snapshots and exact historical baseline manifests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
import re
import sqlite3
from typing import Any, Callable, Collection, Iterable, Mapping
from zoneinfo import ZoneInfo

from company_locality import normalize_bizno, status_at
from contract_population import (
    ITERATOR_VERSION,
    CanonicalContract,
    canonical_contract_from_row,
    content_fingerprint,
    iter_canonical_contracts,
    normalize_contract_key,
)


SEOUL = ZoneInfo("Asia/Seoul")
CLASSIFIER_VERSION = "supplier_locality_snapshot_v1"
VALID_MODES = {"legacy", "shadow", "snapshot"}
REQUIRED_SECTORS = ("공사", "용역", "물품", "쇼핑몰")
CANONICAL_SOURCE_TABLES = (
    "cnstwk_cntrct",
    "servc_cntrct",
    "thng_cntrct",
    "shopping_cntrct",
)
CANONICAL_EVIDENCE_TABLES = (
    "bid_notices_raw",
    "busan_award_cnstwk",
    "busan_award_servc",
    "busan_award_thng",
)
_GENERATION_OWNER_PREFIX = "@generation:"


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
class BaselineVerification:
    baseline_id: str
    source_data_generation: int
    source_schema_version: int
    source_table_signature: str
    source_trigger_signature: str
    eligible_agencies_fingerprint: str
    source_population_fingerprint: str
    manifest_fingerprint: str
    contract_fingerprint: str
    supplier_fingerprint: str
    snapshot_fingerprint: str
    proof_fingerprint: str
    verified_at: str


@dataclass(frozen=True)
class _SnapshotCandidate:
    contract: CanonicalContract
    bizno: str
    share_pct: float
    is_busan: bool | None
    basis: str
    baseline_id: str | None


@dataclass(frozen=True)
class _FrozenBindings:
    named: bool
    values: tuple[Any, ...]


@dataclass(frozen=True)
class _SourceSchemaOperation:
    kind: str
    target: str
    sql: str
    destination: str | None
    index_name: str | None
    columns: tuple[str, ...]
    rows: tuple[_FrozenBindings, ...]
    many: bool


@dataclass(frozen=True)
class _RefreshControlBoundary:
    clock: tuple[int, int]
    state: tuple[tuple[Any, ...], ...]
    audit: tuple[tuple[Any, ...], ...]
    audit_sequence: int | None


class SourceSchemaEditor:
    """Record an immutable source-only DDL/DML plan without touching SQLite."""

    __slots__ = ("__operations", "__sealed")

    def __init__(self) -> None:
        self.__operations: list[_SourceSchemaOperation] = []
        self.__sealed = False

    def execute(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Iterable[Any] = (),
    ) -> None:
        self.__append(sql, (_freeze_bindings(parameters),), many=False)

    def executemany(
        self,
        sql: str,
        parameter_rows: Iterable[Mapping[str, Any] | Iterable[Any]],
    ) -> None:
        self.__append(
            sql,
            tuple(_freeze_bindings(parameters) for parameters in parameter_rows),
            many=True,
        )

    def __append(
        self,
        sql: str,
        rows: tuple[_FrozenBindings, ...],
        *,
        many: bool,
    ) -> None:
        if self.__sealed:
            raise SnapshotError("source schema replacement plan is already sealed")
        (
            kind,
            target,
            normalized_sql,
            destination,
            index_name,
            columns,
        ) = _validate_source_operation(sql)
        self.__operations.append(
            _SourceSchemaOperation(
                kind=kind,
                target=target,
                sql=normalized_sql,
                destination=destination,
                index_name=index_name,
                columns=columns,
                rows=rows,
                many=many,
            )
        )


def _seal_source_schema_plan(
    editor: SourceSchemaEditor,
) -> tuple[_SourceSchemaOperation, ...]:
    editor._SourceSchemaEditor__sealed = True
    operations = tuple(editor._SourceSchemaEditor__operations)
    editor._SourceSchemaEditor__operations.clear()
    return operations


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


def _create_contract_supplier_locality(conn: sqlite3.Connection) -> None:
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
            baseline_id TEXT NOT NULL DEFAULT '',
            introduced_generation_id TEXT NOT NULL,
            PRIMARY KEY(baseline_id, sector, contract_key, contract_revision, bizno)
        ) WITHOUT ROWID
        """
    )


def _migrate_contract_supplier_locality(conn: sqlite3.Connection) -> None:
    columns = conn.execute("PRAGMA table_info(contract_supplier_locality)").fetchall()
    if not columns:
        _create_contract_supplier_locality(conn)
        return
    primary_key = [row[1] for row in sorted(columns, key=lambda row: row[5]) if row[5]]
    if primary_key == [
        "baseline_id",
        "sector",
        "contract_key",
        "contract_revision",
        "bizno",
    ]:
        return
    if primary_key != ["sector", "contract_key", "contract_revision", "bizno"]:
        raise SnapshotError("unsupported contract locality snapshot schema")
    conn.execute(
        "ALTER TABLE contract_supplier_locality "
        "RENAME TO contract_supplier_locality_unversioned"
    )
    _create_contract_supplier_locality(conn)
    conn.execute(
        """
        INSERT INTO contract_supplier_locality
        (sector, contract_key, contract_revision, bizno, share_pct, is_busan,
         basis, contract_date, classified_at, classifier_version,
         content_fingerprint, baseline_id, introduced_generation_id)
        SELECT sector, contract_key, contract_revision, bizno, share_pct, is_busan,
               basis, contract_date, classified_at, classifier_version,
               content_fingerprint,
               COALESCE(baseline_id, ? || introduced_generation_id),
               introduced_generation_id
        FROM contract_supplier_locality_unversioned
        """,
        (_GENERATION_OWNER_PREFIX,),
    )
    conn.execute("DROP TABLE contract_supplier_locality_unversioned")


def _create_snapshot_tables(conn: sqlite3.Connection) -> None:
    _migrate_contract_supplier_locality(conn)
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
        CREATE TABLE IF NOT EXISTS locality_source_schema_state (
            singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
            schema_version INTEGER NOT NULL,
            protected_tables TEXT NOT NULL,
            table_signature TEXT NOT NULL,
            trigger_signature TEXT NOT NULL,
            refreshed_at TEXT NOT NULL,
            operator TEXT NOT NULL,
            reason TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS locality_source_schema_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operator TEXT NOT NULL,
            reason TEXT NOT NULL,
            schema_version_before INTEGER NOT NULL,
            schema_version_after INTEGER NOT NULL,
            protected_tables TEXT NOT NULL,
            table_signature TEXT NOT NULL,
            trigger_signature TEXT NOT NULL,
            refreshed_at TEXT NOT NULL
        )
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS locality_baseline_verification (
            baseline_id TEXT PRIMARY KEY,
            source_data_generation INTEGER NOT NULL,
            source_schema_version INTEGER NOT NULL,
            source_table_signature TEXT NOT NULL,
            source_trigger_signature TEXT NOT NULL,
            eligible_agencies_fingerprint TEXT NOT NULL,
            source_population_fingerprint TEXT NOT NULL,
            manifest_fingerprint TEXT NOT NULL,
            contract_fingerprint TEXT NOT NULL,
            supplier_fingerprint TEXT NOT NULL,
            snapshot_fingerprint TEXT NOT NULL,
            proof_fingerprint TEXT NOT NULL,
            verified_at TEXT NOT NULL,
            FOREIGN KEY(baseline_id) REFERENCES locality_baseline_manifest(baseline_id)
        ) WITHOUT ROWID
        """
    )


def _clock_trigger_sql(table: str, action: str, clock: str) -> str:
    name = f"locality_{table}_{action.lower()}_clock"
    return (
        f"CREATE TRIGGER {name} AFTER {action} ON {table} BEGIN "
        f"UPDATE locality_generation_clock SET {clock} = {clock} + 1 "
        "WHERE singleton_id = 1; END"
    )


def _guard_trigger_sql(table: str, action: str) -> str:
    name = f"locality_{table}_{action.lower()}_guard"
    return (
        f"CREATE TRIGGER {name} BEFORE {action} ON {table} "
        "WHEN locality_guarded_write() = 0 OR "
        "(locality_guarded_write() != 2 AND COALESCE((SELECT writes_enabled "
        "FROM locality_activation_state WHERE singleton_id = 1), 0) = 0) "
        "BEGIN SELECT RAISE(ABORT, "
        "'locality protected write requires guarded session'); END"
    )


def _install_table_triggers(
    conn: sqlite3.Connection,
    table: str,
    clock: str,
    *,
    replace: bool = False,
) -> None:
    for action in ("INSERT", "UPDATE", "DELETE"):
        for name, sql in (
            (f"locality_{table}_{action.lower()}_clock", _clock_trigger_sql(table, action, clock)),
            (f"locality_{table}_{action.lower()}_guard", _guard_trigger_sql(table, action)),
        ):
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?",
                (name,),
            ).fetchone()
            if replace and exists:
                conn.execute(f"DROP TRIGGER {name}")
                exists = None
            if exists is None:
                conn.execute(sql)


def _existing_protected_source_tables(conn: sqlite3.Connection) -> tuple[str, ...]:
    existing_tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    return tuple(
        table
        for table in (*CANONICAL_SOURCE_TABLES, *CANONICAL_EVIDENCE_TABLES)
        if table in existing_tables
    )


def _install_snapshot_triggers(conn: sqlite3.Connection) -> None:
    data_tables = _existing_protected_source_tables(conn)
    control_tables = (
        "contract_supplier_locality",
        "locality_baseline_manifest",
        "locality_baseline_contract",
        "locality_baseline_supplier",
        "locality_baseline_verification",
        "locality_source_schema_state",
        "locality_source_schema_audit",
    )
    for table, clock in (
        *((table, "data_generation") for table in data_tables),
        *((table, "control_revision") for table in control_tables),
    ):
        _install_table_triggers(conn, table, clock)


def _install_complete_baseline_immutability(conn: sqlite3.Connection) -> None:
    triggers = {
        "locality_complete_manifest_insert_immutable": """
            BEFORE INSERT ON locality_baseline_manifest
            WHEN NEW.status = 'complete'
        """,
        "locality_complete_manifest_update_immutable": """
            BEFORE UPDATE ON locality_baseline_manifest
            WHEN OLD.status = 'complete'
        """,
        "locality_complete_manifest_delete_immutable": """
            BEFORE DELETE ON locality_baseline_manifest
            WHEN OLD.status = 'complete'
        """,
    }
    child_tables = (
        "locality_baseline_contract",
        "locality_baseline_supplier",
        "locality_baseline_verification",
        "contract_supplier_locality",
    )
    for table in child_tables:
        triggers[f"locality_{table}_complete_insert_immutable"] = f"""
            BEFORE INSERT ON {table}
            WHEN EXISTS (
                SELECT 1 FROM locality_baseline_manifest
                WHERE baseline_id = NEW.baseline_id AND status = 'complete'
            )
        """
        triggers[f"locality_{table}_complete_update_immutable"] = f"""
            BEFORE UPDATE ON {table}
            WHEN EXISTS (
                SELECT 1 FROM locality_baseline_manifest
                WHERE baseline_id IN (OLD.baseline_id, NEW.baseline_id)
                  AND status = 'complete'
            )
        """
        triggers[f"locality_{table}_complete_delete_immutable"] = f"""
            BEFORE DELETE ON {table}
            WHEN EXISTS (
                SELECT 1 FROM locality_baseline_manifest
                WHERE baseline_id = OLD.baseline_id AND status = 'complete'
            )
        """
    for name, clause in triggers.items():
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?",
            (name,),
        ).fetchone()
        if exists is None:
            conn.execute(
                f"CREATE TRIGGER {name} {clause} BEGIN "
                "SELECT RAISE(ABORT, 'complete baseline is immutable'); END"
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
            _install_complete_baseline_immutability(conn)
        if owns_transaction:
            conn.commit()


def _normalize_sql(sql: str) -> str:
    return " ".join(str(sql or "").split())


def _expected_source_trigger_sql(tables: Collection[str]) -> dict[str, str]:
    expected: dict[str, str] = {}
    for table in sorted(tables):
        for action in ("INSERT", "UPDATE", "DELETE"):
            expected[f"locality_{table}_{action.lower()}_clock"] = _clock_trigger_sql(
                table, action, "data_generation"
            )
            expected[f"locality_{table}_{action.lower()}_guard"] = _guard_trigger_sql(
                table, action
            )
    return expected


def _signature(payload: object) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _source_trigger_surface(
    conn: sqlite3.Connection, tables: Collection[str]
) -> dict[str, str]:
    table_names = tuple(sorted(tables))
    if not table_names:
        return {}
    placeholders = ",".join("?" for _ in table_names)
    return {
        name: _normalize_sql(sql)
        for name, sql in conn.execute(
            f"SELECT name, sql FROM sqlite_master WHERE type='trigger' "
            f"AND tbl_name IN ({placeholders})",
            table_names,
        )
    }


def _validate_source_trigger_surface(
    conn: sqlite3.Connection,
    tables: Collection[str],
    *,
    require_complete: bool,
) -> None:
    actual = _source_trigger_surface(conn, tables)
    expected = {
        name: _normalize_sql(sql)
        for name, sql in _expected_source_trigger_sql(tables).items()
    }
    if any(expected.get(name) != sql for name, sql in actual.items()):
        raise SnapshotError("source schema trigger surface contains an unapproved trigger")
    if require_complete and actual != expected:
        raise SnapshotError("source schema trigger surface is incomplete or divergent")


def _source_trigger_signature(conn: sqlite3.Connection, tables: Collection[str]) -> str:
    expected = _expected_source_trigger_sql(tables)
    actual = _source_trigger_surface(conn, tables)
    normalized_expected = {
        name: _normalize_sql(sql) for name, sql in expected.items()
    }
    if actual != normalized_expected:
        raise SnapshotError("source schema trigger surface is incomplete or divergent")
    return _signature(sorted(actual.items()))


def _expected_source_trigger_signature(tables: Collection[str]) -> str:
    expected = {
        name: _normalize_sql(sql)
        for name, sql in _expected_source_trigger_sql(tables).items()
    }
    return _signature(sorted(expected.items()))


def _source_table_signature(conn: sqlite3.Connection, tables: Collection[str]) -> str:
    selected_tables = set(tables)
    rows = conn.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master "
        "WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%' "
        "ORDER BY type, name"
    ).fetchall()
    selected = [
        (object_type, name, owner, _normalize_sql(sql))
        for object_type, name, owner, sql in rows
        if (object_type == "table" and name in selected_tables)
        or (object_type == "index" and owner in selected_tables)
    ]
    return _signature(selected)


def _assert_source_schema_protected(
    conn: sqlite3.Connection,
) -> tuple[int, tuple[str, ...], str, str]:
    row = conn.execute(
        """
        SELECT schema_version, protected_tables, table_signature, trigger_signature
        FROM locality_source_schema_state WHERE singleton_id=1
        """
    ).fetchone()
    if row is None:
        raise MissingHistoricalSnapshot(
            "source schema protection has not been refreshed and audited"
        )
    try:
        protected_tables = tuple(json.loads(row[1]))
    except (TypeError, json.JSONDecodeError) as error:
        raise MissingHistoricalSnapshot(
            "source schema protection metadata is invalid"
        ) from error
    if not set(CANONICAL_SOURCE_TABLES).issubset(protected_tables):
        raise MissingHistoricalSnapshot(
            "source schema protection is missing canonical source tables"
        )
    current_schema_version = conn.execute("PRAGMA schema_version").fetchone()[0]
    if current_schema_version != row[0]:
        raise MissingHistoricalSnapshot(
            "source schema protection drifted from its pinned schema_version"
        )
    if row[3] != _expected_source_trigger_signature(protected_tables):
        raise MissingHistoricalSnapshot(
            "source schema protection trigger signature is invalid"
        )
    if not row[2]:
        raise MissingHistoricalSnapshot(
            "source schema protection table signature is missing"
        )
    return (
        int(current_schema_version),
        protected_tables,
        str(row[2]),
        str(row[3]),
    )


_REFRESH_SAVEPOINT = "locality_source_schema_refresh_boundary"
_REFRESH_FAILURE_STAGES = frozenset(
    {
        "after_replacement",
        "after_trigger_reinstall",
        "after_state_update",
        "after_audit_insert",
        "before_return",
    }
)
_SOURCE_SCHEMA_TARGETS = frozenset(
    (*CANONICAL_SOURCE_TABLES, *CANONICAL_EVIDENCE_TABLES)
)
_SQL_IDENTIFIER = (
    r'(?:"[A-Za-z_][A-Za-z0-9_]*"|`[A-Za-z_][A-Za-z0-9_]*`|'
    r'\[[A-Za-z_][A-Za-z0-9_]*\]|[A-Za-z_][A-Za-z0-9_]*)'
)
_ALTER_RENAME_PATTERN = re.compile(
    rf"^ALTER\s+TABLE\s+({_SQL_IDENTIFIER})\s+RENAME\s+TO\s+"
    rf"({_SQL_IDENTIFIER})$",
    re.IGNORECASE,
)
_CREATE_INDEX_PATTERN = re.compile(
    rf"^CREATE\s+INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    rf"({_SQL_IDENTIFIER})\s+ON\s+({_SQL_IDENTIFIER})\s*\(\s*"
    rf"({_SQL_IDENTIFIER}(?:\s*,\s*{_SQL_IDENTIFIER})*)\s*\)$",
    re.IGNORECASE,
)
_SOURCE_OPERATION_PATTERNS = (
    (
        "create_table",
        re.compile(
            rf"^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?({_SQL_IDENTIFIER})(?![A-Za-z0-9_])",
            re.IGNORECASE,
        ),
    ),
    (
        "drop_table",
        re.compile(
            rf"^DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?({_SQL_IDENTIFIER})(?![A-Za-z0-9_])",
            re.IGNORECASE,
        ),
    ),
    (
        "insert",
        re.compile(
            rf"^INSERT(?:\s+OR\s+(?:ABORT|FAIL|IGNORE|REPLACE))?\s+INTO\s+({_SQL_IDENTIFIER})(?![A-Za-z0-9_])",
            re.IGNORECASE,
        ),
    ),
    (
        "replace",
        re.compile(
            rf"^REPLACE\s+INTO\s+({_SQL_IDENTIFIER})(?![A-Za-z0-9_])",
            re.IGNORECASE,
        ),
    ),
    (
        "update",
        re.compile(
            rf"^UPDATE(?:\s+OR\s+(?:ABORT|FAIL|IGNORE|REPLACE))?\s+({_SQL_IDENTIFIER})(?![A-Za-z0-9_])",
            re.IGNORECASE,
        ),
    ),
    (
        "delete",
        re.compile(
            rf"^DELETE\s+FROM\s+({_SQL_IDENTIFIER})(?![A-Za-z0-9_])",
            re.IGNORECASE,
        ),
    ),
)
_UNSAFE_SOURCE_SQL = re.compile(
    r"\b(?:ATTACH|BEGIN|COMMIT|DETACH|PRAGMA|RELEASE|RETURNING|ROLLBACK|SAVEPOINT|VACUUM)\b",
    re.IGNORECASE,
)
_CONTROL_SOURCE_SQL = re.compile(
    r"\b(?:contract_supplier_locality|locality_[A-Za-z0-9_]*|sqlite_sequence)\b",
    re.IGNORECASE,
)


def _freeze_sql_value(value: Any) -> Any:
    if isinstance(value, memoryview):
        return bytes(value)
    if value is None or isinstance(value, (bytes, float, int, str)):
        return value
    raise SnapshotError("source schema plan parameters must be immutable SQLite values")


def _freeze_bindings(
    parameters: Mapping[str, Any] | Iterable[Any],
) -> _FrozenBindings:
    if isinstance(parameters, Mapping):
        items = []
        if not all(isinstance(key, str) for key in parameters):
            raise SnapshotError("source schema named parameter keys must be strings")
        for key, value in sorted(parameters.items()):
            items.append((key, _freeze_sql_value(value)))
        return _FrozenBindings(True, tuple(items))
    if isinstance(parameters, (bytes, str)):
        raise SnapshotError("source schema positional parameters must be a sequence")
    return _FrozenBindings(
        False, tuple(_freeze_sql_value(value) for value in parameters)
    )


def _thaw_bindings(bindings: _FrozenBindings) -> Mapping[str, Any] | tuple[Any, ...]:
    return dict(bindings.values) if bindings.named else bindings.values


def _unquote_sql_identifier(value: str) -> str:
    if value[:1] in ('"', "`", "["):
        return value[1:-1]
    return value


def _validate_source_operation(
    sql: str,
) -> tuple[str, str, str, str | None, str | None, tuple[str, ...]]:
    statement = str(sql).strip()
    if statement.endswith(";"):
        statement = statement[:-1].rstrip()
    if (
        not statement
        or ";" in statement
        or "--" in statement
        or "/*" in statement
        or "*/" in statement
        or _UNSAFE_SOURCE_SQL.search(statement)
    ):
        raise SnapshotError(
            "source schema plan rejects transaction control, control tables, or unsafe SQL"
        )

    alter_match = _ALTER_RENAME_PATTERN.fullmatch(statement)
    if alter_match is not None:
        target = _unquote_sql_identifier(alter_match.group(1)).lower()
        destination = _unquote_sql_identifier(alter_match.group(2)).lower()
        if target not in _SOURCE_SCHEMA_TARGETS:
            raise SnapshotError(
                f"source schema plan target is not approved: {target}"
            )
        if destination not in _SOURCE_SCHEMA_TARGETS:
            raise SnapshotError(
                "source schema plan ALTER destination is not approved: "
                f"{destination}"
            )
        return "alter_rename", target, statement, destination, None, ()
    if statement.upper().startswith("ALTER"):
        raise SnapshotError("source schema plan operation kind is not approved")

    index_match = _CREATE_INDEX_PATTERN.fullmatch(statement)
    if index_match is not None:
        index_name = _unquote_sql_identifier(index_match.group(1)).lower()
        target = _unquote_sql_identifier(index_match.group(2)).lower()
        columns = tuple(
            _unquote_sql_identifier(column.strip()).lower()
            for column in index_match.group(3).split(",")
        )
        if target not in _SOURCE_SCHEMA_TARGETS:
            raise SnapshotError(
                f"source schema plan target is not approved: {target}"
            )
        if index_name.startswith(("sqlite_", "locality_")):
            raise SnapshotError("source schema plan index name is not approved")
        if len(set(columns)) != len(columns):
            raise SnapshotError("source schema plan index columns must be unique")
        return "create_index", target, statement, None, index_name, columns
    if statement.upper().startswith("CREATE INDEX"):
        raise SnapshotError("source schema plan index definition is not approved")

    if _CONTROL_SOURCE_SQL.search(statement):
        raise SnapshotError(
            "source schema plan rejects transaction control, control tables, or unsafe SQL"
        )
    for kind, pattern in _SOURCE_OPERATION_PATTERNS:
        match = pattern.match(statement)
        if match is None:
            continue
        target = _unquote_sql_identifier(match.group(1)).lower()
        if target not in _SOURCE_SCHEMA_TARGETS:
            raise SnapshotError(
                f"source schema plan target is not approved: {target}"
            )
        return kind, target, statement, None, None, ()
    raise SnapshotError("source schema plan operation kind is not approved")


def _validate_source_index(
    conn: sqlite3.Connection,
    operation: _SourceSchemaOperation,
    *,
    require_existing: bool,
) -> None:
    columns = {
        str(row[1]).lower()
        for row in conn.execute(f'PRAGMA table_info("{operation.target}")')
    }
    if not columns:
        raise SnapshotError(
            f"source schema plan index owner does not exist: {operation.target}"
        )
    missing = [column for column in operation.columns if column not in columns]
    if missing:
        raise SnapshotError(
            "source schema plan index column is not approved by the owner schema: "
            + ", ".join(missing)
        )
    existing = conn.execute(
        "SELECT name, tbl_name, sql FROM sqlite_master "
        "WHERE type='index' AND lower(name)=?",
        (operation.index_name,),
    ).fetchone()
    if existing is None:
        if require_existing:
            raise SnapshotError("source schema plan index was not created")
        return
    try:
        existing_spec = _validate_source_operation(existing[2])
    except SnapshotError as error:
        raise SnapshotError(
            "source schema plan index name collides with an unapproved definition"
        ) from error
    if (
        existing_spec[0] != "create_index"
        or str(existing[1]).lower() != operation.target
        or existing_spec[1] != operation.target
        or existing_spec[4] != operation.index_name
        or existing_spec[5] != operation.columns
    ):
        raise SnapshotError(
            "source schema plan index name collides with an unapproved definition"
        )


def _complete_user_schema(
    conn: sqlite3.Connection,
) -> dict[tuple[str, str], tuple[str, str]]:
    return {
        (str(object_type).lower(), str(name).lower()): (
            str(owner).lower(),
            _normalize_sql(sql),
        )
        for object_type, name, owner, sql in conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE type IN ('table','index','view','trigger') "
            "AND name NOT LIKE 'sqlite_%' ORDER BY type, name"
        )
    }


def _validate_source_schema_effects(
    before: Mapping[tuple[str, str], tuple[str, str]],
    after: Mapping[tuple[str, str], tuple[str, str]],
    protected_before: Collection[str],
    protected_after: Collection[str],
    operations: Collection[_SourceSchemaOperation],
) -> None:
    required_tables = set(CANONICAL_SOURCE_TABLES) | set(protected_before)
    missing = sorted(required_tables - set(protected_after))
    if missing:
        raise SnapshotError(
            "source schema refresh requires protected source tables: "
            + ", ".join(missing)
        )

    permitted_triggers = set(
        _expected_source_trigger_sql(set(protected_before) | set(protected_after))
    )
    planned_indexes = {
        operation.index_name: operation.target
        for operation in operations
        if operation.kind == "create_index" and operation.index_name is not None
    }
    for key in sorted(set(before) | set(after)):
        if before.get(key) == after.get(key):
            continue
        object_type, name = key
        current = after.get(key)
        if object_type == "table" and name in _SOURCE_SCHEMA_TARGETS:
            continue
        if object_type == "trigger" and name in permitted_triggers:
            continue
        if (
            object_type == "index"
            and name in planned_indexes
            and current is not None
            and current[0] == planned_indexes[name]
        ):
            continue
        raise SnapshotError(
            "source schema refresh left an unapproved schema effect: "
            f"{object_type} {name}"
        )


def _refresh_control_boundary(conn: sqlite3.Connection) -> _RefreshControlBoundary:
    clock_row = conn.execute(
        "SELECT data_generation, control_revision "
        "FROM locality_generation_clock WHERE singleton_id=1"
    ).fetchone()
    if clock_row is None:
        raise SnapshotError("source schema refresh control clock is missing")
    sequence_row = conn.execute(
        "SELECT seq FROM sqlite_sequence "
        "WHERE name='locality_source_schema_audit'"
    ).fetchone()
    return _RefreshControlBoundary(
        clock=(int(clock_row[0]), int(clock_row[1])),
        state=tuple(
            conn.execute(
                "SELECT * FROM locality_source_schema_state ORDER BY singleton_id"
            ).fetchall()
        ),
        audit=tuple(
            conn.execute(
                "SELECT * FROM locality_source_schema_audit ORDER BY id"
            ).fetchall()
        ),
        audit_sequence=int(sequence_row[0]) if sequence_row is not None else None,
    )


def _assert_unchanged_refresh_boundary(
    conn: sqlite3.Connection, expected: _RefreshControlBoundary
) -> None:
    if _refresh_control_boundary(conn) != expected:
        raise SnapshotError(
            "source schema plan changed refresh state, audit, or control clocks"
        )


def _execute_source_schema_plan(
    conn: sqlite3.Connection,
    operations: tuple[_SourceSchemaOperation, ...],
) -> None:
    for operation in operations:
        if not isinstance(operation, _SourceSchemaOperation):
            raise SnapshotError("source schema plan contains an invalid operation")
        validated = _validate_source_operation(operation.sql)
        if validated != (
            operation.kind,
            operation.target,
            operation.sql,
            operation.destination,
            operation.index_name,
            operation.columns,
        ):
            raise SnapshotError("source schema plan changed after validation")
        if operation.many and operation.kind in {
            "alter_rename",
            "create_index",
            "create_table",
            "drop_table",
        }:
            raise SnapshotError("source schema DDL cannot use executemany")
        if operation.kind == "create_index":
            _validate_source_index(conn, operation, require_existing=False)
        if operation.many:
            conn.executemany(
                operation.sql,
                tuple(_thaw_bindings(row) for row in operation.rows),
            )
        else:
            if len(operation.rows) != 1:
                raise SnapshotError("source schema execute operation has invalid bindings")
            conn.execute(operation.sql, _thaw_bindings(operation.rows[0]))
        if operation.kind == "create_index":
            _validate_source_index(conn, operation, require_existing=True)


def _drop_source_triggers(conn: sqlite3.Connection, tables: Collection[str]) -> None:
    for name in sorted(_expected_source_trigger_sql(tables)):
        conn.execute(f"DROP TRIGGER IF EXISTS {name}")


def _assert_refresh_transaction(conn: sqlite3.Connection) -> None:
    if not conn.in_transaction:
        raise SnapshotError("source schema refresh lost its guarded transaction")


def refresh_source_schema(
    conn: sqlite3.Connection,
    *,
    operator: str,
    reason: str,
    replace: Callable[[SourceSchemaEditor], None] | None = None,
    fail_after: str | None = None,
) -> None:
    """Coordinate approved source DDL, reinstall guards, audit, and advance data."""
    from maintenance_lock import guarded_write_session

    if not str(operator).strip() or not str(reason).strip():
        raise ValueError("source schema refresh requires operator and reason")
    if conn.in_transaction:
        raise SnapshotError("source schema refresh must own the guarded transaction")
    if fail_after is not None and fail_after not in _REFRESH_FAILURE_STAGES:
        raise ValueError("unknown source schema refresh failure stage")

    plan: tuple[_SourceSchemaOperation, ...] = ()
    if replace is not None:
        editor = SourceSchemaEditor()
        replace(editor)
        plan = _seal_source_schema_plan(editor)
    normalized_operator = str(operator).strip()
    normalized_reason = str(reason).strip()

    def inject(stage: str) -> None:
        if fail_after == stage:
            raise RuntimeError(f"injected source schema refresh failure: {stage}")

    with guarded_write_session(conn):
        _assert_refresh_transaction(conn)
        conn.execute(f"SAVEPOINT {_REFRESH_SAVEPOINT}")
        schema_version_before = conn.execute("PRAGMA schema_version").fetchone()[0]
        protected_before = _existing_protected_source_tables(conn)
        _validate_source_trigger_surface(
            conn, protected_before, require_complete=bool(plan)
        )
        schema_before = _complete_user_schema(conn)
        control_before = _refresh_control_boundary(conn)
        _drop_source_triggers(conn, protected_before)
        _execute_source_schema_plan(conn, plan)
        _assert_refresh_transaction(conn)
        _assert_unchanged_refresh_boundary(conn, control_before)
        inject("after_replacement")
        protected_tables = _existing_protected_source_tables(conn)
        for table in protected_tables:
            _install_table_triggers(
                conn, table, "data_generation", replace=True
            )
        _assert_refresh_transaction(conn)
        _assert_unchanged_refresh_boundary(conn, control_before)
        trigger_signature = _source_trigger_signature(conn, protected_tables)
        expected_signature = _expected_source_trigger_signature(protected_tables)
        if trigger_signature != expected_signature:
            raise SnapshotError("source schema trigger signature validation failed")
        schema_after = _complete_user_schema(conn)
        _validate_source_schema_effects(
            schema_before,
            schema_after,
            protected_before,
            protected_tables,
            plan,
        )
        inject("after_trigger_reinstall")
        table_signature = _source_table_signature(conn, protected_tables)
        protected_json = json.dumps(
            protected_tables, ensure_ascii=False, separators=(",", ":")
        )
        schema_version_after = conn.execute("PRAGMA schema_version").fetchone()[0]
        refreshed_at = _now()
        conn.execute(
            """
            INSERT INTO locality_source_schema_state
            (singleton_id, schema_version, protected_tables, table_signature,
             trigger_signature, refreshed_at, operator, reason)
            VALUES (1, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(singleton_id) DO UPDATE SET
                schema_version=excluded.schema_version,
                protected_tables=excluded.protected_tables,
                table_signature=excluded.table_signature,
                trigger_signature=excluded.trigger_signature,
                refreshed_at=excluded.refreshed_at,
                operator=excluded.operator,
                reason=excluded.reason
            """,
            (
                schema_version_after,
                protected_json,
                table_signature,
                trigger_signature,
                refreshed_at,
                normalized_operator,
                normalized_reason,
            ),
        )
        _assert_refresh_transaction(conn)
        expected_state = (
            1,
            schema_version_after,
            protected_json,
            table_signature,
            trigger_signature,
            refreshed_at,
            normalized_operator,
            normalized_reason,
        )
        after_state = _refresh_control_boundary(conn)
        if (
            after_state.clock
            != (control_before.clock[0], control_before.clock[1] + 1)
            or after_state.state != (expected_state,)
            or after_state.audit != control_before.audit
            or after_state.audit_sequence != control_before.audit_sequence
        ):
            raise SnapshotError(
                "source schema refresh state update crossed its control boundary"
            )
        inject("after_state_update")
        conn.execute(
            """
            INSERT INTO locality_source_schema_audit
            (operator, reason, schema_version_before, schema_version_after,
             protected_tables, table_signature, trigger_signature, refreshed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_operator,
                normalized_reason,
                schema_version_before,
                schema_version_after,
                protected_json,
                table_signature,
                trigger_signature,
                refreshed_at,
            ),
        )
        _assert_refresh_transaction(conn)
        expected_audit_id = (control_before.audit_sequence or 0) + 1
        expected_audit = (
            expected_audit_id,
            normalized_operator,
            normalized_reason,
            schema_version_before,
            schema_version_after,
            protected_json,
            table_signature,
            trigger_signature,
            refreshed_at,
        )
        after_audit = _refresh_control_boundary(conn)
        if (
            after_audit.clock
            != (control_before.clock[0], control_before.clock[1] + 2)
            or after_audit.state != (expected_state,)
            or after_audit.audit != control_before.audit + (expected_audit,)
            or after_audit.audit_sequence != expected_audit_id
        ):
            raise SnapshotError(
                "source schema refresh audit insert crossed its control boundary"
            )
        inject("after_audit_insert")
        conn.execute(
            "UPDATE locality_generation_clock "
            "SET data_generation=data_generation+1 WHERE singleton_id=1"
        )
        _assert_refresh_transaction(conn)
        after_clock = _refresh_control_boundary(conn)
        if (
            after_clock.clock
            != (control_before.clock[0] + 1, control_before.clock[1] + 2)
            or after_clock.state != (expected_state,)
            or after_clock.audit != control_before.audit + (expected_audit,)
            or after_clock.audit_sequence != expected_audit_id
        ):
            raise SnapshotError(
                "source schema refresh did not advance exactly one data generation"
            )
        inject("before_return")
        conn.execute(f"RELEASE {_REFRESH_SAVEPOINT}")


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


def _eligible_agencies_fingerprint(codes: Collection[str]) -> str:
    return _signature(sorted(normalize_contract_key(code) for code in codes))


def _verification_row(
    conn: sqlite3.Connection, baseline_id: str
) -> BaselineVerification | None:
    row = conn.execute(
        """
        SELECT baseline_id, source_data_generation, source_schema_version,
               source_table_signature, source_trigger_signature,
               eligible_agencies_fingerprint, source_population_fingerprint,
               manifest_fingerprint, contract_fingerprint,
               supplier_fingerprint, snapshot_fingerprint,
               proof_fingerprint, verified_at
        FROM locality_baseline_verification WHERE baseline_id=?
        """,
        (baseline_id,),
    ).fetchone()
    return BaselineVerification(*row) if row else None


def _baseline_owned_fingerprints(
    conn: sqlite3.Connection,
    baseline_id: str,
    manifest: BaselineManifest,
) -> tuple[str, str, str, str]:
    complete_manifest = [
        manifest.baseline_id,
        manifest.cutover_at,
        manifest.classifier_version,
        manifest.iterator_version,
        manifest.cache_generation_id,
        manifest.manifest_fingerprint,
        manifest.expected_contracts,
        manifest.expected_suppliers,
        manifest.expected_share_micros,
        manifest.expected_amount_won,
        "complete",
        manifest.created_at,
    ]
    manifest_fingerprint = _signature(complete_manifest)
    contract_rows = conn.execute(
        """
        SELECT baseline_id, sector, contract_key, contract_revision,
               amount_won, content_fingerprint
        FROM locality_baseline_contract WHERE baseline_id=?
        ORDER BY sector, contract_key, contract_revision
        """,
        (baseline_id,),
    ).fetchall()
    supplier_rows = conn.execute(
        """
        SELECT baseline_id, sector, contract_key, contract_revision,
               bizno, share_micros
        FROM locality_baseline_supplier WHERE baseline_id=?
        ORDER BY sector, contract_key, contract_revision, bizno
        """,
        (baseline_id,),
    ).fetchall()
    snapshot_rows = conn.execute(
        """
        SELECT baseline_id, sector, contract_key, contract_revision, bizno,
               share_pct, is_busan, basis, contract_date, classified_at,
               classifier_version, content_fingerprint,
               introduced_generation_id
        FROM contract_supplier_locality WHERE baseline_id=?
        ORDER BY sector, contract_key, contract_revision, bizno
        """,
        (baseline_id,),
    ).fetchall()
    normalized_snapshots = [
        (*row[:5], _share_micros(row[5]), *row[6:]) for row in snapshot_rows
    ]
    return (
        manifest_fingerprint,
        _signature(contract_rows),
        _signature(supplier_rows),
        _signature(normalized_snapshots),
    )


def _proof_fingerprint(values: tuple[Any, ...]) -> str:
    return _signature(values)


def _validate_baseline_verification(
    conn: sqlite3.Connection,
    baseline_id: str,
    manifest: BaselineManifest,
    eligible_agency_codes: Collection[str],
) -> BaselineVerification:
    proof = _verification_row(conn, baseline_id)
    if proof is None:
        raise MissingHistoricalSnapshot(
            "completed historical baseline has no source-verification evidence"
        )
    owned = _baseline_owned_fingerprints(conn, baseline_id, manifest)
    expected_eligible = _eligible_agencies_fingerprint(eligible_agency_codes)
    proof_values = (
        proof.baseline_id,
        proof.source_data_generation,
        proof.source_schema_version,
        proof.source_table_signature,
        proof.source_trigger_signature,
        proof.eligible_agencies_fingerprint,
        proof.source_population_fingerprint,
        *owned,
    )
    if (
        manifest.status != "complete"
        or proof.eligible_agencies_fingerprint != expected_eligible
        or proof.source_population_fingerprint != manifest.manifest_fingerprint
        or (
            proof.manifest_fingerprint,
            proof.contract_fingerprint,
            proof.supplier_fingerprint,
            proof.snapshot_fingerprint,
        )
        != owned
        or proof.proof_fingerprint != _proof_fingerprint(proof_values)
    ):
        raise MissingHistoricalSnapshot(
            "historical baseline source-verification evidence is invalid"
        )
    return proof


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
        conn.execute(
            "DELETE FROM locality_baseline_verification WHERE baseline_id = ?",
            (baseline_id,),
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
    with guarded_write_session(conn):
        from maintenance_lock import read_data_generation

        source_generation = read_data_generation(conn)
        source_schema: tuple[int, tuple[str, ...], str, str] | None = None
        try:
            source_schema = _assert_source_schema_protected(conn)
        except (MissingHistoricalSnapshot, sqlite3.Error):
            pass
        coverage = verify_baseline_manifest(
            conn,
            baseline_id,
            eligible_agency_codes=eligible_agency_codes,
        )
        current_manifest = _manifest_row(conn, baseline_id)
        source_stable = (
            source_schema is not None
            and read_data_generation(conn) == source_generation
            and _assert_source_schema_protected(conn) == source_schema
        )
        conn.execute(
            "DELETE FROM locality_baseline_verification WHERE baseline_id = ?",
            (baseline_id,),
        )
        if (
            coverage.complete
            and source_stable
            and current_manifest is not None
            and eligible_agency_codes is not None
        ):
            owned = _baseline_owned_fingerprints(
                conn, baseline_id, current_manifest
            )
            eligible_fingerprint = _eligible_agencies_fingerprint(
                eligible_agency_codes
            )
            proof_values = (
                baseline_id,
                source_generation,
                source_schema[0],
                source_schema[2],
                source_schema[3],
                eligible_fingerprint,
                current_manifest.manifest_fingerprint,
                *owned,
            )
            conn.execute(
                """
                INSERT INTO locality_baseline_verification
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (*proof_values, _proof_fingerprint(proof_values), _now()),
            )
            conn.execute(
                "UPDATE locality_baseline_manifest SET status='complete' WHERE baseline_id=?",
                (baseline_id,),
            )
            completed = _manifest_row(conn, baseline_id)
            if completed is None:
                raise RuntimeError("completed baseline manifest disappeared")
            _validate_baseline_verification(
                conn, baseline_id, completed, eligible_agency_codes
            )
        else:
            conn.execute(
                "UPDATE locality_baseline_manifest SET status='failed' WHERE baseline_id=?",
                (baseline_id,),
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
    try:
        _assert_source_schema_protected(conn)
    except (MissingHistoricalSnapshot, sqlite3.Error):
        source_enumeration_valid = False
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
        selected_baseline_id: str | None = None,
        eligible_agency_codes: Collection[str] | None = None,
    ) -> None:
        if mode not in VALID_MODES:
            raise ValueError("locality mode must be legacy, shadow, or snapshot")
        if mode != "legacy" and not eligible_agency_codes:
            raise ValueError(
                "eligible agency codes are required for shadow and snapshot modes"
            )
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
        self.selected_baseline_id = (
            str(selected_baseline_id).strip() if selected_baseline_id else None
        )
        self.eligible_agency_codes = (
            None
            if eligible_agency_codes is None
            else frozenset(eligible_agency_codes)
        )
        self._normalized_eligible_agencies = frozenset(
            normalize_contract_key(code)
            for code in (self.eligible_agency_codes or ())
        )
        self._pending: dict[tuple[str, str, str, str, str], _SnapshotCandidate] = {}
        self._validated_baselines: dict[
            str, tuple[int, BaselineManifest, str]
        ] = {}
        self._source_schema_pin: tuple[
            int, tuple[str, ...], str, str
        ] | None = None
        require_source_schema = False
        if self.mode == "snapshot" and self.selected_baseline_id:
            selected_manifest = _manifest_row(
                self.procurement_conn, self.selected_baseline_id
            )
            if selected_manifest is not None and selected_manifest.status == "complete":
                require_source_schema = True
        if self.mode != "legacy":
            self._check_source_schema(require=require_source_schema)

    def _source_schema_state_exists(self) -> bool:
        try:
            row = self.procurement_conn.execute(
                "SELECT 1 FROM locality_source_schema_state WHERE singleton_id=1"
            ).fetchone()
        except sqlite3.Error:
            return False
        return row is not None

    def _check_source_schema(self, *, require: bool) -> None:
        if self.mode == "legacy":
            return
        if not self._source_schema_state_exists():
            if require or self._source_schema_pin is not None:
                raise MissingHistoricalSnapshot(
                    "source schema protection has not been refreshed and audited"
                )
            return
        current = _assert_source_schema_protected(self.procurement_conn)
        if self._source_schema_pin is None:
            self._source_schema_pin = current
        elif current != self._source_schema_pin:
            raise MissingHistoricalSnapshot(
                "source schema changed after resolver activation"
            )

    def _contract(self, row: CanonicalContract | Mapping[str, Any]) -> CanonicalContract:
        if isinstance(row, CanonicalContract):
            if (
                self.mode != "legacy"
                and row.agency not in self._normalized_eligible_agencies
            ):
                raise SnapshotContentMismatch(
                    "canonical contract has an ineligible agency"
                )
            return row
        sector = row.get("sector") or row.get("_locality_sector")
        if not sector:
            raise ValueError("sector is required for locality snapshot resolution")
        contract = canonical_contract_from_row(
            row,
            str(sector),
            eligible_agency_codes=self.eligible_agency_codes,
        )
        selected_agency = row.get("_locality_selected_agency")
        if (
            selected_agency is not None
            and normalize_contract_key(selected_agency) != contract.agency
        ):
            raise SnapshotContentMismatch(
                "resolver canonical agency differs from calculated agency"
            )
        return contract

    def _generation_owner(self) -> str:
        return f"{_GENERATION_OWNER_PREFIX}{self.generation_id}"

    def require_eligible_agency_codes(self, codes: Collection[str]) -> None:
        if self.mode == "legacy":
            return
        if frozenset(codes) != self.eligible_agency_codes:
            raise SnapshotContentMismatch(
                "resolver eligible agency codes differ from calculation"
            )

    @staticmethod
    def _key(
        contract: CanonicalContract,
        bizno: str,
        baseline_id: str | None,
    ) -> tuple[str, str, str, str, str]:
        return (baseline_id or "", *contract.identity, normalize_bizno(bizno))

    def _existing(
        self,
        contract: CanonicalContract,
        bizno: str,
        *,
        pre_cutover: bool,
    ) -> tuple[Any, ...] | None:
        owner_id = (
            self.selected_baseline_id
            if pre_cutover
            else self._generation_owner()
        )
        if pre_cutover and self.mode == "snapshot" and owner_id is None:
            raise MissingHistoricalSnapshot(
                "pre-cutover snapshot resolution requires a selected baseline version"
            )
        key = self._key(contract, bizno, owner_id)
        pending = self._pending.get(key)
        if pending is not None:
            return (
                pending.share_pct,
                None if pending.is_busan is None else int(pending.is_busan),
                pending.basis,
                content_fingerprint(pending.contract),
                key[0],
                self.generation_id,
            )
        return self.procurement_conn.execute(
            """
            SELECT share_pct, is_busan, basis, content_fingerprint,
                   baseline_id, introduced_generation_id
            FROM contract_supplier_locality
            WHERE baseline_id=? AND sector=? AND contract_key=?
              AND contract_revision=? AND bizno=?
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
        if baseline_id != self.selected_baseline_id:
            raise MissingHistoricalSnapshot(
                "pre-cutover snapshot does not belong to the selected baseline version"
            )
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

        self._check_source_schema(require=True)
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
            proof = _verification_row(self.procurement_conn, baseline_id)
            if proof is None or cached[2] != proof.proof_fingerprint:
                raise MissingHistoricalSnapshot(
                    "baseline verification evidence drifted during snapshot resolution"
                )
            return
        if self.eligible_agency_codes is None:
            raise MissingHistoricalSnapshot(
                "historical baseline validation requires eligible agency codes"
            )
        proof = _validate_baseline_verification(
            self.procurement_conn,
            baseline_id,
            manifest,
            self.eligible_agency_codes,
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
        if proof.source_data_generation > source_generation:
            raise MissingHistoricalSnapshot(
                "historical baseline proof is newer than the source data generation"
            )
        self._validated_baselines[baseline_id] = (
            source_generation,
            manifest,
            proof.proof_fingerprint,
        )

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
        owner_id = baseline_id or self._generation_owner()
        key = self._key(contract, normalized, owner_id)
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
        baseline_scopes = (
            (self._generation_owner(), self.selected_baseline_id)
            if self.selected_baseline_id
            else (self._generation_owner(),)
        )
        placeholders = ",".join("?" for _ in baseline_scopes)
        rows = self.procurement_conn.execute(
            f"""
            SELECT contract_revision, bizno, is_busan
            FROM contract_supplier_locality
            WHERE sector=? AND contract_key=? AND contract_revision<>?
              AND baseline_id IN ({placeholders})
            """,
            (
                contract.sector,
                contract.contract_key,
                contract.contract_revision,
                *baseline_scopes,
            ),
        ).fetchall()
        prior_revisions = {
            row[0] for row in rows if _revision_rank(row[0]) < current_rank
        }
        for key, pending in self._pending.items():
            if (
                key[0] in baseline_scopes
                and key[1] == contract.sector
                and key[2] == contract.contract_key
                and key[3] != contract.contract_revision
                and _revision_rank(key[3]) < current_rank
            ):
                prior_revisions.add(key[3])
        if not prior_revisions:
            return _MISSING
        immediate_revision = max(prior_revisions, key=_revision_rank)
        values = [
            row[2]
            for row in rows
            if row[0] == immediate_revision and row[1] == bizno
        ]
        for scope in baseline_scopes:
            pending = self._pending.get(
                (scope, contract.sector, contract.contract_key, immediate_revision, bizno)
            )
            if pending is not None:
                values.append(
                    None if pending.is_busan is None else int(pending.is_busan)
                )
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
        if self.procurement_conn.in_transaction:
            raise MissingHistoricalSnapshot(
                "snapshot resolution rejects a caller-owned procurement transaction"
            )
        self._check_source_schema(require=False)
        contract = self._contract(row)
        normalized = normalize_bizno(bizno)
        pre_cutover = self._is_pre_cutover(contract)
        existing = self._existing(
            contract, normalized, pre_cutover=pre_cutover
        )
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
                baseline_scope = key[0]
                identity_key = key[1:]
                existing = self.procurement_conn.execute(
                    """
                    SELECT share_pct, is_busan, basis, content_fingerprint, baseline_id,
                           introduced_generation_id
                    FROM contract_supplier_locality
                    WHERE baseline_id=? AND sector=? AND contract_key=?
                      AND contract_revision=? AND bizno=?
                    """,
                    key,
                ).fetchone()
                expected = (
                    candidate.share_pct,
                    None if candidate.is_busan is None else int(candidate.is_busan),
                    candidate.basis,
                    content_fingerprint(candidate.contract),
                    baseline_scope,
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
                        *identity_key,
                        candidate.share_pct,
                        None if candidate.is_busan is None else int(candidate.is_busan),
                        candidate.basis,
                        candidate.contract.contract_date,
                        self.now,
                        CLASSIFIER_VERSION,
                        content_fingerprint(candidate.contract),
                        baseline_scope,
                        self.generation_id,
                    ),
                )
                inserted += 1
        self._pending.clear()
        return inserted


_MISSING = object()
