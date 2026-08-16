"""Supplier locality state, ordered change handling, and audit records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import sqlite3
from typing import Any, Iterable
from zoneinfo import ZoneInfo


SEOUL = ZoneInfo("Asia/Seoul")
BOOTSTRAP_EFFECTIVE_AT = "1900-01-01 00:00:00+09:00"
VALID_STATUSES = {"active_local", "moved_out", "branch_changed", "unverified"}


@dataclass(frozen=True)
class ChangeSummary:
    received: int = 0
    applied: int = 0
    duplicates: int = 0
    ignored: int = 0
    retrograde: int = 0
    conflicts: int = 0
    invalid: int = 0


@dataclass(frozen=True)
class Resolution:
    id: int
    bizno: str
    selected_status: str
    selected_effective_at: str


def normalize_bizno(value: Any) -> str:
    """Return a digits-only Korean business number, or an empty string."""
    text = "" if value is None else str(value).strip()
    return "".join(character for character in text if character.isdigit())


def current_status(rgn_nm: str, head_office: str) -> tuple[str, str | None]:
    if "부산" not in (rgn_nm or ""):
        return "moved_out", "region_changed"
    if (head_office or "").strip() != "본사":
        return "branch_changed", "head_office_changed"
    return "active_local", None


def _parse_timestamp(value: Any, *, allow_date: bool = False) -> tuple[datetime | None, bool]:
    if value is None:
        return None, False
    text = str(value).strip()
    if not text:
        return None, False
    if len(text) == 8 and text.isdigit():
        if not allow_date:
            return None, True
        try:
            return datetime.strptime(text, "%Y%m%d").replace(tzinfo=SEOUL), True
        except ValueError:
            return None, True
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        if not allow_date:
            return None, True
        try:
            return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=SEOUL), True
        except ValueError:
            return None, True
    formats = ("%Y%m%d%H%M%S", "%Y%m%d%H%M", "%Y-%m-%d %H:%M:%S")
    for format_string in formats:
        try:
            return datetime.strptime(text, format_string).replace(tzinfo=SEOUL), False
        except ValueError:
            pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None, False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SEOUL)
    return parsed.astimezone(SEOUL), False


def _canonical_timestamp(value: Any) -> str | None:
    parsed, _ = _parse_timestamp(value)
    if parsed is None:
        return None
    return parsed.strftime("%Y-%m-%d %H:%M:%S%z")[:-2] + ":" + parsed.strftime("%z")[-2:]


def _item_value(item: Any, *names: str) -> str:
    if isinstance(item, dict):
        for name in names:
            if name in item and item[name] is not None:
                return str(item[name])
    for name in names:
        value = getattr(item, name, None)
        if value is not None:
            return str(value)
    return ""


def _hash(parts: Iterable[str]) -> str:
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
    ).fetchone() is not None


def _master_columns(conn: sqlite3.Connection) -> set[str]:
    if not _table_exists(conn, "company_master"):
        return set()
    return {row[1] for row in conn.execute("PRAGMA table_info(company_master)")}


def _create_schema(conn: sqlite3.Connection) -> None:
    # journal_mode cannot change inside a transaction; callers already in one retain it.
    if not conn.in_transaction:
        conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS company_locality_status (
            bizno TEXT PRIMARY KEY,
            status TEXT NOT NULL CHECK(status IN ('active_local','moved_out','branch_changed','unverified')),
            source_rgn_nm TEXT NOT NULL DEFAULT '',
            source_hdoffce_div_nm TEXT NOT NULL DEFAULT '',
            source_effective_at TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            inactive_at TEXT,
            inactive_reason TEXT,
            last_verified_at TEXT NOT NULL,
            source_chg_dt TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS company_locality_event (
            id INTEGER PRIMARY KEY,
            bizno TEXT NOT NULL,
            previous_status TEXT,
            new_status TEXT NOT NULL,
            source_effective_at TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            source_chg_dt TEXT NOT NULL DEFAULT '',
            locality_hash TEXT NOT NULL,
            descriptive_hash TEXT NOT NULL,
            processed_at TEXT NOT NULL,
            job_id TEXT NOT NULL,
            disposition TEXT NOT NULL CHECK(disposition IN ('applied','duplicate','quarantined_retrograde','quarantined_conflict','quarantined_invalid_time')),
            UNIQUE(bizno, new_status, source_chg_dt, locality_hash)
        );
        CREATE TABLE IF NOT EXISTS company_sync_job_log (
            job_name TEXT NOT NULL,
            source_date TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('running','success','failed')),
            expected_rows INTEGER,
            received_rows INTEGER,
            page_count INTEGER,
            retry_count INTEGER NOT NULL DEFAULT 0,
            call_count INTEGER NOT NULL DEFAULT 0,
            call_budget INTEGER NOT NULL DEFAULT 0,
            circuit_state TEXT NOT NULL DEFAULT 'closed',
            started_at TEXT NOT NULL,
            completed_at TEXT,
            error_detail TEXT,
            PRIMARY KEY(job_name, source_date)
        );
        CREATE TABLE IF NOT EXISTS company_sync_response_metric (
            job_name TEXT NOT NULL,
            source_date TEXT NOT NULL,
            response_class TEXT NOT NULL,
            response_count INTEGER NOT NULL,
            PRIMARY KEY(job_name, source_date, response_class)
        );
        CREATE TABLE IF NOT EXISTS company_locality_resolution (
            id INTEGER PRIMARY KEY,
            bizno TEXT NOT NULL,
            event_ids_json TEXT NOT NULL,
            before_status TEXT,
            selected_status TEXT NOT NULL,
            selected_effective_at TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            operator TEXT NOT NULL,
            reason TEXT NOT NULL,
            generation_id TEXT NOT NULL,
            resolved_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS locality_activation_state (
            singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
            writes_enabled INTEGER NOT NULL CHECK(writes_enabled IN (0,1)),
            active_generation_id TEXT,
            ever_snapshot_activated INTEGER NOT NULL CHECK(ever_snapshot_activated IN (0,1)),
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS locality_fence_audit (
            id INTEGER PRIMARY KEY,
            writes_enabled INTEGER NOT NULL,
            operator TEXT NOT NULL,
            reason TEXT NOT NULL,
            changed_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS locality_generation_clock (
            singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
            data_generation INTEGER NOT NULL,
            control_revision INTEGER NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT OR IGNORE INTO locality_generation_clock VALUES (1, 0, 0)"
    )
    conn.execute(
        "INSERT OR IGNORE INTO locality_activation_state VALUES (1, 1, NULL, 0, ?)",
        (_now(),),
    )
    _install_generation_triggers(conn)


def _install_generation_triggers(conn: sqlite3.Connection) -> None:
    cache_input = ("company_locality_status", "company_locality_event", "company_locality_resolution")
    control = (
        "company_sync_job_log",
        "company_sync_response_metric",
        "locality_activation_state",
        "locality_fence_audit",
    )
    for table in cache_input:
        for action in ("INSERT", "UPDATE", "DELETE"):
            conn.execute(
                f"CREATE TRIGGER IF NOT EXISTS locality_{table}_{action.lower()}_data "
                f"AFTER {action} ON {table} BEGIN "
                "UPDATE locality_generation_clock SET data_generation = data_generation + 1 "
                "WHERE singleton_id = 1; END"
            )
    for table in control:
        for action in ("INSERT", "UPDATE", "DELETE"):
            conn.execute(
                f"CREATE TRIGGER IF NOT EXISTS locality_{table}_{action.lower()}_control "
                f"AFTER {action} ON {table} BEGIN "
                "UPDATE locality_generation_clock SET control_revision = control_revision + 1 "
                "WHERE singleton_id = 1; END"
            )


def _now() -> str:
    return datetime.now(SEOUL).strftime("%Y-%m-%d %H:%M:%S%z")[:-2] + ":" + datetime.now(SEOUL).strftime("%z")[-2:]


def _bootstrap_master_rows(conn: sqlite3.Connection, observed_at: str) -> None:
    columns = _master_columns(conn)
    if "bizno" not in columns:
        return
    selected = "bizno" + (", chgDt" if "chgDt" in columns else "")
    for row in conn.execute(f"SELECT {selected} FROM company_master"):
        bizno = normalize_bizno(row[0])
        if not bizno:
            continue
        source_chg_dt = row[1] if len(row) > 1 and row[1] else ""
        existing = conn.execute(
            "SELECT 1 FROM company_locality_status WHERE bizno = ?", (bizno,)
        ).fetchone()
        if existing:
            continue
        conn.execute(
            """
            INSERT INTO company_locality_status
            (bizno, status, source_effective_at, observed_at, last_verified_at, source_chg_dt)
            VALUES (?, 'active_local', ?, ?, ?, ?)
            """,
            (bizno, BOOTSTRAP_EFFECTIVE_AT, observed_at, observed_at, source_chg_dt),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO company_locality_event
            (bizno, previous_status, new_status, source_effective_at, observed_at, source_chg_dt,
             locality_hash, descriptive_hash, processed_at, job_id, disposition)
            VALUES (?, NULL, 'active_local', ?, ?, ?, ?, ?, ?, 'schema-bootstrap', 'applied')
            """,
            (
                bizno,
                BOOTSTRAP_EFFECTIVE_AT,
                observed_at,
                source_chg_dt,
                _hash((bizno, "bootstrap", BOOTSTRAP_EFFECTIVE_AT)),
                _hash((bizno, "bootstrap")),
                observed_at,
            ),
        )


def ensure_locality_schema(conn: sqlite3.Connection) -> None:
    """Install locality tables and bootstrap any existing supplier master rows."""
    owns_transaction = not conn.in_transaction
    _create_schema(conn)
    _bootstrap_master_rows(conn, _now())
    if owns_transaction:
        conn.commit()


def _update_master(conn: sqlite3.Connection, item: Any, bizno: str) -> None:
    columns = _master_columns(conn)
    if "bizno" not in columns:
        return
    aliases = {
        "corpNm": ("corpNm", "company_name"),
        "rgnNm": ("rgnNm",),
        "hdoffceDivNm": ("hdoffceDivNm",),
        "chgDt": ("chgDt",),
        "adrs": ("adrs",),
        "dtlAdrs": ("dtlAdrs",),
    }
    values = {"bizno": bizno}
    for source_name, destination_names in aliases.items():
        for destination in destination_names:
            if destination in columns:
                values[destination] = _item_value(item, source_name)
                break
    existing = conn.execute("SELECT 1 FROM company_master WHERE bizno = ?", (bizno,)).fetchone()
    if existing:
        assignments = [name for name in values if name != "bizno"]
        if assignments:
            conn.execute(
                "UPDATE company_master SET " + ", ".join(f"{name} = ?" for name in assignments) + " WHERE bizno = ?",
                tuple(values[name] for name in assignments) + (bizno,),
            )
        return
    names = list(values)
    conn.execute(
        "INSERT INTO company_master (" + ", ".join(names) + ") VALUES (" + ", ".join("?" for _ in names) + ")",
        tuple(values[name] for name in names),
    )


def _insert_event(
    conn: sqlite3.Connection,
    *,
    bizno: str,
    previous_status: str | None,
    new_status: str,
    effective_at: str,
    observed_at: str,
    source_chg_dt: str,
    locality_hash: str,
    descriptive_hash: str,
    job_id: str,
    disposition: str,
) -> bool:
    try:
        conn.execute(
            """
            INSERT INTO company_locality_event
            (bizno, previous_status, new_status, source_effective_at, observed_at, source_chg_dt,
             locality_hash, descriptive_hash, processed_at, job_id, disposition)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                bizno,
                previous_status,
                new_status,
                effective_at,
                observed_at,
                source_chg_dt,
                locality_hash,
                descriptive_hash,
                observed_at,
                job_id,
                disposition,
            ),
        )
        return True
    except sqlite3.IntegrityError:
        return False


def apply_company_changes(
    conn: sqlite3.Connection,
    items: Iterable[Any],
    source_date: str,
    job_id: str,
    verified_at: str,
) -> ChangeSummary:
    """Apply a complete staged supplier batch in source-effective-time order."""
    ensure_locality_schema(conn)
    summary = ChangeSummary()
    with conn:
        for item in items:
            summary = ChangeSummary(**{**summary.__dict__, "received": summary.received + 1})
            bizno = normalize_bizno(_item_value(item, "bizno", "brno", "businessNo"))
            region = _item_value(item, "rgnNm", "rgn_nm")
            head_office = _item_value(item, "hdoffceDivNm", "head_office")
            source_chg_dt = _item_value(item, "chgDt", "source_chg_dt")
            new_status, inactive_reason = current_status(region, head_office)
            effective_at = _canonical_timestamp(source_chg_dt)
            if not bizno or effective_at is None:
                if bizno:
                    _insert_event(
                        conn,
                        bizno=bizno,
                        previous_status=None,
                        new_status=new_status,
                        effective_at="",
                        observed_at=verified_at,
                        source_chg_dt=source_chg_dt,
                        locality_hash=_hash((bizno, region.strip(), head_office.strip(), source_chg_dt)),
                        descriptive_hash=_hash((bizno, _item_value(item, "corpNm", "adrs", "dtlAdrs"))),
                        job_id=job_id,
                        disposition="quarantined_invalid_time",
                    )
                summary = ChangeSummary(**{**summary.__dict__, "invalid": summary.invalid + 1})
                continue
            locality_hash = _hash((bizno, region.strip(), head_office.strip(), effective_at))
            descriptive_hash = _hash(
                (bizno, _item_value(item, "corpNm"), _item_value(item, "adrs"), _item_value(item, "dtlAdrs"))
            )
            state = conn.execute(
                "SELECT status, source_effective_at FROM company_locality_status WHERE bizno = ?", (bizno,)
            ).fetchone()
            is_master = "bizno" in _master_columns(conn) and conn.execute(
                "SELECT 1 FROM company_master WHERE bizno = ?", (bizno,)
            ).fetchone() is not None
            if state is None and new_status != "active_local" and not is_master:
                summary = ChangeSummary(**{**summary.__dict__, "ignored": summary.ignored + 1})
                continue
            if state is not None:
                current_effective = state[1]
                if effective_at < current_effective:
                    inserted = _insert_event(
                        conn,
                        bizno=bizno,
                        previous_status=state[0],
                        new_status=new_status,
                        effective_at=effective_at,
                        observed_at=verified_at,
                        source_chg_dt=source_chg_dt,
                        locality_hash=locality_hash,
                        descriptive_hash=descriptive_hash,
                        job_id=job_id,
                        disposition="quarantined_retrograde",
                    )
                    summary = ChangeSummary(**{**summary.__dict__, "retrograde": summary.retrograde + int(inserted), "duplicates": summary.duplicates + int(not inserted)})
                    continue
                if effective_at == current_effective:
                    matching = conn.execute(
                        "SELECT 1 FROM company_locality_event WHERE bizno = ? AND source_effective_at = ? "
                        "AND locality_hash = ? AND disposition = 'applied'",
                        (bizno, effective_at, locality_hash),
                    ).fetchone()
                    if matching:
                        _update_master(conn, item, bizno)
                        summary = ChangeSummary(**{**summary.__dict__, "duplicates": summary.duplicates + 1})
                        continue
                    inserted = _insert_event(
                        conn,
                        bizno=bizno,
                        previous_status=state[0],
                        new_status=new_status,
                        effective_at=effective_at,
                        observed_at=verified_at,
                        source_chg_dt=source_chg_dt,
                        locality_hash=locality_hash,
                        descriptive_hash=descriptive_hash,
                        job_id=job_id,
                        disposition="quarantined_conflict",
                    )
                    summary = ChangeSummary(**{**summary.__dict__, "conflicts": summary.conflicts + int(inserted), "duplicates": summary.duplicates + int(not inserted)})
                    continue
            previous_status = state[0] if state else None
            _update_master(conn, item, bizno)
            conn.execute(
                """
                INSERT INTO company_locality_status
                (bizno, status, source_rgn_nm, source_hdoffce_div_nm, source_effective_at,
                 observed_at, inactive_at, inactive_reason, last_verified_at, source_chg_dt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(bizno) DO UPDATE SET
                    status=excluded.status, source_rgn_nm=excluded.source_rgn_nm,
                    source_hdoffce_div_nm=excluded.source_hdoffce_div_nm,
                    source_effective_at=excluded.source_effective_at, observed_at=excluded.observed_at,
                    inactive_at=excluded.inactive_at, inactive_reason=excluded.inactive_reason,
                    last_verified_at=excluded.last_verified_at, source_chg_dt=excluded.source_chg_dt
                """,
                (
                    bizno,
                    new_status,
                    region,
                    head_office,
                    effective_at,
                    verified_at,
                    effective_at if inactive_reason else None,
                    inactive_reason,
                    verified_at,
                    source_chg_dt,
                ),
            )
            _insert_event(
                conn,
                bizno=bizno,
                previous_status=previous_status,
                new_status=new_status,
                effective_at=effective_at,
                observed_at=verified_at,
                source_chg_dt=source_chg_dt,
                locality_hash=locality_hash,
                descriptive_hash=descriptive_hash,
                job_id=job_id,
                disposition="applied",
            )
            summary = ChangeSummary(**{**summary.__dict__, "applied": summary.applied + 1})
    return summary


def active_local_biznos(conn: sqlite3.Connection) -> set[str]:
    ensure_locality_schema(conn)
    return {
        row[0]
        for row in conn.execute(
            "SELECT bizno FROM company_locality_status WHERE status = 'active_local'"
        )
    }


def status_at(conn: sqlite3.Connection, bizno: str, effective_at: str) -> str | None:
    ensure_locality_schema(conn)
    normalized = normalize_bizno(bizno)
    at, date_only = _parse_timestamp(effective_at, allow_date=True)
    if not normalized or at is None:
        return None
    if date_only:
        date_prefix = at.strftime("%Y-%m-%d")
        same_day = conn.execute(
            "SELECT 1 FROM company_locality_event WHERE bizno = ? AND source_effective_at LIKE ? LIMIT 1",
            (normalized, date_prefix + "%"),
        ).fetchone()
        if same_day:
            return None
        at = at.replace(hour=23, minute=59, second=59)
    point = _canonical_timestamp(at.isoformat())
    conflicts = conn.execute(
        "SELECT id FROM company_locality_event WHERE bizno = ? AND disposition = 'quarantined_conflict' "
        "AND source_effective_at <= ?",
        (normalized, point),
    ).fetchall()
    resolved_ids: set[int] = set()
    for (event_ids_json,) in conn.execute(
        "SELECT event_ids_json FROM company_locality_resolution WHERE bizno = ?", (normalized,)
    ):
        resolved_ids.update(json.loads(event_ids_json))
    if any(event_id not in resolved_ids for (event_id,) in conflicts):
        return None
    candidates: list[tuple[str, int, str]] = []
    for row in conn.execute(
        "SELECT source_effective_at, new_status FROM company_locality_event "
        "WHERE bizno = ? AND disposition = 'applied' AND source_effective_at <= ?",
        (normalized, point),
    ):
        candidates.append((row[0], 0, row[1]))
    for row in conn.execute(
        "SELECT selected_effective_at, selected_status FROM company_locality_resolution "
        "WHERE bizno = ? AND selected_effective_at <= ?",
        (normalized, point),
    ):
        candidates.append((row[0], 1, row[1]))
    return max(candidates)[2] if candidates else None


def resolve_company_conflict(
    conn: sqlite3.Connection,
    event_ids: Iterable[int],
    selected_status: str,
    effective_at: str,
    operator: str,
    reason: str,
    evidence: Any,
    generation_id: str = "manual",
) -> Resolution:
    ensure_locality_schema(conn)
    ordered_ids = sorted({int(event_id) for event_id in event_ids})
    if not ordered_ids or selected_status not in VALID_STATUSES:
        raise ValueError("a conflict selection and valid status are required")
    canonical_effective = _canonical_timestamp(effective_at)
    if canonical_effective is None:
        raise ValueError("effective_at must include a time")
    placeholders = ", ".join("?" for _ in ordered_ids)
    events = conn.execute(
        f"SELECT id, bizno FROM company_locality_event WHERE id IN ({placeholders}) "
        "AND disposition = 'quarantined_conflict'",
        ordered_ids,
    ).fetchall()
    if len(events) != len(ordered_ids) or len({row[1] for row in events}) != 1:
        raise ValueError("event_ids must name conflicts for one supplier")
    bizno = events[0][1]
    event_ids_json = json.dumps(ordered_ids, separators=(",", ":"))
    evidence_json = json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    existing = conn.execute(
        """
        SELECT id FROM company_locality_resolution
        WHERE bizno = ? AND event_ids_json = ? AND selected_status = ?
          AND selected_effective_at = ? AND evidence_json = ? AND operator = ?
          AND reason = ? AND generation_id = ?
        """,
        (bizno, event_ids_json, selected_status, canonical_effective, evidence_json, operator, reason, generation_id),
    ).fetchone()
    if existing:
        return Resolution(existing[0], bizno, selected_status, canonical_effective)
    with conn:
        before = conn.execute(
            "SELECT status FROM company_locality_status WHERE bizno = ?", (bizno,)
        ).fetchone()
        cursor = conn.execute(
            """
            INSERT INTO company_locality_resolution
            (bizno, event_ids_json, before_status, selected_status, selected_effective_at,
             evidence_json, operator, reason, generation_id, resolved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                bizno,
                event_ids_json,
                before[0] if before else None,
                selected_status,
                canonical_effective,
                evidence_json,
                operator,
                reason,
                generation_id,
                _now(),
            ),
        )
        return Resolution(cursor.lastrowid, bizno, selected_status, canonical_effective)


def start_sync_job(conn: sqlite3.Connection, job_name: str, source_date: str, *, started_at: str | None = None, **metrics: Any) -> None:
    ensure_locality_schema(conn)
    conn.execute(
        """
        INSERT INTO company_sync_job_log (job_name, source_date, status, expected_rows, received_rows,
            page_count, retry_count, call_count, call_budget, circuit_state, started_at)
        VALUES (?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_name, source_date) DO UPDATE SET status='running', started_at=excluded.started_at,
            completed_at=NULL, error_detail=NULL
        """,
        (
            job_name, source_date, metrics.get("expected_rows"), metrics.get("received_rows"),
            metrics.get("page_count"), metrics.get("retry_count", 0), metrics.get("call_count", 0),
            metrics.get("call_budget", 0), metrics.get("circuit_state", "closed"), started_at or _now(),
        ),
    )
    conn.commit()


def finish_sync_job(conn: sqlite3.Connection, job_name: str, source_date: str, *, completed_at: str | None = None, **metrics: Any) -> None:
    conn.execute(
        "UPDATE company_sync_job_log SET status='success', completed_at=?, expected_rows=COALESCE(?, expected_rows), "
        "received_rows=COALESCE(?, received_rows), page_count=COALESCE(?, page_count) WHERE job_name=? AND source_date=?",
        (completed_at or _now(), metrics.get("expected_rows"), metrics.get("received_rows"), metrics.get("page_count"), job_name, source_date),
    )
    conn.commit()


def fail_sync_job(conn: sqlite3.Connection, job_name: str, source_date: str, error_detail: str, *, completed_at: str | None = None) -> None:
    conn.execute(
        "UPDATE company_sync_job_log SET status='failed', completed_at=?, error_detail=? WHERE job_name=? AND source_date=?",
        (completed_at or _now(), error_detail, job_name, source_date),
    )
    conn.commit()
