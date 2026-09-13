#!/usr/bin/env python3
"""Reliable G2B shopping-performance ingestion with resumable shadow mode.

The script deliberately separates procurement-performance rows from product
catalog/MAS master ingestion.  Shadow mode never writes to the production DB.
Live mode must be requested explicitly with ``--live``.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import ssl
import sys
import time
import urllib.error
import urllib.request


API_URL = (
    "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/"
    "getDlvrReqDtlInfoList"
)
PAGE_SIZE = 999
DEFAULT_PRODUCTION_DB = "procurement_contracts.db"
DEFAULT_AGENCY_DB = "busan_agencies_master.db"
DEFAULT_SHADOW_DB = "scratch/shopping_performance_backfill/shopping_backfill.db"
SHOPPING_TABLE = "shopping_cntrct"
SYNC_TABLE = "shopping_sync_log"
KEY_COLUMNS = ("dlvrReqNo", "prdctSno", "dlvrReqChgOrd")
PARTITION_DATE_SQL = 'SUBSTR(REPLACE("dlvrReqRcptDate", \'-\', \'\'), 1, 8)'


class SyncError(RuntimeError):
    pass


class QuotaReserveReached(SyncError):
    pass


class StorageGuardTriggered(SyncError):
    pass


def safe_error_text(value) -> str:
    """Remove public-API credentials from exceptions before logging."""
    text = str(value or "")
    text = re.sub(r"(?i)(serviceKey=)[^&\s]+", r"\1***", text)
    return text[:500]


def parse_yyyymmdd(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y%m%d").date()


def date_key(value: str) -> str | None:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) < 8:
        return None
    try:
        return parse_yyyymmdd(digits[:8]).strftime("%Y%m%d")
    except ValueError:
        return None


def inclusive_dates(start: str, end: str) -> list[str]:
    first, last = parse_yyyymmdd(start), parse_yyyymmdd(end)
    if first > last:
        raise ValueError("start date must not be after end date")
    result = []
    current = first
    while current <= last:
        result.append(current.strftime("%Y%m%d"))
        current += dt.timedelta(days=1)
    return result


def page_count(total_count: int, page_size: int = PAGE_SIZE) -> int:
    if total_count <= 0:
        return 1
    return math.ceil(total_count / page_size)


def normalize_items(value) -> list[dict]:
    if value is None:
        return []
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        nested = value.get("item")
        if isinstance(nested, list):
            return [item for item in nested if isinstance(item, dict)]
        if isinstance(nested, dict):
            return [nested]
        return [value] if value else []
    return []


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    return [str(row[1]) for row in rows]


def existing_source_dates(conn: sqlite3.Connection) -> set[str]:
    dates: set[str] = set()
    for (value,) in conn.execute(
        f'SELECT DISTINCT "dlvrReqRcptDate" FROM "{SHOPPING_TABLE}" '
        'WHERE "dlvrReqRcptDate" IS NOT NULL'
    ):
        normalized = date_key(value)
        if normalized:
            dates.add(normalized)
    return dates


def initialize_shadow(
    shadow_conn: sqlite3.Connection, production_columns: list[str]
) -> None:
    column_sql = ", ".join(f'"{name}" TEXT' for name in production_columns)
    shadow_conn.execute(
        f'CREATE TABLE IF NOT EXISTS "{SHOPPING_TABLE}" ({column_sql})'
    )
    shadow_conn.execute(
        f"""CREATE TABLE IF NOT EXISTS {SYNC_TABLE} (
            target_date TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            source_total INTEGER NOT NULL DEFAULT 0,
            source_received INTEGER NOT NULL DEFAULT 0,
            busan_rows INTEGER NOT NULL DEFAULT 0,
            stored_rows INTEGER NOT NULL DEFAULT 0,
            request_count INTEGER NOT NULL DEFAULT 0,
            rate_limit_remaining INTEGER,
            error TEXT,
            completed_at TEXT
        )"""
    )
    shadow_conn.execute(
        f'CREATE UNIQUE INDEX IF NOT EXISTS ux_{SHOPPING_TABLE}_key '
        f'ON "{SHOPPING_TABLE}" ("dlvrReqNo", "prdctSno", "dlvrReqChgOrd")'
    )
    shadow_conn.commit()


def initialize_live_sync_log(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""CREATE TABLE IF NOT EXISTS {SYNC_TABLE} (
            target_date TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            source_total INTEGER NOT NULL DEFAULT 0,
            source_received INTEGER NOT NULL DEFAULT 0,
            busan_rows INTEGER NOT NULL DEFAULT 0,
            stored_rows INTEGER NOT NULL DEFAULT 0,
            request_count INTEGER NOT NULL DEFAULT 0,
            rate_limit_remaining INTEGER,
            error TEXT,
            completed_at TEXT
        )"""
    )
    conn.commit()


def record_status(
    conn: sqlite3.Connection,
    target_date: str,
    status: str,
    *,
    source_total: int = 0,
    source_received: int = 0,
    busan_rows: int = 0,
    stored_rows: int = 0,
    request_count: int = 0,
    rate_remaining: int | None = None,
    error: str | None = None,
    commit: bool = True,
) -> None:
    conn.execute(
        f"""INSERT INTO {SYNC_TABLE}
        (target_date, status, source_total, source_received, busan_rows,
         stored_rows, request_count, rate_limit_remaining, error, completed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(target_date) DO UPDATE SET
          status=excluded.status,
          source_total=excluded.source_total,
          source_received=excluded.source_received,
          busan_rows=excluded.busan_rows,
          stored_rows=excluded.stored_rows,
          request_count=excluded.request_count,
          rate_limit_remaining=excluded.rate_limit_remaining,
          error=excluded.error,
          completed_at=excluded.completed_at""",
        (
            target_date,
            status,
            source_total,
            source_received,
            busan_rows,
            stored_rows,
            request_count,
            rate_remaining,
            safe_error_text(error) or None,
            dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        ),
    )
    if commit:
        conn.commit()


def source_key(item: dict) -> tuple[str, str, str]:
    # API change order may be the number 0, which is not a missing identity.
    return tuple("" if item.get(name) is None else str(item[name]) for name in KEY_COLUMNS)


def store_rows(
    conn: sqlite3.Connection,
    rows: list[dict],
    production_columns: list[str],
    *,
    target_date: str | None = None,
    replace_partition: bool = False,
) -> int:
    if replace_partition:
        if not target_date:
            raise SyncError("target_date is required when replacing a partition")
        conn.execute(
            f'DELETE FROM "{SHOPPING_TABLE}" '
            f'WHERE {PARTITION_DATE_SQL} = ?',
            (target_date,),
        )

    # Match the production pipeline's exact-key duplicate defense while also
    # making retries idempotent in a table that has no unique constraint.
    unique_rows: dict[tuple[str, str, str], dict] = {}
    for item in rows:
        key = source_key(item)
        unique_rows[key] = item
    rows = list(unique_rows.values())
    if not rows:
        return 0
    quoted = ", ".join(f'"{name}"' for name in production_columns)
    placeholders = ", ".join("?" for _ in production_columns)
    sql = (
        f'INSERT OR REPLACE INTO "{SHOPPING_TABLE}" ({quoted}) '
        f"VALUES ({placeholders})"
    )
    values = []
    for item in rows:
        values.append(
            tuple(
                json.dumps(item.get(name), ensure_ascii=False)
                if isinstance(item.get(name), (dict, list))
                else "" if item.get(name) is None
                else str(item.get(name))
                for name in production_columns
            )
        )
    keys = [source_key(item) for item in rows]
    conn.executemany(
        f'DELETE FROM "{SHOPPING_TABLE}" '
        'WHERE "dlvrReqNo"=? AND "prdctSno"=? AND "dlvrReqChgOrd"=?',
        keys,
    )
    conn.executemany(sql, values)
    return len(values)


def busan_agency_codes(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[0]).strip()
        for row in conn.execute("SELECT dminsttCd FROM agency_master")
        if row[0] is not None and str(row[0]).strip()
    }


def checked_storage(
    directory: Path,
    size_guard_path: Path | None,
    min_free_bytes: int,
    max_shadow_bytes: int,
) -> dict:
    usage = shutil.disk_usage(directory)
    guarded_bytes = (
        size_guard_path.stat().st_size
        if size_guard_path is not None and size_guard_path.exists()
        else 0
    )
    if usage.free < min_free_bytes:
        raise StorageGuardTriggered(
            f"free bytes {usage.free} below required {min_free_bytes}"
        )
    if size_guard_path is not None and guarded_bytes > max_shadow_bytes:
        raise StorageGuardTriggered(
            f"guarded file bytes {guarded_bytes} above limit {max_shadow_bytes}"
        )
    return {
        "filesystem_total": usage.total,
        "filesystem_free": usage.free,
        "guarded_file_bytes": guarded_bytes,
    }


class ApiClient:
    def __init__(
        self,
        service_key: str,
        *,
        timeout: int = 30,
        retries: int = 3,
        reserve_requests: int = 150,
        max_requests: int = 100,
    ) -> None:
        if not service_key:
            raise SyncError("SERVICE_KEY is empty")
        self.service_key = service_key
        self.timeout = timeout
        self.retries = retries
        self.reserve_requests = reserve_requests
        self.max_requests = max_requests
        self.request_count = 0
        self.rate_limit_remaining: int | None = None
        self.context = ssl.create_default_context()
        self.context.check_hostname = False
        self.context.verify_mode = ssl.CERT_NONE

    def _ensure_budget(self, requests_needed: int = 1) -> None:
        if self.request_count + requests_needed > self.max_requests:
            raise QuotaReserveReached(
                f"run request cap would be exceeded: "
                f"{self.request_count}+{requests_needed}>{self.max_requests}"
            )
        if (
            self.rate_limit_remaining is not None
            and self.rate_limit_remaining - requests_needed < self.reserve_requests
        ):
            raise QuotaReserveReached(
                f"API reserve would be crossed: remaining={self.rate_limit_remaining}, "
                f"needed={requests_needed}, reserve={self.reserve_requests}"
            )

    def fetch_page(self, target_date: str, page_no: int) -> tuple[list[dict], int]:
        query = (
            f"?serviceKey={self.service_key}&inqryDiv=1"
            f"&inqryBgnDate={target_date}&inqryEndDate={target_date}"
            f"&numOfRows={PAGE_SIZE}&pageNo={page_no}&type=json"
        )
        last_error: Exception | None = None
        for attempt in range(self.retries):
            self._ensure_budget(1)
            # Count attempts conservatively: an HTTP/network failure may still have
            # consumed an upstream request even when no rate-limit header returns.
            self.request_count += 1
            try:
                request = urllib.request.Request(
                    API_URL + query, headers={"User-Agent": "Mozilla/5.0"}
                )
                with urllib.request.urlopen(
                    request, context=self.context, timeout=self.timeout
                ) as response:
                    remaining = response.headers.get("X-RateLimit-Remaining")
                    if remaining is not None:
                        self.rate_limit_remaining = int(remaining)
                    payload = json.loads(response.read().decode("utf-8"))
                root = payload.get("response", payload)
                header = root.get("header", {})
                if str(header.get("resultCode")) != "00":
                    raise SyncError(
                        f"API result {header.get('resultCode')}: "
                        f"{header.get('resultMsg', '')}"
                    )
                body = root.get("body", {})
                return normalize_items(body.get("items")), int(
                    body.get("totalCount", 0) or 0
                )
            except QuotaReserveReached:
                raise
            except (OSError, ValueError, json.JSONDecodeError, SyncError) as exc:
                last_error = exc
                if attempt + 1 < self.retries:
                    time.sleep(2**attempt)
        raise SyncError(
            f"page {page_no} failed after retries: {safe_error_text(last_error)}"
        )

    def fetch_day(self, target_date: str) -> tuple[list[dict], int, int]:
        before = self.request_count
        first, total = self.fetch_page(target_date, 1)
        pages = page_count(total)
        self._ensure_budget(max(0, pages - 1))
        rows = list(first)
        for page_no in range(2, pages + 1):
            page_rows, page_total = self.fetch_page(target_date, page_no)
            if page_total != total:
                raise SyncError(
                    f"totalCount changed within date: {total} -> {page_total}"
                )
            rows.extend(page_rows)
        if len(rows) != total:
            raise SyncError(
                f"incomplete source rows: received={len(rows)}, totalCount={total}"
            )
        seen: set[tuple[str, str, str]] = set()
        for row in rows:
            key = source_key(row)
            if not all(part.strip() for part in key):
                raise SyncError("missing source key; refusing an incomplete date")
            if key in seen:
                raise SyncError("duplicate source key across API rows; date completeness is unproven")
            seen.add(key)
            if date_key(row.get("dlvrReqRcptDate")) != target_date:
                raise SyncError("source receipt date differs from requested date")
        return rows, total, self.request_count - before


def fetch_day_with_zero_confirmation(
    client: ApiClient, target_date: str
) -> tuple[list[dict], int, int]:
    """Re-read a zero result once before accepting it as a complete empty day."""
    before = client.request_count
    rows, total, _ = client.fetch_day(target_date)
    if total == 0:
        rows, total, _ = client.fetch_day(target_date)
    return rows, total, client.request_count - before


def status_summary(conn: sqlite3.Connection, db_path: Path) -> dict:
    statuses = dict(
        conn.execute(
            f"SELECT status, COUNT(*) FROM {SYNC_TABLE} GROUP BY status"
        ).fetchall()
    )
    completed = conn.execute(
        f"""SELECT COUNT(*), COALESCE(SUM(source_total),0),
        COALESCE(SUM(source_received),0), COALESCE(SUM(busan_rows),0),
        COALESCE(SUM(stored_rows),0), COALESCE(SUM(request_count),0)
        FROM {SYNC_TABLE} WHERE status IN ('complete','complete_zero')"""
    ).fetchone()
    return {
        "db_path": str(db_path),
        "db_bytes": db_path.stat().st_size if db_path.exists() else 0,
        "statuses": statuses,
        "completed_days": int(completed[0]),
        "source_total": int(completed[1]),
        "source_received": int(completed[2]),
        "busan_rows": int(completed[3]),
        "stored_rows": int(completed[4]),
        "request_count": int(completed[5]),
    }


@contextlib.contextmanager
def file_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    except BlockingIOError as exc:
        raise SyncError(f"another sync process holds {path}") from exc
    finally:
        try:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except (ImportError, OSError):
            pass
        handle.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--production-db", default=DEFAULT_PRODUCTION_DB)
    parser.add_argument("--agency-db", default=DEFAULT_AGENCY_DB)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--shadow-db", default=None)
    mode.add_argument("--live", action="store_true")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--lag-days", type=int, default=2)
    parser.add_argument("--lookback-days", type=int, default=7)
    parser.add_argument(
        "--coverage-start", default="20260101",
        help="Earliest date to check for live collection gaps, in YYYYMMDD format",
    )
    parser.add_argument("--max-requests", type=int, default=100)
    parser.add_argument("--reserve-requests", type=int, default=150)
    parser.add_argument("--min-free-gb", type=float, default=8.0)
    parser.add_argument("--max-shadow-mb", type=float, default=512.0)
    parser.add_argument("--include-existing", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--ascending", action="store_true")
    return parser


def target_dates(args, production_conn: sqlite3.Connection) -> list[str]:
    if args.start or args.end:
        if not args.start or not args.end:
            raise SyncError("--start and --end must be provided together")
        dates = inclusive_dates(args.start, args.end)
    else:
        end = dt.date.today() - dt.timedelta(days=args.lag_days)
        start = end - dt.timedelta(days=max(0, args.lookback_days - 1))
        dates = inclusive_dates(start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
        if args.live:
            coverage = inclusive_dates(args.coverage_start, end.strftime("%Y%m%d"))
            existing = existing_source_dates(production_conn)
            log_exists = production_conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (SYNC_TABLE,),
            ).fetchone()
            complete: set[str] = set()
            incomplete: set[str] = set()
            if log_exists:
                for day, status, total, received, selected, stored, error in production_conn.execute(
                    f"SELECT target_date, status, source_total, source_received, busan_rows, stored_rows, error FROM {SYNC_TABLE}"
                ):
                    if (status in ("complete", "complete_zero") and int(total or 0) == int(received or 0)
                            and int(selected or 0) == int(stored or 0) and not error):
                        complete.add(str(day))
                    else:
                        incomplete.add(str(day))
            # Legacy production days predate the independent log. Their data
            # presence is a migration baseline, not a claim of API completeness;
            # do not re-download that whole history just because logs are absent.
            backlog = [
                day for day in coverage
                if day in incomplete or (day not in existing and day not in complete)
            ]
            latest = end.strftime("%Y%m%d")
            # Keep the newest day current, then drain oldest unresolved dates
            # before spending the remaining quota on rolling corrections.
            ordered = [latest] + backlog + sorted(dates, reverse=True)
            return list(dict.fromkeys(ordered))
    if not args.live and not args.include_existing:
        existing = existing_source_dates(production_conn)
        dates = [value for value in dates if value not in existing]
    return sorted(dates, reverse=not args.ascending)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    base_dir = Path(__file__).resolve().parent
    load_env_file(base_dir / ".env")
    production_db = Path(args.production_db).resolve()
    agency_db = Path(args.agency_db).resolve()
    destination_db = (
        production_db if args.live else Path(args.shadow_db or DEFAULT_SHADOW_DB).resolve()
    )
    destination_db.parent.mkdir(parents=True, exist_ok=True)

    if not production_db.exists() or not agency_db.exists():
        raise SyncError("production or agency database is missing")
    if not args.live and destination_db == production_db:
        raise SyncError("shadow DB must differ from production DB")

    min_free_bytes = int(args.min_free_gb * 1024**3)
    max_shadow_bytes = int(args.max_shadow_mb * 1024**2)
    lock_path = destination_db.with_suffix(destination_db.suffix + ".lock")

    with file_lock(lock_path):
        prod_conn = sqlite3.connect(f"file:{production_db}?mode=ro", uri=True)
        agency_conn = sqlite3.connect(f"file:{agency_db}?mode=ro", uri=True)
        try:
            columns = table_columns(prod_conn, SHOPPING_TABLE)
            if not columns or not all(name in columns for name in KEY_COLUMNS):
                raise SyncError("production shopping table schema is incompatible")
            busan_codes = busan_agency_codes(agency_conn)
            if not busan_codes:
                raise SyncError("agency master is empty; refusing shopping partition replacement")
            dates = target_dates(args, prod_conn)
        finally:
            agency_conn.close()

        destination_conn = sqlite3.connect(destination_db, timeout=30)
        try:
            if args.live:
                initialize_live_sync_log(destination_conn)
            else:
                initialize_shadow(destination_conn, columns)
                os.chmod(destination_db, 0o600)

            if args.status:
                print(json.dumps(status_summary(destination_conn, destination_db), ensure_ascii=False))
                return 0

            # Shadow backfills resume completed dates. Live mode deliberately
            # re-fetches the full rolling window so late upstream corrections
            # replace the affected date partition on the next run.
            if not args.live:
                done = {
                    row[0]
                    for row in destination_conn.execute(
                        f"SELECT target_date FROM {SYNC_TABLE} "
                        "WHERE status IN ('complete','complete_zero')"
                    )
                }
                dates = [value for value in dates if value not in done]
            client = ApiClient(
                os.environ.get("SERVICE_KEY", ""),
                reserve_requests=args.reserve_requests,
                max_requests=args.max_requests,
            )
            print(
                json.dumps(
                    {
                        "mode": "live" if args.live else "shadow",
                        "candidate_days": len(dates),
                        "max_requests": args.max_requests,
                        "reserve_requests": args.reserve_requests,
                        "min_free_gb": args.min_free_gb,
                        "max_shadow_mb": args.max_shadow_mb,
                    },
                    ensure_ascii=False,
                )
            )

            attempted_days = 0
            completed_this_run = 0
            failed_this_run = 0
            deferred_this_run = 0
            for index, target_date in enumerate(dates, start=1):
                checked_storage(
                    destination_db.parent,
                    None if args.live else destination_db,
                    min_free_bytes,
                    max_shadow_bytes,
                )
                before_requests = client.request_count
                attempted_days += 1
                try:
                    rows, source_total, requests_used = fetch_day_with_zero_confirmation(
                        client, target_date
                    )
                    filtered = [
                        row
                        for row in rows
                        if str(row.get("dminsttCd", "")).strip() in busan_codes
                    ]
                    destination_conn.execute("BEGIN IMMEDIATE")
                    if args.live and source_total == 0:
                        previous_count = destination_conn.execute(
                            f'SELECT COUNT(*) FROM "{SHOPPING_TABLE}" WHERE {PARTITION_DATE_SQL} = ?',
                            (target_date,),
                        ).fetchone()[0]
                        if previous_count:
                            raise SyncError(
                                f"zero API response conflicts with {previous_count} existing rows; "
                                "production date partition preserved"
                            )
                    stored = store_rows(
                        destination_conn,
                        filtered,
                        columns,
                        target_date=target_date,
                        replace_partition=args.live,
                    )
                    status = "complete_zero" if source_total == 0 else "complete"
                    record_status(
                        destination_conn,
                        target_date,
                        status,
                        source_total=source_total,
                        source_received=len(rows),
                        busan_rows=len(filtered),
                        stored_rows=stored,
                        request_count=requests_used,
                        rate_remaining=client.rate_limit_remaining,
                        commit=False,
                    )
                    destination_conn.commit()
                    completed_this_run += 1
                    print(
                        f"[{index}/{len(dates)}] {target_date} {status}: "
                        f"source={source_total}, busan={len(filtered)}, "
                        f"stored={stored}, requests={requests_used}, "
                        f"remaining={client.rate_limit_remaining}"
                    )
                except QuotaReserveReached as exc:
                    if destination_conn.in_transaction:
                        destination_conn.rollback()
                    deferred_this_run += 1
                    record_status(
                        destination_conn,
                        target_date,
                        "deferred_quota",
                        request_count=client.request_count - before_requests,
                        rate_remaining=client.rate_limit_remaining,
                        error=str(exc),
                    )
                    print(f"STOP quota guard: {exc}")
                    break
                except StorageGuardTriggered:
                    raise
                except Exception as exc:
                    if destination_conn.in_transaction:
                        destination_conn.rollback()
                    failed_this_run += 1
                    record_status(
                        destination_conn,
                        target_date,
                        "failed",
                        request_count=client.request_count - before_requests,
                        rate_remaining=client.rate_limit_remaining,
                        error=str(exc),
                    )
                    print(f"ERROR {target_date}: {safe_error_text(exc)}", file=sys.stderr)

            storage = checked_storage(
                destination_db.parent,
                None if args.live else destination_db,
                min_free_bytes,
                max_shadow_bytes,
            )
            summary = status_summary(destination_conn, destination_db)
            summary.update(
                {
                    "run_requests": client.request_count,
                    "rate_limit_remaining": client.rate_limit_remaining,
                    "filesystem_free": storage["filesystem_free"],
                    "planned_days": len(dates),
                    "completed_this_run": completed_this_run,
                    "failed_this_run": failed_this_run,
                    "deferred_this_run": deferred_this_run,
                    "unprocessed_days": len(dates) - attempted_days,
                    "remaining_days": len(dates) - completed_this_run,
                }
            )
            print("SUMMARY " + json.dumps(summary, ensure_ascii=False))
            return 2 if completed_this_run != len(dates) else 0
        finally:
            destination_conn.close()
            prod_conn.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (SyncError, StorageGuardTriggered) as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        raise SystemExit(2)
