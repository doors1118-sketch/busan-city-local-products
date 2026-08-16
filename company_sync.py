from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
import json
import math
from pathlib import Path
import random
import sqlite3
import ssl
import time
from typing import Any, Callable, Mapping
import urllib.error
import urllib.request

from company_locality import ChangeSummary, apply_company_changes, ensure_locality_schema, fail_sync_job, finish_sync_job, normalize_bizno, start_sync_job
from maintenance_lock import guarded_write_session


EXPLICIT_NOT_FOUND_RESULT_CODES = {"03"}
REQUIRED_ITEM_FIELDS = ("rgnNm", "hdoffceDivNm", "chgDt")


class IncompleteCompanyBatch(RuntimeError):
    """Raised when a supplier response cannot prove a complete source batch."""

    def __init__(self, message: str, metrics: "CollectionMetrics | None" = None):
        super().__init__(message)
        self.metrics = metrics


class CompanyFetchError(RuntimeError):
    def __init__(self, message: str, *, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


@dataclass(frozen=True)
class CompanyBatch:
    items: tuple[dict[str, Any], ...]
    total_count: int
    page_count: int
    retry_count: int


@dataclass(frozen=True)
class CompanyPage:
    items: tuple[dict[str, Any], ...]
    total_count: int | None
    page_number: int | None
    num_of_rows: int | None
    response_class: str = "success"
    retry_after: float | None = None
    explicit_not_found: bool = False


@dataclass(frozen=True)
class CompanySyncPolicy:
    max_attempts: int = 3
    requests_per_second: float = 5.0
    daily_call_budget: int = 10_000
    circuit_failure_threshold: int = 3
    backoff_seconds: float = 0.5
    jitter_seconds: float = 0.2


@dataclass
class CollectionMetrics:
    call_budget: int
    call_count: int = 0
    retry_count: int = 0
    circuit_state: str = "closed"
    response_classes: Counter[str] = field(default_factory=Counter)


@dataclass
class RequestControls:
    call_budget: int
    call_count: int = 0
    retry_count: int = 0
    circuit_state: str = "closed"
    response_classes: Counter[str] = field(default_factory=Counter)
    last_call_at: float | None = None
    consecutive_failures: int = 0


@dataclass
class SupplierRun:
    source_date: str
    company_db_path: Path
    policy: CompanySyncPolicy
    controls: RequestControls


FetchPage = Callable[[int], Any]
Clock = Callable[[], float]
Sleep = Callable[[float], None]
RandomUniform = Callable[[float, float], float]


def _validate_source_date(source_date: str) -> None:
    if not isinstance(source_date, str) or len(source_date) != 8 or not source_date.isdigit():
        raise ValueError("source_date must be YYYYMMDD")


def _retry_after_seconds(value: str | None) -> float | None:
    try:
        return max(float(value), 0.0) if value else None
    except ValueError:
        return None


def make_verified_company_page_reader(source_date: str, *, api_url: str, service_key: str, rows_per_page: int = 999, timeout_seconds: float = 20) -> FetchPage:
    """Build the unchanged supplier request using the default verifying TLS context."""
    _validate_source_date(source_date)
    begin, end = f"{source_date}0000", f"{source_date}2359"
    tls_context = ssl.create_default_context()

    def fetch_page(page_number: int) -> CompanyPage:
        query = f"?serviceKey={service_key}&inqryDiv=1&inqryBgnDt={begin}&inqryEndDt={end}&numOfRows={rows_per_page}&pageNo={page_number}&type=json"
        request = urllib.request.Request(api_url + query, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(request, context=tls_context, timeout=timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            raise CompanyFetchError("HTTP " + str(error.code), retry_after=_retry_after_seconds(error.headers.get("Retry-After"))) from error
        except (OSError, ValueError, UnicodeDecodeError) as error:
            raise CompanyFetchError(repr(error)) from error
        response = payload.get("response", {})
        header, body = response.get("header", {}), response.get("body", {})
        result_code = str(header.get("resultCode", ""))
        if result_code != "00" and result_code not in EXPLICIT_NOT_FOUND_RESULT_CODES:
            raise CompanyFetchError(f"resultCode={result_code} resultMsg={header.get('resultMsg', '')}")
        items = body.get("items")
        if isinstance(items, Mapping):
            items = items.get("item")
        return CompanyPage(
            tuple(items) if isinstance(items, (list, tuple)) else (), body.get("totalCount"), body.get("pageNo"), body.get("numOfRows"),
            "not_found" if result_code in EXPLICIT_NOT_FOUND_RESULT_CODES else "success", explicit_not_found=result_code in EXPLICIT_NOT_FOUND_RESULT_CODES,
        )

    return fetch_page


def _as_page(raw: Any, requested_page: int, requested_rows: int) -> CompanyPage:
    if not isinstance(raw, CompanyPage):
        raise IncompleteCompanyBatch(f"invalid page response for page {requested_page}")
    if raw.page_number is None:
        raise IncompleteCompanyBatch(f"missing pageNo metadata for page {requested_page}")
    if raw.num_of_rows is None:
        raise IncompleteCompanyBatch(f"missing numOfRows metadata for page {requested_page}")
    try:
        page_number, num_of_rows, total_count = int(raw.page_number), int(raw.num_of_rows), int(raw.total_count)
    except (TypeError, ValueError) as error:
        raise IncompleteCompanyBatch(f"invalid page metadata for page {requested_page}") from error
    if page_number != requested_page:
        raise IncompleteCompanyBatch(f"pageNo metadata drift for page {requested_page}")
    if num_of_rows != requested_rows:
        raise IncompleteCompanyBatch(f"numOfRows metadata drift for page {requested_page}")
    if total_count < 0 or any(not isinstance(item, dict) for item in raw.items):
        raise IncompleteCompanyBatch(f"invalid response schema for page {requested_page}")
    if not raw.items and total_count == 0 and not raw.explicit_not_found:
        raise IncompleteCompanyBatch("empty supplier response is not authoritative")
    return CompanyPage(raw.items, total_count, page_number, num_of_rows, raw.response_class, raw.retry_after, raw.explicit_not_found)


def _record(metrics: CollectionMetrics, controls: RequestControls, response_class: str) -> None:
    metrics.response_classes[response_class] += 1
    controls.response_classes[response_class] += 1
    metrics.circuit_state = controls.circuit_state


def _validate_items(items: tuple[dict[str, Any], ...], metrics: CollectionMetrics) -> None:
    identities: set[tuple[str, str]] = set()
    for item in items:
        bizno = normalize_bizno(item.get("bizno", item.get("brno", item.get("businessNo"))))
        if not bizno or any(not str(item.get(field, "")).strip() for field in REQUIRED_ITEM_FIELDS):
            raise IncompleteCompanyBatch("required supplier fields are invalid", metrics)
        identity = (bizno, str(item["chgDt"]).strip())
        if identity in identities:
            raise IncompleteCompanyBatch("duplicate source identity", metrics)
        identities.add(identity)


def _read_page(page_number: int, fetch_page: FetchPage, rows_per_page: int, policy: CompanySyncPolicy, metrics: CollectionMetrics, controls: RequestControls, *, clock: Clock, sleep: Sleep, random_uniform: RandomUniform) -> CompanyPage:
    for attempt in range(1, policy.max_attempts + 1):
        if controls.circuit_state == "open":
            _record(metrics, controls, "circuit_open")
            raise IncompleteCompanyBatch("circuit open", metrics)
        if controls.call_count >= controls.call_budget:
            controls.circuit_state = "budget_exhausted"
            _record(metrics, controls, "budget_exhausted")
            raise IncompleteCompanyBatch("call budget exhausted", metrics)
        if policy.requests_per_second > 0 and controls.last_call_at is not None:
            delay = (1 / policy.requests_per_second) - (clock() - controls.last_call_at)
            if delay > 0:
                sleep(delay)
        controls.call_count += 1
        metrics.call_count += 1
        controls.last_call_at = clock()
        try:
            page = _as_page(fetch_page(page_number), page_number, rows_per_page)
        except IncompleteCompanyBatch as error:
            _record(metrics, controls, "invalid_response")
            error.metrics = metrics
            raise
        except Exception as error:
            _record(metrics, controls, "exception")
            controls.consecutive_failures += 1
            if controls.consecutive_failures >= policy.circuit_failure_threshold:
                controls.circuit_state = "open"
                metrics.circuit_state = "open"
                raise IncompleteCompanyBatch(f"circuit open after page {page_number} failure: {error}", metrics) from error
            if attempt == policy.max_attempts:
                raise IncompleteCompanyBatch(f"page {page_number} failed after {attempt} attempts: {error}", metrics) from error
            metrics.retry_count += 1
            controls.retry_count += 1
            retry_after = error.retry_after if isinstance(error, CompanyFetchError) else None
            delay = retry_after if retry_after is not None else policy.backoff_seconds * (2 ** (attempt - 1))
            sleep(max(delay + random_uniform(0, policy.jitter_seconds), 0))
            continue
        _record(metrics, controls, page.response_class)
        controls.consecutive_failures = 0
        return page
    raise AssertionError("unreachable")


@dataclass(frozen=True)
class _CollectedBatch:
    batch: CompanyBatch
    metrics: CollectionMetrics


def _collect_complete_change_batch(source_date: str, fetch_page: FetchPage, *, rows_per_page: int, policy: CompanySyncPolicy, controls: RequestControls | None = None, clock: Clock = time.monotonic, sleep: Sleep = time.sleep, random_uniform: RandomUniform = random.uniform) -> _CollectedBatch:
    _validate_source_date(source_date)
    if rows_per_page <= 0 or policy.max_attempts < 1 or policy.daily_call_budget < 1:
        raise ValueError("rows and policy budgets must be positive")
    active_controls = controls or RequestControls(policy.daily_call_budget)
    metrics = CollectionMetrics(active_controls.call_budget, circuit_state=active_controls.circuit_state)
    first = _read_page(1, fetch_page, rows_per_page, policy, metrics, active_controls, clock=clock, sleep=sleep, random_uniform=random_uniform)
    expected = first.total_count
    page_count = math.ceil(expected / rows_per_page)
    items = list(first.items)
    for page_number in range(2, page_count + 1):
        page = _read_page(page_number, fetch_page, rows_per_page, policy, metrics, active_controls, clock=clock, sleep=sleep, random_uniform=random_uniform)
        if page.total_count != expected:
            _record(metrics, active_controls, "invalid_response")
            raise IncompleteCompanyBatch(f"totalCount drift: expected={expected} received={page.total_count}", metrics)
        items.extend(page.items)
    complete_items = tuple(items)
    _validate_items(complete_items, metrics)
    if len(complete_items) != expected:
        raise IncompleteCompanyBatch(f"expected={expected} received={len(complete_items)}", metrics)
    return _CollectedBatch(CompanyBatch(complete_items, expected, page_count, metrics.retry_count), metrics)


def fetch_complete_change_batch(source_date: str, fetch_page: FetchPage, rows_per_page: int = 999, *, policy: CompanySyncPolicy | None = None, clock: Clock = time.monotonic, sleep: Sleep = time.sleep, random_uniform: RandomUniform = random.uniform) -> CompanyBatch:
    return _collect_complete_change_batch(source_date, fetch_page, rows_per_page=rows_per_page, policy=policy or CompanySyncPolicy(), clock=clock, sleep=sleep, random_uniform=random_uniform).batch


def pending_supplier_dates(conn: sqlite3.Connection, through_date: str) -> list[str]:
    _validate_source_date(through_date)
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='company_sync_job_log'").fetchone() is None:
        return []
    rows = conn.execute("SELECT source_date, status FROM company_sync_job_log WHERE job_name='company_changes' AND source_date <= ? ORDER BY source_date", (through_date,)).fetchall()
    unresolved = [source_date for source_date, status in rows if status != "success"]
    if not unresolved:
        return []
    known = dict(rows)
    current, through = datetime.strptime(min(unresolved), "%Y%m%d").date(), datetime.strptime(through_date, "%Y%m%d").date()
    dates = []
    while current <= through:
        source_date = current.strftime("%Y%m%d")
        if known.get(source_date) != "success":
            dates.append(source_date)
        current = current.fromordinal(current.toordinal() + 1)
    return dates


def _write_metrics(conn: sqlite3.Connection, job_name: str, source_date: str, metrics: CollectionMetrics | RequestControls, *, status: str | None = None) -> None:
    with guarded_write_session(conn):
        assignments = "retry_count=?, call_count=?, call_budget=?, circuit_state=?"
        values: list[Any] = [metrics.retry_count, metrics.call_count, metrics.call_budget, metrics.circuit_state]
        if status is not None:
            assignments += ", status=?"
            values.append(status)
        values.extend([job_name, source_date])
        conn.execute(f"UPDATE company_sync_job_log SET {assignments} WHERE job_name=? AND source_date=?", values)
        conn.execute("DELETE FROM company_sync_response_metric WHERE job_name=? AND source_date=?", (job_name, source_date))
        conn.executemany(
            "INSERT INTO company_sync_response_metric (job_name, source_date, response_class, response_count) VALUES (?, ?, ?, ?)",
            [(job_name, source_date, response_class, count) for response_class, count in sorted(metrics.response_classes.items())],
        )


def start_supplier_run(source_date: str, company_db_path: str | Path, *, policy: CompanySyncPolicy | None = None) -> SupplierRun:
    """Restore or create the one persisted supplier request budget for this run date."""
    _validate_source_date(source_date)
    active_policy = policy or CompanySyncPolicy()
    path = Path(company_db_path).resolve()
    conn = sqlite3.connect(path, timeout=30)
    try:
        ensure_locality_schema(conn)
        row = conn.execute("SELECT call_count, retry_count, call_budget, circuit_state FROM company_sync_job_log WHERE job_name='company_changes_run' AND source_date=?", (source_date,)).fetchone()
        if row is None:
            start_sync_job(conn, "company_changes_run", source_date, call_budget=active_policy.daily_call_budget)
            controls = RequestControls(active_policy.daily_call_budget)
        else:
            response_classes = Counter(dict(conn.execute("SELECT response_class, response_count FROM company_sync_response_metric WHERE job_name='company_changes_run' AND source_date=?", (source_date,)).fetchall()))
            controls = RequestControls(int(row[2]), int(row[0]), int(row[1]), str(row[3]), response_classes)
        return SupplierRun(source_date, path, active_policy, controls)
    finally:
        conn.close()


def _persist_run(run: SupplierRun, *, status: str | None = None) -> None:
    conn = sqlite3.connect(run.company_db_path, timeout=30)
    try:
        _write_metrics(conn, "company_changes_run", run.source_date, run.controls, status=status)
    finally:
        conn.close()


def finish_supplier_run(run: SupplierRun) -> None:
    _persist_run(run, status="success" if run.controls.circuit_state == "closed" else "failed")


def sync_company_change_date(source_date: str, fetch_page: FetchPage, company_db_path: str | Path, *, rows_per_page: int = 999, policy: CompanySyncPolicy | None = None, run: SupplierRun | None = None, clock: Clock = time.monotonic, sleep: Sleep = time.sleep, random_uniform: RandomUniform = random.uniform) -> ChangeSummary:
    """Collect a verified batch and apply it only after Task 1 guarded writes are ready."""
    _validate_source_date(source_date)
    active_policy = policy or CompanySyncPolicy()
    path = Path(company_db_path).resolve()
    if run is not None and run.company_db_path != path:
        raise ValueError("supplier run belongs to a different company database")
    controls = run.controls if run is not None else RequestControls(active_policy.daily_call_budget)
    conn = sqlite3.connect(path, timeout=30)
    try:
        ensure_locality_schema(conn)
        start_sync_job(conn, "company_changes", source_date, call_budget=controls.call_budget)
        try:
            collected = _collect_complete_change_batch(source_date, fetch_page, rows_per_page=rows_per_page, policy=active_policy, controls=controls, clock=clock, sleep=sleep, random_uniform=random_uniform)
        except IncompleteCompanyBatch as error:
            metrics = error.metrics or CollectionMetrics(controls.call_budget, circuit_state=controls.circuit_state)
            _write_metrics(conn, "company_changes", source_date, metrics)
            fail_sync_job(conn, "company_changes", source_date, str(error))
            if run is not None:
                _persist_run(run)
            raise
        try:
            summary = apply_company_changes(conn, collected.batch.items, source_date, f"company_changes:{source_date}", datetime.now().astimezone().isoformat(timespec="seconds"))
        except Exception as error:
            _write_metrics(conn, "company_changes", source_date, collected.metrics)
            fail_sync_job(conn, "company_changes", source_date, repr(error))
            if run is not None:
                _persist_run(run)
            raise
        _write_metrics(conn, "company_changes", source_date, collected.metrics)
        finish_sync_job(conn, "company_changes", source_date, expected_rows=collected.batch.total_count, received_rows=len(collected.batch.items), page_count=collected.batch.page_count)
        if run is not None:
            _persist_run(run)
        return summary
    finally:
        conn.close()
