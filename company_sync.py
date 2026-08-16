from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
import math
from pathlib import Path
import random
import sqlite3
import ssl
import time
from typing import Any, Callable, Mapping
import urllib.error
import urllib.request
import json

from company_locality import (
    ChangeSummary,
    apply_company_changes,
    ensure_locality_schema,
    fail_sync_job,
    finish_sync_job,
    normalize_bizno,
    start_sync_job,
)
from maintenance_lock import guarded_write_session


EXPLICIT_NOT_FOUND_RESULT_CODES = {"03"}
REQUIRED_ITEM_FIELDS = ("rgnNm", "hdoffceDivNm", "chgDt")


class IncompleteCompanyBatch(RuntimeError):
    """Raised when an API change date cannot be verified as complete."""

    def __init__(self, message: str, metrics: "CollectionMetrics | None" = None):
        super().__init__(message)
        self.metrics = metrics


class CompanyFetchError(RuntimeError):
    """A retryable page-read failure, optionally honoring a Retry-After header."""

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
    total_count: int
    page_number: int | None = None
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
    last_call_at: float | None = None
    consecutive_failures: int = 0


FetchPage = Callable[[int], Any]


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(float(value), 0.0)
    except ValueError:
        return None


def make_verified_company_page_reader(
    source_date: str,
    *,
    api_url: str,
    service_key: str,
    rows_per_page: int = 999,
    timeout_seconds: float = 20,
) -> FetchPage:
    """Build the existing supplier API request with certificate verification enabled."""
    begin = f"{source_date}0000"
    end = f"{source_date}2359"
    tls_context = ssl.create_default_context()

    def fetch_page(page_number: int) -> CompanyPage:
        query = (
            f"?serviceKey={service_key}&inqryDiv=1&inqryBgnDt={begin}&inqryEndDt={end}"
            f"&numOfRows={rows_per_page}&pageNo={page_number}&type=json"
        )
        request = urllib.request.Request(api_url + query, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(request, context=tls_context, timeout=timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            raise CompanyFetchError(
                f"HTTP {error.code}", retry_after=_retry_after_seconds(error.headers.get("Retry-After"))
            ) from error
        except (OSError, ValueError, UnicodeDecodeError) as error:
            raise CompanyFetchError(repr(error)) from error

        header = payload.get("response", {}).get("header", {})
        result_code = str(header.get("resultCode", ""))
        if result_code in EXPLICIT_NOT_FOUND_RESULT_CODES:
            return CompanyPage((), 0, page_number=page_number, response_class="not_found", explicit_not_found=True)
        if result_code != "00":
            raise CompanyFetchError(f"resultCode={result_code} resultMsg={header.get('resultMsg', '')}")
        body = payload.get("response", {}).get("body", {})
        items = body.get("items")
        if isinstance(items, Mapping):
            items = items.get("item")
        return CompanyPage(
            tuple(items) if isinstance(items, (list, tuple)) else (),
            body.get("totalCount"),
            page_number=body.get("pageNo", page_number),
            response_class="success",
        )

    return fetch_page


def _as_page(raw: Any, requested_page: int) -> CompanyPage:
    if isinstance(raw, CompanyPage):
        page = raw
    elif isinstance(raw, tuple) and len(raw) == 2:
        page = CompanyPage(tuple(raw[0]) if isinstance(raw[0], (list, tuple)) else (), raw[1], requested_page)
    else:
        raise IncompleteCompanyBatch(f"invalid page response for page {requested_page}")
    if page.page_number is not None and int(page.page_number) != requested_page:
        raise IncompleteCompanyBatch(f"page metadata drift for page {requested_page}")
    try:
        total_count = int(page.total_count)
    except (TypeError, ValueError) as error:
        raise IncompleteCompanyBatch(f"invalid totalCount for page {requested_page}") from error
    if total_count < 0:
        raise IncompleteCompanyBatch(f"invalid totalCount for page {requested_page}")
    if not isinstance(page.items, tuple) or any(not isinstance(item, dict) for item in page.items):
        raise IncompleteCompanyBatch(f"invalid items schema for page {requested_page}")
    if not page.items and total_count == 0 and not page.explicit_not_found:
        raise IncompleteCompanyBatch("empty supplier response is not authoritative")
    return CompanyPage(
        page.items,
        total_count,
        requested_page,
        page.response_class,
        page.retry_after,
        page.explicit_not_found,
    )


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


def _wait_for_rate_limit(metrics: CollectionMetrics, policy: CompanySyncPolicy, sleep: Callable[[float], None]) -> None:
    if policy.requests_per_second <= 0:
        return
    if metrics.last_call_at is not None:
        delay = (1 / policy.requests_per_second) - (time.monotonic() - metrics.last_call_at)
        if delay > 0:
            sleep(delay)


def _read_page(
    page_number: int,
    fetch_page: FetchPage,
    policy: CompanySyncPolicy,
    metrics: CollectionMetrics,
    sleep: Callable[[float], None],
) -> CompanyPage:
    for attempt in range(1, policy.max_attempts + 1):
        if metrics.call_count >= policy.daily_call_budget:
            metrics.circuit_state = "budget_exhausted"
            metrics.response_classes["budget_exhausted"] += 1
            raise IncompleteCompanyBatch("call budget exhausted", metrics)
        _wait_for_rate_limit(metrics, policy, sleep)
        metrics.call_count += 1
        metrics.last_call_at = time.monotonic()
        try:
            page = _as_page(fetch_page(page_number), page_number)
            metrics.response_classes[page.response_class] += 1
            metrics.consecutive_failures = 0
            return page
        except IncompleteCompanyBatch:
            metrics.response_classes["invalid_response"] += 1
            raise
        except Exception as error:
            metrics.response_classes["exception"] += 1
            metrics.consecutive_failures += 1
            if attempt == policy.max_attempts:
                if metrics.consecutive_failures >= policy.circuit_failure_threshold:
                    metrics.circuit_state = "open"
                raise IncompleteCompanyBatch(f"page {page_number} failed after {attempt} attempts: {error}", metrics) from error
            metrics.retry_count += 1
            retry_after = error.retry_after if isinstance(error, CompanyFetchError) else None
            delay = retry_after if retry_after is not None else policy.backoff_seconds * (2 ** (attempt - 1))
            delay += random.uniform(0, policy.jitter_seconds)
            if delay > 0:
                sleep(delay)
    raise AssertionError("unreachable")


@dataclass(frozen=True)
class _CollectedBatch:
    batch: CompanyBatch
    metrics: CollectionMetrics


def _collect_complete_change_batch(
    source_date: str,
    fetch_page: FetchPage,
    *,
    rows_per_page: int,
    policy: CompanySyncPolicy,
    sleep: Callable[[float], None],
) -> _CollectedBatch:
    if not source_date or len(source_date) != 8 or not source_date.isdigit():
        raise ValueError("source_date must be YYYYMMDD")
    if rows_per_page <= 0:
        raise ValueError("rows_per_page must be positive")
    if policy.max_attempts < 1 or policy.daily_call_budget < 1:
        raise ValueError("policy attempt and call budgets must be positive")
    metrics = CollectionMetrics(call_budget=policy.daily_call_budget)
    first = _read_page(1, fetch_page, policy, metrics, sleep)
    expected = first.total_count
    page_count = math.ceil(expected / rows_per_page)
    all_items = list(first.items)
    for page_number in range(2, page_count + 1):
        page = _read_page(page_number, fetch_page, policy, metrics, sleep)
        if page.total_count != expected:
            raise IncompleteCompanyBatch(
                f"totalCount drift: expected={expected} received={page.total_count}", metrics
            )
        all_items.extend(page.items)
    items = tuple(all_items)
    _validate_items(items, metrics)
    if len(items) != expected:
        raise IncompleteCompanyBatch(f"expected={expected} received={len(items)}", metrics)
    return _CollectedBatch(CompanyBatch(items, expected, page_count, metrics.retry_count), metrics)


def fetch_complete_change_batch(
    source_date: str,
    fetch_page: FetchPage,
    rows_per_page: int = 999,
    *,
    policy: CompanySyncPolicy | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> CompanyBatch:
    """Return one immutable supplier batch only after every source page verifies."""
    collected = _collect_complete_change_batch(
        source_date,
        fetch_page,
        rows_per_page=rows_per_page,
        policy=policy or CompanySyncPolicy(),
        sleep=sleep,
    )
    return collected.batch


def pending_supplier_dates(conn: sqlite3.Connection, through_date: str) -> list[str]:
    """Return known failed/missing supplier dates through the requested source date."""
    if len(through_date) != 8 or not through_date.isdigit():
        raise ValueError("through_date must be YYYYMMDD")
    table_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='company_sync_job_log'"
    ).fetchone()
    if not table_exists:
        return []
    rows = conn.execute(
        "SELECT source_date, status FROM company_sync_job_log "
        "WHERE job_name='company_changes' AND source_date <= ? ORDER BY source_date",
        (through_date,),
    ).fetchall()
    unresolved = [source_date for source_date, status in rows if status != "success"]
    if not unresolved:
        return []
    known = {source_date: status for source_date, status in rows}
    current = datetime.strptime(min(unresolved), "%Y%m%d").date()
    through = datetime.strptime(through_date, "%Y%m%d").date()
    dates = []
    while current <= through:
        source_date = current.strftime("%Y%m%d")
        if known.get(source_date) != "success":
            dates.append(source_date)
        current = current.fromordinal(current.toordinal() + 1)
    return dates


def _persist_metrics(conn: sqlite3.Connection, source_date: str, metrics: CollectionMetrics) -> None:
    with guarded_write_session(conn):
        conn.execute(
            "UPDATE company_sync_job_log SET retry_count=?, call_count=?, call_budget=?, circuit_state=? "
            "WHERE job_name='company_changes' AND source_date=?",
            (metrics.retry_count, metrics.call_count, metrics.call_budget, metrics.circuit_state, source_date),
        )
        conn.execute(
            "DELETE FROM company_sync_response_metric WHERE job_name='company_changes' AND source_date=?",
            (source_date,),
        )
        conn.executemany(
            "INSERT INTO company_sync_response_metric (job_name, source_date, response_class, response_count) "
            "VALUES ('company_changes', ?, ?, ?)",
            [(source_date, response_class, count) for response_class, count in sorted(metrics.response_classes.items())],
        )


def sync_company_change_date(
    source_date: str,
    fetch_page: FetchPage,
    company_db_path: str | Path,
    *,
    rows_per_page: int = 999,
    policy: CompanySyncPolicy | None = None,
) -> ChangeSummary:
    """Fetch, verify, and apply one supplier change date through Task 1 guards."""
    active_policy = policy or CompanySyncPolicy()
    conn = sqlite3.connect(company_db_path, timeout=30)
    try:
        ensure_locality_schema(conn)
        start_sync_job(conn, "company_changes", source_date, call_budget=active_policy.daily_call_budget)
        try:
            collected = _collect_complete_change_batch(
                source_date,
                fetch_page,
                rows_per_page=rows_per_page,
                policy=active_policy,
                sleep=time.sleep,
            )
        except IncompleteCompanyBatch as error:
            metrics = error.metrics or CollectionMetrics(call_budget=active_policy.daily_call_budget)
            _persist_metrics(conn, source_date, metrics)
            fail_sync_job(conn, "company_changes", source_date, str(error))
            raise
        try:
            summary = apply_company_changes(
                conn,
                collected.batch.items,
                source_date,
                f"company_changes:{source_date}",
                datetime.now().astimezone().isoformat(timespec="seconds"),
            )
        except Exception as error:
            _persist_metrics(conn, source_date, collected.metrics)
            fail_sync_job(conn, "company_changes", source_date, repr(error))
            raise
        _persist_metrics(conn, source_date, collected.metrics)
        finish_sync_job(
            conn,
            "company_changes",
            source_date,
            expected_rows=collected.batch.total_count,
            received_rows=len(collected.batch.items),
            page_count=collected.batch.page_count,
        )
        return summary
    finally:
        conn.close()
