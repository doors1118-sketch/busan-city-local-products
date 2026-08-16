"""Rolling, per-supplier locality revalidation."""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import random
import sqlite3
import ssl
import threading
import time
from typing import Any, Iterable, Protocol
import urllib.error
import urllib.parse
import urllib.request

from company_locality import (
    ChangeSummary,
    apply_company_changes,
    ensure_locality_schema,
    normalize_bizno,
    SEOUL,
    start_sync_job,
)
from maintenance_lock import LocalityPaths, configure_locality_paths, guarded_write_session


SUCCESS_RESULT_CODE = "00"
EXPLICIT_NOT_FOUND_RESULT_CODES = {"03"}
REQUIRED_ITEM_FIELDS = ("bizno", "rgnNm", "hdoffceDivNm", "chgDt")
MAX_RESPONSE_BYTES = 1_000_000


class CompanyLookupError(RuntimeError):
    """Raised when one direct supplier response cannot prove a current record."""

    def __init__(self, message: str, *, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class CompanySourceClient(Protocol):
    def lookup(self, bizno: str) -> dict[str, Any] | list[Any]: ...


@dataclass(frozen=True)
class RevalidationPolicy:
    max_attempts: int = 3
    requests_per_second: float = 5.0
    daily_call_budget: int = 10_000
    circuit_failure_threshold: int = 3
    backoff_seconds: float = 0.5
    jitter_seconds: float = 0.2


@dataclass
class _RequestControls:
    call_budget: int
    call_count: int = 0
    retry_count: int = 0
    circuit_state: str = "closed"
    response_classes: Counter[str] = field(default_factory=Counter)
    last_call_at: float | None = None
    consecutive_failures: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


@dataclass(frozen=True)
class RevalidationSummary:
    selected: int = 0
    calls: int = 0
    retries: int = 0
    activated: int = 0
    deactivated: int = 0
    branch_changed: int = 0
    unchanged: int = 0
    failed: int = 0
    deferred: int = 0
    unverified: int = 0
    circuit_state: str = "closed"


class VerifiedCompanyLookupClient:
    """Direct lookup client with a deliberately narrow, verified response contract."""

    def __init__(self, api_url: str, service_key: str, *, timeout_seconds: float = 20):
        self.api_url = api_url
        self.service_key = service_key
        self.timeout_seconds = timeout_seconds
        self.tls_context = ssl.create_default_context()

    def lookup(self, bizno: str) -> dict[str, Any] | list[Any]:
        normalized = normalize_bizno(bizno)
        if not normalized:
            raise CompanyLookupError("business number is required")
        query = urllib.parse.urlencode(
            {
                "serviceKey": self.service_key,
                "inqryDiv": "2",
                "bizno": normalized,
                "numOfRows": "1",
                "pageNo": "1",
                "type": "json",
            }
        )
        request = urllib.request.Request(self.api_url + "?" + query, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(request, context=self.tls_context, timeout=self.timeout_seconds) as response:
                payload_bytes = response.read(MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as error:
            raise CompanyLookupError(
                "HTTP " + str(error.code), retry_after=_retry_after_seconds(error.headers.get("Retry-After"))
            ) from error
        except (OSError, UnicodeDecodeError) as error:
            raise CompanyLookupError(repr(error)) from error
        if len(payload_bytes) > MAX_RESPONSE_BYTES:
            raise CompanyLookupError("supplier response exceeds size limit")
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
            response = payload["response"]
            header = response["header"]
            body = response.get("body", {})
        except (KeyError, TypeError, ValueError, UnicodeDecodeError) as error:
            raise CompanyLookupError("invalid supplier response schema") from error
        result_code = str(header.get("resultCode", ""))
        if result_code in EXPLICIT_NOT_FOUND_RESULT_CODES:
            return []
        if result_code != SUCCESS_RESULT_CODE:
            raise CompanyLookupError(f"resultCode={result_code} resultMsg={header.get('resultMsg', '')}")
        item = _direct_lookup_item(body, normalized)
        return item


def make_verified_company_lookup_client(
    api_url: str, service_key: str, *, timeout_seconds: float = 20
) -> VerifiedCompanyLookupClient:
    return VerifiedCompanyLookupClient(api_url, service_key, timeout_seconds=timeout_seconds)


def bucket_for_bizno(bizno: str, bucket_count: int = 30) -> int:
    """Return the stable SHA-256 bucket for a normalized business number."""
    normalized = normalize_bizno(bizno)
    if not normalized or bucket_count < 1:
        raise ValueError("a normalized business number and positive bucket_count are required")
    return int(hashlib.sha256(normalized.encode("ascii")).hexdigest()[:8], 16) % bucket_count


def biznos_for_bucket(conn: sqlite3.Connection, bucket: int, bucket_count: int = 30) -> list[str]:
    if bucket_count < 1 or bucket < 0 or bucket >= bucket_count:
        raise ValueError("bucket must be within bucket_count")
    rows = conn.execute("SELECT bizno FROM company_locality_status ORDER BY bizno").fetchall()
    return [bizno for (bizno,) in rows if bucket_for_bizno(bizno, bucket_count) == bucket]


def _retry_after_seconds(value: str | None) -> float | None:
    try:
        return max(float(value), 0.0) if value else None
    except ValueError:
        return None


def _direct_lookup_item(body: Any, requested_bizno: str) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise CompanyLookupError("invalid supplier response schema")
    items = body.get("items")
    if isinstance(items, dict):
        items = items.get("item")
    if isinstance(items, dict):
        items = (items,)
    if not isinstance(items, (list, tuple)) or len(items) != 1:
        raise CompanyLookupError("successful supplier response is not authoritative")
    try:
        total_count = int(body["totalCount"])
        page_number = int(body["pageNo"])
        rows_per_page = int(body["numOfRows"])
    except (KeyError, TypeError, ValueError) as error:
        raise CompanyLookupError("invalid supplier response pagination") from error
    item = items[0]
    if total_count != 1 or page_number != 1 or rows_per_page != 1 or not isinstance(item, dict):
        raise CompanyLookupError("successful supplier response is not authoritative")
    if normalize_bizno(item.get("bizno")) != requested_bizno:
        raise CompanyLookupError("supplier response business number mismatch")
    if any(not str(item.get(field, "")).strip() for field in REQUIRED_ITEM_FIELDS):
        raise CompanyLookupError("supplier response required fields are invalid")
    return item


def _seoul_timestamp(value: date | datetime | str) -> str:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.strftime("%Y-%m-%d") + " 12:00:00+09:00"
    if isinstance(value, datetime):
        converted = value.astimezone(SEOUL)
        return converted.strftime("%Y-%m-%d %H:%M:%S%z")[:-2] + ":" + converted.strftime("%z")[-2:]
    text = str(value).strip()
    if not text:
        raise ValueError("run time is required")
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.strftime("%Y-%m-%d %H:%M:%S") + "+09:00"
    converted = parsed.astimezone(SEOUL)
    return converted.strftime("%Y-%m-%d %H:%M:%S%z")[:-2] + ":" + converted.strftime("%z")[-2:]


def _queue_retry_at(run_at: str, attempt_count: int) -> str:
    parsed = datetime.fromisoformat(run_at)
    seconds = min(300 * (2 ** max(attempt_count - 1, 0)), 86_400)
    return (parsed + timedelta(seconds=seconds)).isoformat(sep=" ", timespec="seconds")


def _start_job(conn: sqlite3.Connection, run_at: str, controls: _RequestControls) -> str:
    source_date = run_at[:10].replace("-", "")
    start_sync_job(conn, "company_revalidation", source_date, call_budget=controls.call_budget)
    return source_date


def _finish_job(conn: sqlite3.Connection, source_date: str, summary: RevalidationSummary, controls: _RequestControls) -> None:
    with guarded_write_session(conn):
        conn.execute(
            "UPDATE company_sync_job_log SET status=?, completed_at=?, expected_rows=?, received_rows=?, "
            "retry_count=?, call_count=?, call_budget=?, circuit_state=? "
            "WHERE job_name='company_revalidation' AND source_date=?",
            (
                "failed" if controls.circuit_state == "open" else "success",
                _seoul_timestamp(datetime.now(SEOUL)), summary.selected,
                summary.activated + summary.deactivated + summary.branch_changed + summary.unchanged,
                controls.retry_count, controls.call_count, controls.call_budget, controls.circuit_state, source_date,
            ),
        )
        conn.execute(
            "DELETE FROM company_sync_response_metric WHERE job_name='company_revalidation' AND source_date=?",
            (source_date,),
        )
        conn.executemany(
            "INSERT INTO company_sync_response_metric (job_name, source_date, response_class, response_count) VALUES (?, ?, ?, ?)",
            [("company_revalidation", source_date, key, value) for key, value in sorted(controls.response_classes.items())],
        )


def _request_item(
    source_client: CompanySourceClient,
    bizno: str,
    policy: RevalidationPolicy,
    controls: _RequestControls,
) -> tuple[str, dict[str, Any] | None, str | None]:
    with controls.lock:
        return _request_item_locked(source_client, bizno, policy, controls)


def _request_item_locked(
    source_client: CompanySourceClient,
    bizno: str,
    policy: RevalidationPolicy,
    controls: _RequestControls,
) -> tuple[str, dict[str, Any] | None, str | None]:
    for attempt in range(1, policy.max_attempts + 1):
        if controls.circuit_state == "open":
            controls.response_classes["circuit_open"] += 1
            return "deferred", None, "circuit open"
        if controls.call_count >= controls.call_budget:
            controls.circuit_state = "budget_exhausted"
            controls.response_classes["budget_exhausted"] += 1
            return "deferred", None, "call budget exhausted"
        if policy.requests_per_second > 0 and controls.last_call_at is not None:
            delay = (1 / policy.requests_per_second) - (time.monotonic() - controls.last_call_at)
            if delay > 0:
                time.sleep(delay)
        controls.call_count += 1
        controls.last_call_at = time.monotonic()
        try:
            response = source_client.lookup(bizno)
            if response == []:
                controls.response_classes["not_found"] += 1
                controls.consecutive_failures = 0
                return "not_found", None, None
            if response is None:
                controls.response_classes["unauthoritative_empty"] += 1
                controls.consecutive_failures += 1
                if controls.consecutive_failures >= policy.circuit_failure_threshold:
                    controls.circuit_state = "open"
                    return "failed", None, None
                if attempt == policy.max_attempts:
                    return "failed", None, None
                controls.retry_count += 1
                time.sleep(max(policy.backoff_seconds * (2 ** (attempt - 1)) + random.uniform(0, policy.jitter_seconds), 0))
                continue
            if not isinstance(response, dict):
                raise CompanyLookupError("invalid supplier response schema")
            normalized = normalize_bizno(response.get("bizno", response.get("brno", response.get("businessNo"))))
            if normalized != bizno or any(not str(response.get(field, "")).strip() for field in REQUIRED_ITEM_FIELDS[1:]):
                raise CompanyLookupError("invalid supplier response schema")
            controls.response_classes["success"] += 1
            controls.consecutive_failures = 0
            return "success", response, None
        except CompanyLookupError as caught:
            detail = str(caught)
            response_class = "invalid_response" if "response" in detail else "lookup_error"
            retry_after = caught.retry_after
        except Exception as caught:
            response_class = "exception"
            retry_after = None
            detail = repr(caught)
        controls.response_classes[response_class] += 1
        controls.consecutive_failures += 1
        if controls.consecutive_failures >= policy.circuit_failure_threshold:
            controls.circuit_state = "open"
            return "failed", None, detail
        if attempt == policy.max_attempts:
            return "failed", None, detail
        controls.retry_count += 1
        backoff = retry_after if retry_after is not None else policy.backoff_seconds * (2 ** (attempt - 1))
        time.sleep(max(backoff + random.uniform(0, policy.jitter_seconds), 0))
    raise AssertionError("unreachable")


def _update_queue(
    conn: sqlite3.Connection,
    bizno: str,
    outcome: str,
    response_class: str,
    run_at: str,
    error_detail: str | None,
) -> None:
    existing = conn.execute(
        "SELECT attempt_count FROM company_revalidation_queue WHERE bizno=?", (bizno,)
    ).fetchone()
    attempt_count = (existing[0] if existing else 0) + (outcome in {"failed", "deferred"})
    if outcome in {"success", "not_found"}:
        status, next_attempt, last_success = "complete", None, run_at
    elif outcome == "deferred":
        status, next_attempt, last_success = "deferred_budget", _queue_retry_at(run_at, max(attempt_count, 1)), None
    else:
        status, next_attempt, last_success = "failed", _queue_retry_at(run_at, max(attempt_count, 1)), None
    with guarded_write_session(conn):
        conn.execute(
            "INSERT INTO company_revalidation_queue "
            "(bizno, status, attempt_count, last_response_class, next_attempt_at, last_attempt_at, last_success_at, error_detail) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(bizno) DO UPDATE SET status=excluded.status, attempt_count=excluded.attempt_count, "
            "last_response_class=excluded.last_response_class, next_attempt_at=excluded.next_attempt_at, "
            "last_attempt_at=excluded.last_attempt_at, last_success_at=COALESCE(excluded.last_success_at, company_revalidation_queue.last_success_at), "
            "error_detail=excluded.error_detail",
            (bizno, status, attempt_count, response_class, next_attempt, run_at, last_success, error_detail),
        )


def _mark_verified(conn: sqlite3.Connection, bizno: str, verified_at: str) -> None:
    with guarded_write_session(conn):
        conn.execute(
            "UPDATE company_locality_status SET last_verified_at=? WHERE bizno=?", (verified_at, bizno)
        )


def _apply_revalidation_result(
    conn: sqlite3.Connection,
    bizno: str,
    run_at: str,
    controls: _RequestControls,
    outcome: str,
    item: dict[str, Any] | None,
    error_detail: str | None,
) -> RevalidationSummary:
    before = conn.execute("SELECT status FROM company_locality_status WHERE bizno=?", (bizno,)).fetchone()
    if outcome == "success" and item is not None:
        result: ChangeSummary = apply_company_changes(
            conn, [item], run_at[:10].replace("-", ""), "company_revalidation:" + run_at[:10], run_at
        )
        after = conn.execute("SELECT status FROM company_locality_status WHERE bizno=?", (bizno,)).fetchone()
        _mark_verified(conn, bizno, run_at)
        _update_queue(conn, bizno, "success", "success", run_at, None)
        before_status, after_status = (before[0] if before else "unverified"), (after[0] if after else "unverified")
        return RevalidationSummary(
            selected=1, calls=0, activated=int(after_status == "active_local" and before_status != "active_local"),
            deactivated=int(after_status == "moved_out" and before_status != "moved_out"),
            branch_changed=int(after_status == "branch_changed" and before_status != "branch_changed"),
            unchanged=int(before_status == after_status), unverified=int(after_status == "unverified"),
        )
    if outcome == "not_found":
        if before is not None:
            _mark_verified(conn, bizno, run_at)
        _update_queue(conn, bizno, "not_found", "not_found", run_at, None)
        return RevalidationSummary(selected=1, unchanged=1)
    response_class = "budget_exhausted" if outcome == "deferred" and controls.circuit_state == "budget_exhausted" else (
        "circuit_open" if outcome == "deferred" else "unauthoritative_empty" if error_detail is None else "failed"
    )
    _update_queue(conn, bizno, outcome, response_class, run_at, error_detail)
    return RevalidationSummary(selected=1, failed=int(outcome == "failed"), deferred=int(outcome == "deferred"))


def _combine(summaries: Iterable[RevalidationSummary], controls: _RequestControls) -> RevalidationSummary:
    totals = {name: 0 for name in RevalidationSummary.__dataclass_fields__ if name not in {"calls", "retries", "circuit_state"}}
    for summary in summaries:
        for name in totals:
            totals[name] += getattr(summary, name)
    return RevalidationSummary(**totals, calls=controls.call_count, retries=controls.retry_count, circuit_state=controls.circuit_state)


def _run_biznos(
    conn: sqlite3.Connection,
    source_client: CompanySourceClient,
    biznos: Iterable[str],
    run_at: str,
    policy: RevalidationPolicy,
    controls: _RequestControls,
    workers: int,
) -> RevalidationSummary:
    ordered_biznos = tuple(biznos)
    max_workers = min(max(workers, 1), 8)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        outcomes = list(
            executor.map(
                lambda bizno: (bizno, _request_item(source_client, bizno, policy, controls)),
                ordered_biznos,
            )
        )
    return _combine(
        (
            _apply_revalidation_result(conn, bizno, run_at, controls, outcome, item, error_detail)
            for bizno, (outcome, item, error_detail) in outcomes
        ),
        controls,
    )


def drain_revalidation_queue(
    conn: sqlite3.Connection,
    source_client: CompanySourceClient,
    run_at: date | datetime | str,
    request_budget: int,
) -> RevalidationSummary:
    if request_budget < 1:
        raise ValueError("request_budget must be positive")
    ensure_locality_schema(conn)
    timestamp = _seoul_timestamp(run_at)
    policy = RevalidationPolicy(daily_call_budget=request_budget)
    controls = _RequestControls(request_budget)
    source_date = _start_job(conn, timestamp, controls)
    due = [
        row[0]
        for row in conn.execute(
            "SELECT bizno FROM company_revalidation_queue "
            "WHERE status IN ('pending','deferred_budget','failed') AND (next_attempt_at IS NULL OR next_attempt_at <= ?) "
            "ORDER BY next_attempt_at, bizno",
            (timestamp,),
        )
    ]
    summary = _run_biznos(conn, source_client, due, timestamp, policy, controls, 8)
    _finish_job(conn, source_date, summary, controls)
    return summary


def revalidate_bucket(
    conn: sqlite3.Connection,
    source_client: CompanySourceClient,
    run_date: date | datetime | str,
    bucket_count: int = 30,
    workers: int = 8,
    *,
    policy: RevalidationPolicy | None = None,
) -> RevalidationSummary:
    if workers < 1 or bucket_count < 1:
        raise ValueError("workers and bucket_count must be positive")
    ensure_locality_schema(conn)
    timestamp = _seoul_timestamp(run_date)
    active_policy = policy or RevalidationPolicy()
    controls = _RequestControls(active_policy.daily_call_budget)
    source_date = _start_job(conn, timestamp, controls)
    due = [
        row[0]
        for row in conn.execute(
            "SELECT bizno FROM company_revalidation_queue "
            "WHERE status IN ('pending','deferred_budget','failed') AND (next_attempt_at IS NULL OR next_attempt_at <= ?) "
            "ORDER BY next_attempt_at, bizno",
            (timestamp,),
        )
    ]
    bucket = datetime.fromisoformat(timestamp).date().toordinal() % bucket_count
    bucket_biznos = [bizno for bizno in biznos_for_bucket(conn, bucket, bucket_count) if bizno not in set(due)]
    summary = _combine(
        (
            _run_biznos(conn, source_client, due, timestamp, active_policy, controls, min(workers, 8)),
            _run_biznos(conn, source_client, bucket_biznos, timestamp, active_policy, controls, min(workers, 8)),
        ),
        controls,
    )
    _finish_job(conn, source_date, summary, controls)
    return summary


def _configure_cli_paths(company_db: str, procurement_db: str, coordination_dir: str) -> None:
    root = os.path.abspath(coordination_dir)
    configure_locality_paths(
        LocalityPaths(
            os.path.abspath(company_db), os.path.abspath(procurement_db),
            os.path.join(root, "maintenance.lock"), os.path.join(root, "transition.json"),
            os.path.join(root, "locality_writes_paused"), os.path.join(root, "active_locality_generation.json"),
        )
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Revalidate one stable supplier locality bucket.")
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--bucket-count", type=int, default=30)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--company-db", default="busan_companies_master.db")
    parser.add_argument("--procurement-db", default=os.environ.get("PROCUREMENT_DB", "procurement.db"))
    parser.add_argument("--coordination-dir", default=os.environ.get("LOCALITY_COORDINATION_DIR", "sync_log/locality"))
    parser.add_argument("--api-url", default=os.environ.get("COMPANY_API_URL"))
    parser.add_argument("--service-key", default=os.environ.get("COMPANY_SERVICE_KEY"))
    args = parser.parse_args(argv)
    bucket = args.date.toordinal() % args.bucket_count
    if args.dry_run:
        database_uri = Path(args.company_db).resolve().as_uri() + "?mode=ro"
        conn = sqlite3.connect(database_uri, uri=True, timeout=30)
        try:
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            due = conn.execute(
                "SELECT COUNT(*) FROM company_revalidation_queue WHERE status IN ('pending','deferred_budget','failed')"
            ).fetchone()[0] if "company_revalidation_queue" in tables else 0
            biznos = biznos_for_bucket(conn, bucket, args.bucket_count) if "company_locality_status" in tables else []
            print(f"due={due} bucket={bucket} selected={len(biznos)}")
            return 0
        finally:
            conn.close()
    _configure_cli_paths(args.company_db, args.procurement_db, args.coordination_dir)
    conn = sqlite3.connect(args.company_db, timeout=30)
    try:
        ensure_locality_schema(conn)
        if not args.api_url or not args.service_key:
            parser.error("--apply requires --api-url and --service-key or their environment variables")
        summary = revalidate_bucket(
            conn, make_verified_company_lookup_client(args.api_url, args.service_key), args.date, args.bucket_count
        )
        print(f"selected={summary.selected} calls={summary.calls} failed={summary.failed} deferred={summary.deferred}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
