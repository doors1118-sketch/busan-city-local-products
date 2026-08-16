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
from uuid import uuid4

from company_locality import (
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
    lease_seconds: int = 300
    clock: Any = time.monotonic
    sleep: Any = time.sleep
    random_uniform: Any = random.uniform


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
class _Claim:
    bizno: str
    owner: str
    attempt_count: int


@dataclass(frozen=True)
class _RequestOutcome:
    kind: str
    response_class: str
    item: dict[str, Any] | None = None
    error_detail: str | None = None
    retry_after_seconds: float | None = None


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


def _queue_retry_at(
    run_at: str, attempt_count: int, retry_after_seconds: float | None, policy: RevalidationPolicy
) -> str:
    exponential = min(policy.backoff_seconds * (2 ** max(attempt_count - 1, 0)), 86_400)
    delay = max(retry_after_seconds or 0, exponential + policy.random_uniform(0, policy.jitter_seconds))
    return (datetime.fromisoformat(run_at) + timedelta(seconds=delay)).isoformat(sep=" ", timespec="seconds")


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
                "failed" if controls.circuit_state == "open" else "success", _seoul_timestamp(datetime.now(SEOUL)),
                summary.selected, summary.activated + summary.deactivated + summary.branch_changed + summary.unchanged,
                controls.retry_count, controls.call_count, controls.call_budget, controls.circuit_state, source_date,
            ),
        )
        conn.execute("DELETE FROM company_sync_response_metric WHERE job_name='company_revalidation' AND source_date=?", (source_date,))
        conn.executemany(
            "INSERT INTO company_sync_response_metric (job_name, source_date, response_class, response_count) VALUES (?, ?, ?, ?)",
            [("company_revalidation", source_date, key, value) for key, value in sorted(controls.response_classes.items())],
        )


def _reserve_request_slot(policy: RevalidationPolicy, controls: _RequestControls) -> _RequestOutcome | None:
    while True:
        with controls.lock:
            if controls.circuit_state == "open":
                controls.response_classes["circuit_open"] += 1
                return _RequestOutcome("deferred", "circuit_open", error_detail="circuit open")
            if controls.call_count >= controls.call_budget:
                controls.circuit_state = "budget_exhausted"
                controls.response_classes["budget_exhausted"] += 1
                return _RequestOutcome("deferred", "budget_exhausted", error_detail="call budget exhausted")
            now = policy.clock()
            wait_seconds = 0.0
            if policy.requests_per_second > 0 and controls.last_call_at is not None:
                wait_seconds = max((1 / policy.requests_per_second) - (now - controls.last_call_at), 0)
            if wait_seconds == 0:
                controls.call_count += 1
                controls.last_call_at = now
                return None
        policy.sleep(wait_seconds)


def _record_success(controls: _RequestControls, response_class: str) -> None:
    with controls.lock:
        controls.response_classes[response_class] += 1
        controls.consecutive_failures = 0


def _record_failure(controls: _RequestControls, response_class: str, policy: RevalidationPolicy) -> bool:
    with controls.lock:
        controls.response_classes[response_class] += 1
        controls.consecutive_failures += 1
        if controls.consecutive_failures >= policy.circuit_failure_threshold:
            controls.circuit_state = "open"
            return True
    return False


def _record_retry(controls: _RequestControls) -> None:
    with controls.lock:
        controls.retry_count += 1


def _request_item(
    source_client: CompanySourceClient, bizno: str, policy: RevalidationPolicy, controls: _RequestControls
) -> _RequestOutcome:
    for attempt in range(1, policy.max_attempts + 1):
        deferred = _reserve_request_slot(policy, controls)
        if deferred is not None:
            return deferred
        try:
            response = source_client.lookup(bizno)
            if response == []:
                _record_success(controls, "not_found")
                return _RequestOutcome("not_found", "not_found")
            if response is None:
                raise CompanyLookupError("unauthoritative empty response")
            if not isinstance(response, dict):
                raise CompanyLookupError("invalid supplier response schema")
            normalized = normalize_bizno(response.get("bizno", response.get("brno", response.get("businessNo"))))
            if normalized != bizno or any(not str(response.get(field, "")).strip() for field in REQUIRED_ITEM_FIELDS[1:]):
                raise CompanyLookupError("invalid supplier response schema")
            _record_success(controls, "success")
            return _RequestOutcome("success", "success", item=response)
        except CompanyLookupError as caught:
            detail, retry_after = str(caught), caught.retry_after
            response_class = "unauthoritative_empty" if detail == "unauthoritative empty response" else (
                "invalid_response" if "response" in detail else "lookup_error"
            )
        except Exception as caught:
            detail, retry_after, response_class = type(caught).__name__, None, "exception"
        circuit_open = _record_failure(controls, response_class, policy)
        if circuit_open or attempt == policy.max_attempts:
            return _RequestOutcome("failed", response_class, error_detail=detail, retry_after_seconds=retry_after)
        _record_retry(controls)
        policy.sleep(max(retry_after or 0, policy.backoff_seconds * (2 ** (attempt - 1)) + policy.random_uniform(0, policy.jitter_seconds)))
    raise AssertionError("unreachable")


def _claim_biznos(
    conn: sqlite3.Connection,
    biznos: Iterable[str],
    run_at: str,
    owner: str,
    policy: RevalidationPolicy,
) -> list[_Claim]:
    lease_expires_at = (datetime.fromisoformat(run_at) + timedelta(seconds=policy.lease_seconds)).isoformat(sep=" ", timespec="seconds")
    claimed: list[_Claim] = []
    with guarded_write_session(conn):
        for bizno in dict.fromkeys(biznos):
            conn.execute(
                "INSERT OR IGNORE INTO company_revalidation_queue (bizno, status, next_attempt_at) VALUES (?, 'pending', ?)",
                (bizno, run_at),
            )
            cursor = conn.execute(
                "UPDATE company_revalidation_queue SET attempt_count=attempt_count+1, last_attempt_at=?, "
                "lease_owner=?, lease_expires_at=? WHERE bizno=? AND "
                "(status='complete' OR (status IN ('pending','deferred_budget','failed') AND "
                "(next_attempt_at IS NULL OR next_attempt_at <= ?))) AND "
                "(lease_owner IS NULL OR lease_expires_at IS NULL OR lease_expires_at <= ?)",
                (run_at, owner, lease_expires_at, bizno, run_at, run_at),
            )
            if cursor.rowcount:
                attempt_count = conn.execute(
                    "SELECT attempt_count FROM company_revalidation_queue WHERE bizno=?", (bizno,)
                ).fetchone()[0]
                claimed.append(_Claim(bizno, owner, attempt_count))
    return claimed


def _mark_verified(conn: sqlite3.Connection, bizno: str, verified_at: str) -> None:
    conn.execute("UPDATE company_locality_status SET last_verified_at=? WHERE bizno=?", (verified_at, bizno))


def _persist_claim_outcome(
    conn: sqlite3.Connection, claim: _Claim, outcome: _RequestOutcome, run_at: str, policy: RevalidationPolicy
) -> None:
    if outcome.kind in {"success", "not_found"}:
        status, next_attempt, last_success = "complete", None, run_at
    elif outcome.kind == "deferred":
        status = "deferred_budget"
        next_attempt = _queue_retry_at(run_at, claim.attempt_count, outcome.retry_after_seconds, policy)
        last_success = None
    else:
        status = "failed"
        next_attempt = _queue_retry_at(run_at, claim.attempt_count, outcome.retry_after_seconds, policy)
        last_success = None
    cursor = conn.execute(
        "UPDATE company_revalidation_queue SET status=?, last_response_class=?, retry_after_seconds=?, "
        "next_attempt_at=?, last_attempt_at=?, last_success_at=COALESCE(?, last_success_at), error_detail=?, "
        "lease_owner=NULL, lease_expires_at=NULL WHERE bizno=? AND lease_owner=?",
        (status, outcome.response_class, outcome.retry_after_seconds, next_attempt, run_at, last_success,
         outcome.error_detail, claim.bizno, claim.owner),
    )
    if cursor.rowcount != 1:
        raise RuntimeError("revalidation claim was lost before completion")


def _record_job_observation(
    conn: sqlite3.Connection, source_date: str, outcome: _RequestOutcome, controls: _RequestControls
) -> None:
    conn.execute(
        "UPDATE company_sync_job_log SET call_count=?, retry_count=?, circuit_state=? "
        "WHERE job_name='company_revalidation' AND source_date=?",
        (controls.call_count, controls.retry_count, controls.circuit_state, source_date),
    )
    conn.execute(
        "INSERT INTO company_sync_response_metric (job_name, source_date, response_class, response_count) VALUES (?, ?, ?, 1) "
        "ON CONFLICT(job_name, source_date, response_class) DO UPDATE SET response_count=response_count+1",
        ("company_revalidation", source_date, outcome.response_class),
    )


def _apply_revalidation_result(
    conn: sqlite3.Connection,
    claim: _Claim,
    run_at: str,
    source_date: str,
    controls: _RequestControls,
    outcome: _RequestOutcome,
    policy: RevalidationPolicy,
) -> RevalidationSummary:
    with guarded_write_session(conn):
        before = conn.execute("SELECT status FROM company_locality_status WHERE bizno=?", (claim.bizno,)).fetchone()
        if outcome.kind == "success" and outcome.item is not None:
            apply_company_changes(
                conn, [outcome.item], run_at[:10].replace("-", ""), "company_revalidation:" + run_at[:10], run_at
            )
            after = conn.execute("SELECT status FROM company_locality_status WHERE bizno=?", (claim.bizno,)).fetchone()
            _mark_verified(conn, claim.bizno, run_at)
            _persist_claim_outcome(conn, claim, outcome, run_at, policy)
            _record_job_observation(conn, source_date, outcome, controls)
            before_status, after_status = (before[0] if before else "unverified"), (after[0] if after else "unverified")
            return RevalidationSummary(
                selected=1, activated=int(after_status == "active_local" and before_status != "active_local"),
                deactivated=int(after_status == "moved_out" and before_status != "moved_out"),
                branch_changed=int(after_status == "branch_changed" and before_status != "branch_changed"),
                unchanged=int(before_status == after_status), unverified=int(after_status == "unverified"),
            )
        if outcome.kind == "not_found" and before is not None:
            _mark_verified(conn, claim.bizno, run_at)
        _persist_claim_outcome(conn, claim, outcome, run_at, policy)
        _record_job_observation(conn, source_date, outcome, controls)
        return RevalidationSummary(
            selected=1, unchanged=int(outcome.kind == "not_found"),
            failed=int(outcome.kind == "failed"), deferred=int(outcome.kind == "deferred"),
        )


def _combine(summaries: Iterable[RevalidationSummary], controls: _RequestControls) -> RevalidationSummary:
    totals = {name: 0 for name in RevalidationSummary.__dataclass_fields__ if name not in {"calls", "retries", "circuit_state"}}
    for summary in summaries:
        for name in totals:
            totals[name] += getattr(summary, name)
    return RevalidationSummary(**totals, calls=controls.call_count, retries=controls.retry_count, circuit_state=controls.circuit_state)


def _run_biznos(
    conn: sqlite3.Connection, source_client: CompanySourceClient, biznos: Iterable[str], run_at: str,
    source_date: str, policy: RevalidationPolicy, controls: _RequestControls, workers: int, owner: str,
) -> RevalidationSummary:
    claims = _claim_biznos(conn, biznos, run_at, owner, policy)
    with ThreadPoolExecutor(max_workers=min(max(workers, 1), 8)) as executor:
        outcomes = list(executor.map(lambda claim: (claim, _request_item(source_client, claim.bizno, policy, controls)), claims))
    return _combine(
        (_apply_revalidation_result(conn, claim, run_at, source_date, controls, outcome, policy) for claim, outcome in outcomes),
        controls,
    )


def _due_biznos(conn: sqlite3.Connection, run_at: str) -> list[str]:
    return [
        row[0] for row in conn.execute(
            "SELECT bizno FROM company_revalidation_queue WHERE status IN ('pending','deferred_budget','failed') "
            "AND (next_attempt_at IS NULL OR next_attempt_at <= ?) ORDER BY next_attempt_at, bizno", (run_at,)
        )
    ]


def drain_revalidation_queue(
    conn: sqlite3.Connection, source_client: CompanySourceClient, run_at: date | datetime | str, request_budget: int,
    *, policy: RevalidationPolicy | None = None,
) -> RevalidationSummary:
    if request_budget < 1:
        raise ValueError("request_budget must be positive")
    ensure_locality_schema(conn)
    timestamp = _seoul_timestamp(run_at)
    active_policy = policy or RevalidationPolicy(daily_call_budget=request_budget)
    controls = _RequestControls(request_budget)
    source_date = _start_job(conn, timestamp, controls)
    summary = _run_biznos(conn, source_client, _due_biznos(conn, timestamp), timestamp, source_date, active_policy, controls, 8, uuid4().hex)
    _finish_job(conn, source_date, summary, controls)
    return summary


def revalidate_bucket(
    conn: sqlite3.Connection, source_client: CompanySourceClient, run_date: date | datetime | str, bucket_count: int = 30,
    workers: int = 8, *, policy: RevalidationPolicy | None = None,
) -> RevalidationSummary:
    if workers < 1 or bucket_count < 1:
        raise ValueError("workers and bucket_count must be positive")
    ensure_locality_schema(conn)
    timestamp = _seoul_timestamp(run_date)
    active_policy = policy or RevalidationPolicy()
    controls = _RequestControls(active_policy.daily_call_budget)
    source_date, owner = _start_job(conn, timestamp, controls), uuid4().hex
    due = _due_biznos(conn, timestamp)
    bucket = datetime.fromisoformat(timestamp).date().toordinal() % bucket_count
    bucket_biznos = [bizno for bizno in biznos_for_bucket(conn, bucket, bucket_count) if bizno not in set(due)]
    summary = _combine(
        (
            _run_biznos(conn, source_client, due, timestamp, source_date, active_policy, controls, min(workers, 8), owner),
            _run_biznos(conn, source_client, bucket_biznos, timestamp, source_date, active_policy, controls, min(workers, 8), owner),
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
    if args.apply and (not args.api_url or not args.service_key):
        parser.error("--apply requires --api-url and --service-key or their environment variables")
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
        summary = revalidate_bucket(
            conn, make_verified_company_lookup_client(args.api_url, args.service_key), args.date, args.bucket_count
        )
        print(f"selected={summary.selected} calls={summary.calls} failed={summary.failed} deferred={summary.deferred}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
