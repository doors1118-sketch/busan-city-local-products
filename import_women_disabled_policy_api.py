"""
Women-owned and disabled-owned company certificate incremental sync.

Source:
- Korea SMEs and Startups Distribution Agency SMPP certificate API
- https://apis.data.go.kr/B550598/smppCertInfo

This job intentionally excludes social enterprises because the currently
verified social-enterprise API does not expose business registration numbers.
"""
import argparse
import datetime
import hashlib
import json
import logging
import os
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET

import requests


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("WomenDisabledPolicyAPI")

DB_FILE = os.environ.get("CHATBOT_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "chatbot_company.db"))
SMPP_SERVICE_KEY = (
    os.environ.get("SMPP_CERT_INFO_SERVICE_KEY")
    or os.environ.get("SMPP_SERVICE_KEY")
    or os.environ.get("SERVICE_KEY")
)
SMPP_BASE_URL = os.environ.get("SMPP_CERT_INFO_BASE_URL", "https://apis.data.go.kr/B550598/smppCertInfo")
SOURCE_NAME = "smpp_women_disabled_api_incremental"
DEFAULT_MAX_COMPANIES = int(os.environ.get("SMPP_POLICY_MAX_COMPANIES", "200"))
DEFAULT_SLEEP_SECONDS = float(os.environ.get("SMPP_POLICY_SLEEP_SECONDS", "0.35"))
DEFAULT_RECHECK_DAYS = int(os.environ.get("SMPP_POLICY_RECHECK_DAYS", "120"))

OPERATIONS = (
    ("women_company", "/getFnrssList", "03"),
    ("disabled_company", "/getDspsnList", "04"),
)


class SmppRateLimitError(Exception):
    pass


def hash_string(value: str) -> str:
    if not value:
        return ""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def normalize_business_no(value: object) -> str:
    raw = "" if value is None else str(value).strip()
    raw = raw.replace(".0", "")
    digits = "".join(ch for ch in raw if ch.isdigit())
    if digits and len(digits) < 10:
        digits = digits.zfill(10)
    return digits


def normalize_date(value: object) -> str:
    raw = "" if value is None else str(value).strip()
    if not raw:
        return ""
    raw = raw.replace(".", "-").replace("/", "-")
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        return raw[:10]
    return raw


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS smpp_policy_cert_refresh_queue (
            queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_internal_id INTEGER NOT NULL,
            canonical_business_no TEXT NOT NULL,
            policy_subtype TEXT NOT NULL,
            reason TEXT,
            priority INTEGER DEFAULT 50,
            status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            last_error TEXT,
            last_checked_at DATETIME,
            next_retry_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(company_internal_id, policy_subtype)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_smpp_policy_cert_queue_status
        ON smpp_policy_cert_refresh_queue(status, next_retry_at, priority)
        """
    )
    conn.commit()


def enqueue_company(
    conn: sqlite3.Connection,
    company_internal_id: int,
    business_no: str,
    policy_subtype: str,
    reason: str,
    priority: int = 50,
) -> None:
    conn.execute(
        """
        INSERT INTO smpp_policy_cert_refresh_queue (
            company_internal_id, canonical_business_no, policy_subtype,
            reason, priority, status, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP)
        ON CONFLICT(company_internal_id, policy_subtype) DO UPDATE SET
            canonical_business_no=excluded.canonical_business_no,
            reason=excluded.reason,
            priority=MIN(smpp_policy_cert_refresh_queue.priority, excluded.priority),
            status=CASE
                WHEN smpp_policy_cert_refresh_queue.status IN ('done', 'failed', 'rate_limited')
                THEN 'pending'
                ELSE smpp_policy_cert_refresh_queue.status
            END,
            updated_at=CURRENT_TIMESTAMP
        """,
        (company_internal_id, business_no, policy_subtype, reason, priority),
    )


def build_candidate_rows(conn: sqlite3.Connection, max_companies: int, full_scan: bool = False) -> list[sqlite3.Row]:
    ensure_schema(conn)
    if full_scan:
        return conn.execute(
            """
            SELECT m.company_internal_id, i.canonical_business_no, m.company_name
            FROM company_master m
            JOIN company_identity i ON m.company_internal_id = i.company_internal_id
            WHERE m.is_busan_company = 1
              AND i.canonical_business_no IS NOT NULL
            ORDER BY m.company_internal_id
            LIMIT ?
            """,
            (max_companies,),
        ).fetchall()

    rows = conn.execute(
        """
        SELECT DISTINCT m.company_internal_id, i.canonical_business_no, m.company_name
        FROM smpp_policy_cert_refresh_queue q
        JOIN company_master m ON q.company_internal_id = m.company_internal_id
        JOIN company_identity i ON m.company_internal_id = i.company_internal_id
        WHERE q.status IN ('pending', 'failed', 'rate_limited')
          AND (q.next_retry_at IS NULL OR q.next_retry_at <= datetime('now'))
          AND i.canonical_business_no IS NOT NULL
        ORDER BY q.priority ASC, q.updated_at ASC
        LIMIT ?
        """,
        (max_companies,),
    ).fetchall()

    seen = {row["company_internal_id"] for row in rows}
    remaining = max_companies - len(rows)

    if remaining > 0:
        # Recheck certified companies whose current source is old or whose valid_to is near.
        expiring = conn.execute(
            """
            SELECT DISTINCT m.company_internal_id, i.canonical_business_no, m.company_name
            FROM policy_company_certification p
            JOIN company_master m ON p.company_internal_id = m.company_internal_id
            JOIN company_identity i ON m.company_internal_id = i.company_internal_id
            LEFT JOIN smpp_policy_cert_refresh_queue q
              ON q.company_internal_id = m.company_internal_id
             AND q.policy_subtype = p.policy_subtype
            WHERE p.policy_subtype IN ('women_company', 'disabled_company')
              AND p.validity_status = 'valid'
              AND i.canonical_business_no IS NOT NULL
              AND (
                    p.certification_valid_to IS NULL
                 OR p.certification_valid_to = ''
                 OR p.certification_valid_to <= date('now', '+90 day')
                 OR q.last_checked_at IS NULL
                 OR q.last_checked_at < datetime('now', '-' || ? || ' day')
              )
            ORDER BY COALESCE(p.certification_valid_to, '1900-01-01') ASC, m.company_internal_id ASC
            LIMIT ?
            """,
            (DEFAULT_RECHECK_DAYS, remaining),
        ).fetchall()
        for row in expiring:
            if row["company_internal_id"] not in seen:
                rows.append(row)
                seen.add(row["company_internal_id"])
        remaining = max_companies - len(rows)

    if remaining > 0:
        # Cover new/changed or never-checked Busan companies gradually.
        cycle = conn.execute(
            """
            SELECT m.company_internal_id, i.canonical_business_no, m.company_name
            FROM company_master m
            JOIN company_identity i ON m.company_internal_id = i.company_internal_id
            LEFT JOIN (
                SELECT company_internal_id, MIN(last_checked_at) AS last_checked_at
                FROM smpp_policy_cert_refresh_queue
                GROUP BY company_internal_id
            ) q ON q.company_internal_id = m.company_internal_id
            WHERE m.is_busan_company = 1
              AND i.canonical_business_no IS NOT NULL
              AND (q.last_checked_at IS NULL OR q.last_checked_at < datetime('now', '-' || ? || ' day'))
            ORDER BY COALESCE(m.source_refreshed_at, m.updated_at, m.created_at, '1900-01-01') DESC,
                     m.company_internal_id ASC
            LIMIT ?
            """,
            (DEFAULT_RECHECK_DAYS, remaining),
        ).fetchall()
        for row in cycle:
            if row["company_internal_id"] not in seen:
                rows.append(row)
                seen.add(row["company_internal_id"])

    return rows


def parse_cert_items(xml_bytes: bytes, policy_subtype: str) -> list[dict]:
    root = ET.fromstring(xml_bytes)
    result_code = root.findtext(".//resultCode") or ""
    if result_code == "90":
        return []
    if result_code not in ("", "00"):
        result_msg = root.findtext(".//resultMsg") or ""
        raise RuntimeError(f"SMPP resultCode={result_code} resultMsg={result_msg}")

    items = []
    for item in root.findall(".//item"):
        valid_from = normalize_date(item.findtext("validPdBeginDe") or "")
        valid_to = normalize_date(item.findtext("validPdEndDe") or "")
        cert_date = normalize_date(item.findtext("certfcDe") or "")
        cert_se_code = item.findtext("certSeCode") or ""
        cert_no = item.findtext("certfcNo") or item.findtext("issuNo") or f"{policy_subtype}_{cert_se_code}_{cert_date}_{valid_from}_{valid_to}"
        items.append(
            {
                "policy_subtype": policy_subtype,
                "cert_no": cert_no,
                "valid_from": valid_from,
                "valid_to": valid_to,
                "issuer": item.findtext("issuInstt") or "",
            }
        )
    return items


def fetch_policy_cert(business_no: str, policy_subtype: str, op_path: str, service_key: str) -> list[dict]:
    params = {
        "ServiceKey": service_key,
        "bsnmNo": business_no,
        "stdrDate": datetime.date.today().strftime("%Y%m%d"),
        "numOfRows": 100,
        "pageNo": 1,
    }
    response = requests.get(f"{SMPP_BASE_URL}{op_path}", params=params, timeout=15)
    if response.status_code == 429:
        raise SmppRateLimitError(f"{policy_subtype} HTTP 429")
    if response.status_code != 200:
        raise RuntimeError(f"{policy_subtype} HTTP {response.status_code}: {response.text[:300]}")
    return parse_cert_items(response.content, policy_subtype)


def upsert_cert(conn: sqlite3.Connection, company_internal_id: int, business_no: str, item: dict, now: str) -> int:
    today = datetime.date.today().isoformat()
    valid_to = item["valid_to"]
    validity = "expired" if valid_to and valid_to < today else "valid"
    cert_hash = hash_string(f"{item['policy_subtype']}|{business_no}|{item['cert_no']}|{item['valid_from']}|{item['valid_to']}")
    conn.execute(
        """
        INSERT INTO policy_company_certification (
            company_internal_id, policy_type, policy_subtype, certification_no_hash,
            certification_valid_from, certification_valid_to, validity_status,
            issuer, source_name, source_refreshed_at
        ) VALUES (?, 'policy_company', ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(company_internal_id, policy_subtype, source_name, certification_no_hash)
        DO UPDATE SET
            certification_valid_from=excluded.certification_valid_from,
            certification_valid_to=excluded.certification_valid_to,
            validity_status=excluded.validity_status,
            issuer=excluded.issuer,
            source_refreshed_at=excluded.source_refreshed_at,
            updated_at=CURRENT_TIMESTAMP
        """,
        (
            company_internal_id,
            item["policy_subtype"],
            cert_hash,
            item["valid_from"],
            item["valid_to"],
            validity,
            item["issuer"],
            SOURCE_NAME,
            now,
        ),
    )
    return 1


def insert_job_log(
    conn: sqlite3.Connection,
    started_at: str,
    status: str,
    input_count: int,
    inserted_count: int,
    skipped_count: int,
    error_count: int,
    error_message: str | None,
) -> None:
    finished_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """
        INSERT INTO etl_job_log (
            job_name, source_name, started_at, finished_at, status,
            input_row_count, inserted_count, skipped_count, error_count, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "import_women_disabled_policy_api",
            SOURCE_NAME,
            started_at,
            finished_at,
            status,
            input_count,
            inserted_count,
            skipped_count,
            error_count,
            error_message,
        ),
    )
    conn.execute(
        """
        INSERT INTO source_manifest (
            source_name, source_type, source_url_or_file,
            row_count, source_refreshed_at, status, error_message
        ) VALUES (?, 'api_incremental', ?, ?, ?, ?, ?)
        ON CONFLICT(source_name) DO UPDATE SET
            source_url_or_file=excluded.source_url_or_file,
            row_count=excluded.row_count,
            source_refreshed_at=excluded.source_refreshed_at,
            status=excluded.status,
            error_message=excluded.error_message,
            updated_at=CURRENT_TIMESTAMP
        """,
        (SOURCE_NAME, SMPP_BASE_URL, inserted_count, finished_at, status, error_message),
    )


def run_import(
    *,
    dry_run: bool = False,
    max_companies: int = DEFAULT_MAX_COMPANIES,
    full_scan: bool = False,
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS,
) -> bool:
    started_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    now = started_at
    stats = {"http_429": 0, "request_error": 0, "api_error": 0}

    conn = sqlite3.connect(DB_FILE, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)

    if not SMPP_SERVICE_KEY:
        msg = "SMPP_CERT_INFO_SERVICE_KEY/SMPP_SERVICE_KEY/SERVICE_KEY is not configured"
        logger.warning(msg)
        if not dry_run:
            insert_job_log(conn, started_at, "skipped", 0, 0, 0, 1, msg)
            conn.commit()
        conn.close()
        return True

    rows = build_candidate_rows(conn, max_companies=max_companies, full_scan=full_scan)
    processed = 0
    inserted = 0
    skipped = 0
    rate_limited = False

    for row in rows:
        processed += 1
        business_no = normalize_business_no(row["canonical_business_no"])
        if not business_no:
            skipped += 1
            continue

        for policy_subtype, op_path, _cert_code in OPERATIONS:
            try:
                items = fetch_policy_cert(business_no, policy_subtype, op_path, SMPP_SERVICE_KEY)
            except SmppRateLimitError as exc:
                stats["http_429"] += 1
                rate_limited = True
                if not dry_run:
                    enqueue_company(conn, row["company_internal_id"], business_no, policy_subtype, "rate_limited", priority=1)
                    conn.execute(
                        """
                        UPDATE smpp_policy_cert_refresh_queue
                        SET status='rate_limited',
                            attempts=attempts+1,
                            last_error=?,
                            next_retry_at=datetime('now', '+1 day'),
                            updated_at=CURRENT_TIMESTAMP
                        WHERE company_internal_id=? AND policy_subtype=?
                        """,
                        (str(exc), row["company_internal_id"], policy_subtype),
                    )
                    conn.commit()
                logger.warning("SMPP rate limit reached; stopping this run.")
                break
            except Exception as exc:
                stats["api_error"] += 1
                if not dry_run:
                    enqueue_company(conn, row["company_internal_id"], business_no, policy_subtype, "api_error", priority=10)
                    conn.execute(
                        """
                        UPDATE smpp_policy_cert_refresh_queue
                        SET status='failed',
                            attempts=attempts+1,
                            last_error=?,
                            next_retry_at=datetime('now', '+1 day'),
                            updated_at=CURRENT_TIMESTAMP
                        WHERE company_internal_id=? AND policy_subtype=?
                        """,
                        (str(exc)[:500], row["company_internal_id"], policy_subtype),
                    )
                continue

            if not dry_run:
                enqueue_company(conn, row["company_internal_id"], business_no, policy_subtype, "cycle_check", priority=80)
                conn.execute(
                    """
                    UPDATE smpp_policy_cert_refresh_queue
                    SET status='done',
                        attempts=attempts+1,
                        last_error=NULL,
                        last_checked_at=?,
                        next_retry_at=NULL,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE company_internal_id=? AND policy_subtype=?
                    """,
                    (now, row["company_internal_id"], policy_subtype),
                )

            if not items:
                skipped += 1
            for item in items:
                if dry_run:
                    inserted += 1
                else:
                    inserted += upsert_cert(conn, row["company_internal_id"], business_no, item, now)

            time.sleep(sleep_seconds)

        if rate_limited:
            break

    status = "rate_limited" if rate_limited else "success"
    error_message = json.dumps(stats, ensure_ascii=False)
    if not dry_run:
        insert_job_log(conn, started_at, status, processed, inserted, skipped, stats["http_429"] + stats["api_error"], error_message)
        conn.commit()
    conn.close()

    logger.info(
        "Import finished. processed_companies=%s inserted=%s skipped=%s dry_run=%s status=%s stats=%s",
        processed,
        inserted,
        skipped,
        dry_run,
        status,
        stats,
    )
    return not rate_limited


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SMPP women/disabled company certificate incremental sync")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--full-scan", action="store_true")
    parser.add_argument("--max-companies", type=int, default=DEFAULT_MAX_COMPANIES)
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS)
    args = parser.parse_args()
    ok = run_import(
        dry_run=args.dry_run,
        max_companies=args.max_companies,
        full_scan=args.full_scan,
        sleep_seconds=args.sleep_seconds,
    )
    if not ok:
        sys.exit(1)
