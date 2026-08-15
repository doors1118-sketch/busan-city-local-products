"""
직접생산확인증명서 발급현황 API 연동 모듈
- 출처: 중소벤처기업부 공공데이터포털/ODCloud
- API: https://api.odcloud.kr/api/3061008/v1/uddi:98e7ddf2-e720-43af-b2eb-b92f4f65b0b5
- 매칭 키: 사업자등록번호

주의:
- 이 스크립트는 런타임 질의용이 아니라 chatbot_company.db 배치 적재용이다.
- API 키는 DIRECT_PRODUCTION_SERVICE_KEY, ODCLOUD_DIRECT_PRODUCTION_SERVICE_KEY,
  ODCLOUD_API_KEY, SERVICE_KEY 순서로 로드한다.
"""
import argparse
import datetime
import hashlib
import logging
import os
import re
import sqlite3
import sys
import time
from typing import Iterable

import requests


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("DirectProductionCertAPI")

DB_FILE = os.environ.get("CHATBOT_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "chatbot_company.db"))
DIRECT_PRODUCTION_SERVICE_KEY = (
    os.environ.get("DIRECT_PRODUCTION_SERVICE_KEY")
    or os.environ.get("ODCLOUD_DIRECT_PRODUCTION_SERVICE_KEY")
    or os.environ.get("ODCLOUD_API_KEY")
    or os.environ.get("SERVICE_KEY")
)

SOURCE_NAME = "odcloud_direct_production_cert_api_3061008_20260201"
API_BASE_URL = os.environ.get(
    "DIRECT_PRODUCTION_API_URL",
    "https://api.odcloud.kr/api/3061008/v1/uddi:98e7ddf2-e720-43af-b2eb-b92f4f65b0b5",
)
DEFAULT_PER_PAGE = int(os.environ.get("DIRECT_PRODUCTION_PER_PAGE", "5000"))
DEFAULT_MAX_PAGES = int(os.environ.get("DIRECT_PRODUCTION_MAX_PAGES", "1000"))
RETRY_STATUS_CODES = {429, 502, 503, 504}
RETRY_DELAYS = [5, 15, 30]


def hash_string(value: str) -> str:
    if not value:
        return ""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def normalize_business_no(value: object) -> str:
    raw = "" if value is None else str(value).strip()
    raw = raw.replace(".0", "")
    digits = re.sub(r"\D", "", raw)
    if digits and len(digits) < 10:
        digits = digits.zfill(10)
    return digits


def normalize_name(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", "", str(value).strip()).lower()


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


def resolve_validity(valid_to: str, today: str) -> str:
    if not valid_to:
        return "unknown"
    return "expired" if valid_to < today else "valid"


def ensure_schema(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_direct_production_cert_import (
            raw_direct_production_cert_import_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT NOT NULL,
            source_row_no INTEGER,
            source_collected_at DATETIME,
            raw_company_name TEXT,
            raw_business_no_hash TEXT,
            raw_detail_product_name TEXT,
            raw_valid_from TEXT,
            raw_valid_to TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS direct_production_certificate (
            direct_production_cert_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_internal_id INTEGER NOT NULL,
            detail_product_name TEXT NOT NULL,
            detail_product_name_normalized TEXT NOT NULL DEFAULT '',
            detail_product_code TEXT NOT NULL DEFAULT '',
            valid_from DATE,
            valid_to DATE,
            validity_status TEXT NOT NULL DEFAULT 'unknown',
            source_name TEXT NOT NULL,
            source_refreshed_at DATETIME,
            match_method TEXT,
            raw_business_no_hash TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(company_internal_id) REFERENCES company_master(company_internal_id)
        )
        """
    )
    cursor.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_direct_production_cert_unique
        ON direct_production_certificate(
            company_internal_id,
            detail_product_name_normalized,
            valid_from,
            valid_to,
            source_name
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_direct_production_cert_company
        ON direct_production_certificate(company_internal_id)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_direct_production_cert_product
        ON direct_production_certificate(detail_product_name_normalized)
        """
    )
    conn.commit()


def insert_job_log(
    conn: sqlite3.Connection,
    *,
    started_at: str,
    status: str,
    input_count: int = 0,
    inserted_count: int = 0,
    skipped_count: int = 0,
    error_count: int = 0,
    error_message: str | None = None,
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
            "import_direct_production_cert_api",
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
        ) VALUES (?, 'api_full', ?, ?, ?, ?, ?)
        ON CONFLICT(source_name) DO UPDATE SET
            source_url_or_file=excluded.source_url_or_file,
            row_count=excluded.row_count,
            source_refreshed_at=excluded.source_refreshed_at,
            status=excluded.status,
            error_message=excluded.error_message,
            updated_at=CURRENT_TIMESTAMP
        """,
        (SOURCE_NAME, API_BASE_URL, inserted_count, finished_at, status, error_message),
    )
    conn.commit()


def fetch_page(params: dict, page: int) -> requests.Response:
    attempts = len(RETRY_DELAYS) + 1
    last_error = ""
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(API_BASE_URL, params=params, timeout=45)
            if response.status_code == 200:
                return response
            last_error = f"HTTP {response.status_code}: {response.text[:200]}"
            if response.status_code == 401:
                raise RuntimeError("API 인증 실패(401). DIRECT_PRODUCTION_SERVICE_KEY를 확인하세요.")
            if response.status_code not in RETRY_STATUS_CODES:
                raise RuntimeError(last_error)
        except requests.RequestException as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        if attempt < attempts:
            delay = RETRY_DELAYS[attempt - 1]
            logger.warning("API page %s retry %s/%s after %ss: %s", page, attempt, attempts - 1, delay, last_error)
            time.sleep(delay)
    raise RuntimeError(f"API page {page} retry exhausted: {last_error}")


def fetch_all_pages(service_key: str, per_page: int = DEFAULT_PER_PAGE, max_pages: int = DEFAULT_MAX_PAGES) -> list[dict]:
    all_items: list[dict] = []
    page = 1

    while page <= max_pages:
        params = {
            "serviceKey": service_key,
            "page": page,
            "perPage": per_page,
            "returnType": "JSON",
        }
        logger.info("Fetching page %s (perPage=%s)...", page, per_page)
        response = fetch_page(params, page)

        payload = response.json()
        total_count = int(payload.get("totalCount") or 0)
        current_count = int(payload.get("currentCount") or 0)
        items = payload.get("data") or []
        if not items:
            break

        all_items.extend(items)
        logger.info("  Page %s: %s items (total so far: %s/%s)", page, current_count, len(all_items), total_count)

        if total_count and len(all_items) >= total_count:
            break
        page += 1
        time.sleep(0.2)

    logger.info("Total fetched: %s items", len(all_items))
    return all_items


def build_busan_business_no_map(cursor: sqlite3.Cursor) -> dict[str, int]:
    rows = cursor.execute(
        """
        SELECT m.company_internal_id, i.canonical_business_no
        FROM company_master m
        JOIN company_identity i ON m.company_internal_id = i.company_internal_id
        WHERE m.is_busan_company = 1
        """
    ).fetchall()
    return {
        row["canonical_business_no"]: row["company_internal_id"]
        for row in rows
        if row["canonical_business_no"]
    }


def iter_normalized_items(items: Iterable[dict], today: str):
    for idx, item in enumerate(items, 1):
        bno = normalize_business_no(item.get("사업자번호") or item.get("사업자등록번호"))
        detail_product_name = str(item.get("세부품명", "") or "").strip()
        company_name = str(item.get("업체명", "") or "").strip()
        valid_from = normalize_date(item.get("유효기간 시작일") or item.get("유효기간시작일"))
        valid_to = normalize_date(item.get("유효기간 종료일") or item.get("유효기간종료일"))
        if not bno or not detail_product_name:
            yield idx, None
            continue
        yield idx, {
            "business_no": bno,
            "business_no_hash": hash_string(bno),
            "company_name": company_name,
            "detail_product_name": detail_product_name,
            "detail_product_name_normalized": normalize_name(detail_product_name),
            "valid_from": valid_from,
            "valid_to": valid_to,
            "validity_status": resolve_validity(valid_to, today),
        }


def run_import(dry_run: bool = False, probe: bool = False) -> bool:
    started_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    now_str = started_at

    conn = sqlite3.connect(DB_FILE, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)

    if not DIRECT_PRODUCTION_SERVICE_KEY:
        msg = "DIRECT_PRODUCTION_SERVICE_KEY/ODCLOUD_DIRECT_PRODUCTION_SERVICE_KEY/ODCLOUD_API_KEY 미설정"
        logger.warning("%s. DB 구조만 유지하고 적재는 건너뜁니다.", msg)
        if not dry_run:
            insert_job_log(conn, started_at=started_at, status="skipped", error_message=msg)
        conn.close()
        return True

    try:
        items = fetch_all_pages(
            DIRECT_PRODUCTION_SERVICE_KEY,
            per_page=10 if probe else DEFAULT_PER_PAGE,
            max_pages=1 if probe else DEFAULT_MAX_PAGES,
        )
    except Exception as exc:
        logger.error("API 호출 실패: %s", exc)
        if not dry_run:
            insert_job_log(
                conn,
                started_at=started_at,
                status="failed",
                error_count=1,
                error_message=str(exc),
            )
        conn.close()
        return False

    cursor = conn.cursor()
    bno_map = build_busan_business_no_map(cursor)
    today = datetime.date.today().isoformat()

    total_count = len(items)
    matched_count = 0
    skipped_count = 0

    if not dry_run and not probe:
        cursor.execute("DELETE FROM direct_production_certificate WHERE source_name = ?", (SOURCE_NAME,))
        cursor.execute("DELETE FROM raw_direct_production_cert_import WHERE source_name = ?", (SOURCE_NAME,))

    for idx, normalized in iter_normalized_items(items, today):
        if not normalized:
            skipped_count += 1
            continue

        internal_id = bno_map.get(normalized["business_no"])
        if not internal_id:
            skipped_count += 1
            continue

        matched_count += 1
        if dry_run:
            continue

        cursor.execute(
            """
            INSERT INTO raw_direct_production_cert_import (
                source_name, source_row_no, source_collected_at,
                raw_company_name, raw_business_no_hash, raw_detail_product_name,
                raw_valid_from, raw_valid_to
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                SOURCE_NAME,
                idx,
                now_str,
                normalized["company_name"],
                normalized["business_no_hash"],
                normalized["detail_product_name"],
                normalized["valid_from"],
                normalized["valid_to"],
            ),
        )
        if probe:
            continue

        cursor.execute(
            """
            INSERT INTO direct_production_certificate (
                company_internal_id, detail_product_name, detail_product_name_normalized,
                detail_product_code, valid_from, valid_to, validity_status,
                source_name, source_refreshed_at, match_method, raw_business_no_hash
            ) VALUES (?, ?, ?, '', ?, ?, ?, ?, ?, 'exact_bno', ?)
            ON CONFLICT(
                company_internal_id,
                detail_product_name_normalized,
                valid_from,
                valid_to,
                source_name
            ) DO UPDATE SET
                detail_product_name=excluded.detail_product_name,
                validity_status=excluded.validity_status,
                source_refreshed_at=excluded.source_refreshed_at,
                match_method=excluded.match_method,
                raw_business_no_hash=excluded.raw_business_no_hash,
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                internal_id,
                normalized["detail_product_name"],
                normalized["detail_product_name_normalized"],
                normalized["valid_from"],
                normalized["valid_to"],
                normalized["validity_status"],
                SOURCE_NAME,
                now_str,
                normalized["business_no_hash"],
            ),
        )

    if not dry_run:
        recorded_count = matched_count
        if not probe:
            row = cursor.execute(
                "SELECT COUNT(*) AS cnt FROM direct_production_certificate WHERE source_name = ?",
                (SOURCE_NAME,),
            ).fetchone()
            recorded_count = int(row["cnt"] if row else 0)
        insert_job_log(
            conn,
            started_at=started_at,
            status="success" if not probe else "probe_success",
            input_count=total_count,
            inserted_count=recorded_count,
            skipped_count=skipped_count,
        )

    conn.close()
    logger.info(
        "Import finished. API total=%s, Busan matched=%s, skipped=%s, dry_run=%s, probe=%s",
        total_count,
        matched_count,
        skipped_count,
        dry_run,
        probe,
    )
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="직접생산확인증명서 API → chatbot_company.db 적재")
    parser.add_argument("--dry-run", action="store_true", help="DB 기록 없이 매칭 통계만 확인")
    parser.add_argument("--probe", action="store_true", help="1페이지 10건만 호출하여 API 통신 확인")
    args = parser.parse_args()

    ok = run_import(dry_run=args.dry_run, probe=args.probe)
    if not ok:
        sys.exit(1)
