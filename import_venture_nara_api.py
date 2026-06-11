from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import re
import sqlite3
import time
from typing import Any

import requests


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("VentureNaraAPI")

DB_FILE = os.environ.get("CHATBOT_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "chatbot_company.db"))
SERVICE_KEY = (
    os.environ.get("VENTURE_NARA_SERVICE_KEY")
    or os.environ.get("ODCLOUD_VENTURE_NARA_SERVICE_KEY")
    or os.environ.get("ODCLOUD_API_KEY")
    or os.environ.get("SERVICE_KEY")
)

PRODUCT_API_URL = os.environ.get(
    "VENTURE_NARA_PRODUCT_API_URL",
    "https://api.odcloud.kr/api/15127733/v1/uddi:4d326451-9f87-4727-a6e8-b83afcffc021",
)
DESIGNATED_API_URL = os.environ.get(
    "VENTURE_NARA_DESIGNATED_API_URL",
    "https://api.odcloud.kr/api/15131213/v1/uddi:5d678896-c435-4ac9-b67f-a319fab61f33",
)
PRODUCT_SOURCE_NAME = "odcloud_venture_nara_product_api_15127733_20240331"
DESIGNATED_SOURCE_NAME = "odcloud_venture_nara_designated_company_api_15131213_20240806"

DEFAULT_PER_PAGE = int(os.environ.get("VENTURE_NARA_PER_PAGE", "1000"))
DEFAULT_MAX_PAGES = int(os.environ.get("VENTURE_NARA_MAX_PAGES", "1000"))
RETRY_DELAYS = [3, 10, 20]


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_business_no(value: Any) -> str:
    raw = clean_text(value).replace(".0", "")
    digits = re.sub(r"\D+", "", raw)
    if digits and len(digits) < 10:
        digits = digits.zfill(10)
    return digits


def normalize_bool(value: Any) -> int:
    raw = clean_text(value).lower()
    return 1 if raw in {"y", "yes", "1", "true", "대상", "해당", "벤처기업"} else 0


def normalize_int(value: Any) -> int:
    digits = re.sub(r"[^0-9-]+", "", clean_text(value))
    try:
        return int(digits) if digits else 0
    except ValueError:
        return 0


def now_text() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS venture_nara_product (
            venture_product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_identifier TEXT,
            venture_product_name TEXT,
            bizno TEXT,
            company_internal_id INTEGER,
            company_name TEXT,
            category_name TEXT,
            parent_category_name TEXT,
            price_amount INTEGER,
            price_unit TEXT,
            spec TEXT,
            origin_country TEXT,
            description TEXT,
            delivery_condition TEXT,
            is_sme_competition_product INTEGER,
            venture_company_flag INTEGER,
            is_oem INTEGER,
            valid_from TEXT,
            valid_to TEXT,
            company_cert_list TEXT,
            mandatory_purchase_cert_list TEXT,
            preferential_purchase_cert_list TEXT,
            venture_nara_cert_list TEXT,
            source_name TEXT NOT NULL,
            source_refreshed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_name, product_identifier, bizno)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_venture_nara_product_bizno
        ON venture_nara_product(bizno)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_venture_nara_product_company
        ON venture_nara_product(company_internal_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_venture_nara_product_name
        ON venture_nara_product(venture_product_name)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS venture_nara_designated_company (
            designated_id INTEGER PRIMARY KEY AUTOINCREMENT,
            bizno TEXT NOT NULL,
            company_internal_id INTEGER,
            designated_year INTEGER,
            designated_round INTEGER,
            designated_seq INTEGER,
            product_names TEXT,
            region_name TEXT,
            pps_branch_name TEXT,
            phone_numbers TEXT,
            fax_numbers TEXT,
            confirmed_yn TEXT,
            confirmed_date TEXT,
            input_date TEXT,
            source_name TEXT NOT NULL,
            source_refreshed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_name, bizno, designated_year, designated_round, designated_seq)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_venture_nara_designated_bizno
        ON venture_nara_designated_company(bizno)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_venture_nara_designated_company
        ON venture_nara_designated_company(company_internal_id)
        """
    )
    conn.commit()


def company_id_by_bizno(conn: sqlite3.Connection) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for bizno, company_internal_id in conn.execute(
        """
        SELECT canonical_business_no, company_internal_id
        FROM company_identity
        WHERE IFNULL(canonical_business_no, '') <> ''
          AND company_internal_id IS NOT NULL
        """
    ):
        mapping[str(bizno)] = int(company_internal_id)
    return mapping


def log_etl(
    conn: sqlite3.Connection,
    *,
    job_name: str,
    source_name: str,
    source_url: str,
    started_at: str,
    status: str,
    input_count: int,
    inserted_count: int,
    skipped_count: int = 0,
    error_count: int = 0,
    error_message: str = "",
) -> None:
    finished_at = now_text()
    conn.execute(
        """
        INSERT INTO etl_job_log (
            job_name, source_name, started_at, finished_at, status,
            input_row_count, inserted_count, skipped_count, error_count, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (job_name, source_name, started_at, finished_at, status, input_count, inserted_count, skipped_count, error_count, error_message),
    )
    conn.execute(
        """
        INSERT INTO source_manifest (
            source_name, source_type, source_url_or_file,
            row_count, source_refreshed_at, status, error_message
        ) VALUES (?, 'api_full_refresh', ?, ?, ?, ?, ?)
        ON CONFLICT(source_name) DO UPDATE SET
            source_type=excluded.source_type,
            source_url_or_file=excluded.source_url_or_file,
            row_count=excluded.row_count,
            source_refreshed_at=excluded.source_refreshed_at,
            status=excluded.status,
            error_message=excluded.error_message,
            updated_at=CURRENT_TIMESTAMP
        """,
        (source_name, source_url, inserted_count, finished_at, status, error_message),
    )


def fetch_all_pages(api_url: str, service_key: str, *, per_page: int, max_pages: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    page = 1
    total_count: int | None = None
    while page <= max_pages:
        params = {
            "serviceKey": service_key,
            "page": page,
            "perPage": per_page,
            "returnType": "JSON",
        }
        last_error = ""
        response = None
        for attempt in range(len(RETRY_DELAYS) + 1):
            try:
                response = requests.get(api_url, params=params, timeout=45)
                if response.status_code == 200:
                    break
                last_error = f"HTTP {response.status_code}: {response.text[:300]}"
                if response.status_code not in {429, 502, 503, 504}:
                    break
            except requests.RequestException as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            if attempt < len(RETRY_DELAYS):
                time.sleep(RETRY_DELAYS[attempt])
        if response is None or response.status_code != 200:
            raise RuntimeError(last_error or f"page {page} request failed")

        payload = response.json()
        if total_count is None:
            total_count = int(payload.get("totalCount") or payload.get("matchCount") or 0)
            logger.info("API total_count=%s per_page=%s", total_count, per_page)
        data = payload.get("data") or []
        if not data:
            break
        items.extend(data)
        logger.info("Fetched page=%s current=%s accumulated=%s", page, len(data), len(items))
        if total_count and len(items) >= total_count:
            break
        page += 1
    return items


def refresh_product(conn: sqlite3.Connection, rows: list[dict[str, Any]], source_refreshed_at: str) -> tuple[int, int]:
    company_map = company_id_by_bizno(conn)
    insert_rows = []
    matched = 0
    for rec in rows:
        bizno = normalize_business_no(rec.get("업체사업자등록번호"))
        internal_id = company_map.get(bizno)
        if internal_id:
            matched += 1
        insert_rows.append(
            (
                clean_text(rec.get("물품식별번호")),
                clean_text(rec.get("벤처나라물품명")),
                bizno,
                internal_id,
                clean_text(rec.get("업체명")),
                clean_text(rec.get("벤처나라카테고리명")),
                clean_text(rec.get("벤처나라상위카테고리명")),
                normalize_int(rec.get("단가")),
                clean_text(rec.get("단위")),
                clean_text(rec.get("규격")),
                clean_text(rec.get("원산지")),
                clean_text(rec.get("벤처나라상품설명")),
                clean_text(rec.get("벤처나라납품조건")),
                normalize_bool(rec.get("중기간경쟁제품")),
                normalize_bool(rec.get("벤처기업여부")),
                normalize_bool(rec.get("주문자위탁생산여부")),
                clean_text(rec.get("벤처나라유효기간시작일자")),
                clean_text(rec.get("벤처나라유효기간종료일자")),
                clean_text(rec.get("업체인증목록")),
                clean_text(rec.get("의무구매대상인증목록")),
                clean_text(rec.get("우선구매대상인증목록")),
                clean_text(rec.get("벤처나라인증목록")),
                PRODUCT_SOURCE_NAME,
                source_refreshed_at,
            )
        )
    conn.execute("DELETE FROM venture_nara_product WHERE source_name = ?", (PRODUCT_SOURCE_NAME,))
    conn.executemany(
        """
        INSERT OR REPLACE INTO venture_nara_product (
            product_identifier, venture_product_name, bizno, company_internal_id,
            company_name, category_name, parent_category_name, price_amount, price_unit,
            spec, origin_country, description, delivery_condition,
            is_sme_competition_product, venture_company_flag, is_oem,
            valid_from, valid_to, company_cert_list, mandatory_purchase_cert_list,
            preferential_purchase_cert_list, venture_nara_cert_list,
            source_name, source_refreshed_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        insert_rows,
    )
    return len(insert_rows), matched


def refresh_designated(conn: sqlite3.Connection, rows: list[dict[str, Any]], source_refreshed_at: str) -> tuple[int, int]:
    company_map = company_id_by_bizno(conn)
    insert_rows = []
    matched = 0
    for rec in rows:
        bizno = normalize_business_no(rec.get("사업자번호"))
        internal_id = company_map.get(bizno)
        if internal_id:
            matched += 1
        insert_rows.append(
            (
                bizno,
                internal_id,
                normalize_int(rec.get("지정년도")),
                normalize_int(rec.get("지정차수")),
                normalize_int(rec.get("지정순번")),
                clean_text(rec.get("제품명목록")),
                clean_text(rec.get("지역명")),
                clean_text(rec.get("지청명")),
                clean_text(rec.get("전화번호목록")),
                clean_text(rec.get("팩스번호목록")),
                clean_text(rec.get("확정여부")),
                clean_text(rec.get("확정일자")),
                clean_text(rec.get("입력일자")),
                DESIGNATED_SOURCE_NAME,
                source_refreshed_at,
            )
        )
    conn.execute("DELETE FROM venture_nara_designated_company WHERE source_name = ?", (DESIGNATED_SOURCE_NAME,))
    conn.executemany(
        """
        INSERT OR REPLACE INTO venture_nara_designated_company (
            bizno, company_internal_id, designated_year, designated_round, designated_seq,
            product_names, region_name, pps_branch_name, phone_numbers, fax_numbers,
            confirmed_yn, confirmed_date, input_date, source_name, source_refreshed_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        insert_rows,
    )
    return len(insert_rows), matched


def validate(conn: sqlite3.Connection) -> dict[str, Any]:
    result = {
        "venture_nara_product": conn.execute("SELECT COUNT(*) FROM venture_nara_product").fetchone()[0],
        "venture_nara_product_busan_matched": conn.execute(
            "SELECT COUNT(*) FROM venture_nara_product WHERE company_internal_id IS NOT NULL"
        ).fetchone()[0],
        "venture_nara_designated_company": conn.execute("SELECT COUNT(*) FROM venture_nara_designated_company").fetchone()[0],
        "venture_nara_designated_busan_matched": conn.execute(
            "SELECT COUNT(*) FROM venture_nara_designated_company WHERE company_internal_id IS NOT NULL"
        ).fetchone()[0],
        "candidate_view_venture_product_nonempty": conn.execute(
            """
            SELECT COUNT(*) FROM chatbot_company_candidate_view
            WHERE IFNULL(venture_nara_product_summary_raw, '') <> ''
            """
        ).fetchone()[0],
    }
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Import VentureNara product/designated-company ODCloud data.")
    parser.add_argument("--db", default=DB_FILE)
    parser.add_argument("--per-page", type=int, default=DEFAULT_PER_PAGE)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--product-only", action="store_true")
    parser.add_argument("--designated-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not SERVICE_KEY:
        raise SystemExit("SERVICE_KEY is not set. Set VENTURE_NARA_SERVICE_KEY, ODCLOUD_API_KEY, or SERVICE_KEY.")
    if args.product_only and args.designated_only:
        raise SystemExit("--product-only and --designated-only cannot be used together")

    started_at = now_text()
    source_refreshed_at = now_text()
    product_rows: list[dict[str, Any]] = []
    designated_rows: list[dict[str, Any]] = []
    if not args.designated_only:
        product_rows = fetch_all_pages(PRODUCT_API_URL, SERVICE_KEY, per_page=args.per_page, max_pages=args.max_pages)
    if not args.product_only:
        designated_rows = fetch_all_pages(DESIGNATED_API_URL, SERVICE_KEY, per_page=args.per_page, max_pages=args.max_pages)

    if args.dry_run:
        print(json.dumps({
            "status": "dry_run_ok",
            "product_rows": len(product_rows),
            "designated_rows": len(designated_rows),
            "product_sample": product_rows[:1],
            "designated_sample": designated_rows[:1],
        }, ensure_ascii=False, indent=2))
        return 0

    conn = sqlite3.connect(args.db)
    try:
        ensure_schema(conn)
        with conn:
            product_inserted = product_matched = designated_inserted = designated_matched = 0
            if not args.designated_only:
                product_inserted, product_matched = refresh_product(conn, product_rows, source_refreshed_at)
                log_etl(
                    conn,
                    job_name="import_venture_nara_product_api",
                    source_name=PRODUCT_SOURCE_NAME,
                    source_url=PRODUCT_API_URL,
                    started_at=started_at,
                    status="success",
                    input_count=len(product_rows),
                    inserted_count=product_inserted,
                    skipped_count=max(0, len(product_rows) - product_inserted),
                )
            if not args.product_only:
                designated_inserted, designated_matched = refresh_designated(conn, designated_rows, source_refreshed_at)
                log_etl(
                    conn,
                    job_name="import_venture_nara_designated_company_api",
                    source_name=DESIGNATED_SOURCE_NAME,
                    source_url=DESIGNATED_API_URL,
                    started_at=started_at,
                    status="success",
                    input_count=len(designated_rows),
                    inserted_count=designated_inserted,
                    skipped_count=max(0, len(designated_rows) - designated_inserted),
                )
        result = validate(conn)
        result.update({
            "status": "success",
            "db": args.db,
            "product_api_rows": len(product_rows),
            "product_inserted": product_inserted,
            "product_busan_matched": product_matched,
            "designated_api_rows": len(designated_rows),
            "designated_inserted": designated_inserted,
            "designated_busan_matched": designated_matched,
        })
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as exc:
        with conn:
            log_etl(
                conn,
                job_name="import_venture_nara_api",
                source_name="venture_nara_api",
                source_url=f"{PRODUCT_API_URL} | {DESIGNATED_API_URL}",
                started_at=started_at,
                status="failed",
                input_count=len(product_rows) + len(designated_rows),
                inserted_count=0,
                error_count=1,
                error_message=f"{type(exc).__name__}: {exc}",
            )
        raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
