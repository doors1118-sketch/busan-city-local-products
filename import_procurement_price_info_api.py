from __future__ import annotations

import argparse
import hashlib
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
logger = logging.getLogger("ProcurementPriceInfoAPI")

DB_FILE = os.environ.get(
    "CHATBOT_DB",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "chatbot_company.db"),
)
SERVICE_KEY = (
    os.environ.get("PRICE_INFO_SERVICE_KEY")
    or os.environ.get("PROCUREMENT_PRICE_INFO_SERVICE_KEY")
    or os.environ.get("SERVICE_KEY")
)
BASE_URL = os.environ.get(
    "PRICE_INFO_API_BASE_URL",
    "http://apis.data.go.kr/1230000/ao/PriceInfoService",
).rstrip("/")

PRICE_ENDPOINTS = {
    "getPriceInfoListFcltyCmmnMtrilEngrk": "facility_common_material_civil",
    "getPriceInfoListFcltyCmmnMtrilBildng": "facility_common_material_architecture",
    "getPriceInfoListFcltyCmmnMtrilMchnEqp": "facility_common_material_mechanical",
    "getPriceInfoListFcltyCmmnMtrilElctyIrmc": "facility_common_material_electrical_telecom",
    "getPriceInfoListMrktCnstrctPcEngrk": "market_construction_price_civil",
    "getPriceInfoListMrktCnstrctPcBildng": "market_construction_price_architecture",
    "getPriceInfoListMrktCnstrctPcMchnEqp": "market_construction_price_mechanical",
}
CLASSIFICATION_ENDPOINTS = {
    "getCnsttyClsfcInfoList": "construction_work_classification",
    "getNetRsceinfoList": "construction_resource_classification",
}
SUPPORTED_ENDPOINTS = {**PRICE_ENDPOINTS, **CLASSIFICATION_ENDPOINTS}

# These two operations are listed on data.go.kr as of 2026-06-11, but returned
# HTTP 404 on the live service base during verification. Keep them out of the
# default run until the provider path is confirmed.
KNOWN_UNAVAILABLE_ENDPOINTS = {
    "getStdMarkUprcinfoList",
    "getPriceInfoListFcltyCmmnMtrilTotal",
}

DEFAULT_PER_PAGE = int(os.environ.get("PRICE_INFO_PER_PAGE", "10"))
DEFAULT_MAX_PAGES = int(os.environ.get("PRICE_INFO_MAX_PAGES", "100"))
RETRY_DELAYS = [3, 10, 20]


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_space(value: Any) -> str:
    return re.sub(r"\s+", " ", clean_text(value))


def normalize_int(value: Any) -> int | None:
    raw = re.sub(r"[^0-9-]+", "", clean_text(value))
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def now_text() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_meta_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_job_log (
            job_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            source_name TEXT NOT NULL,
            started_at DATETIME NOT NULL,
            finished_at DATETIME,
            status TEXT NOT NULL,
            input_row_count INTEGER DEFAULT 0,
            inserted_count INTEGER DEFAULT 0,
            updated_count INTEGER DEFAULT 0,
            skipped_count INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0,
            error_message TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_manifest (
            source_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT UNIQUE NOT NULL,
            source_type TEXT NOT NULL,
            source_url_or_file TEXT,
            source_refreshed_at DATETIME,
            row_count INTEGER DEFAULT 0,
            checksum TEXT,
            status TEXT,
            error_message TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def ensure_schema(conn: sqlite3.Connection) -> None:
    ensure_meta_schema(conn)
    existing_price_cols = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(procurement_price_info)").fetchall()
    }
    if existing_price_cols and "source_row_hash" not in existing_price_cols:
        # This table was introduced by this importer. The first draft used an
        # overly broad UNIQUE key and collapsed legitimate price rows, so rebuild
        # it before any downstream API depends on it.
        conn.execute("DROP TABLE IF EXISTS procurement_price_info")
        conn.execute("DROP TABLE IF EXISTS procurement_price_info_summary")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS procurement_price_info (
            price_info_id INTEGER PRIMARY KEY AUTOINCREMENT,
            endpoint TEXT NOT NULL,
            source_category TEXT NOT NULL,
            source_row_hash TEXT NOT NULL,
            price_notice_no TEXT,
            notice_datetime TEXT,
            business_division_code TEXT,
            business_division_name TEXT,
            product_classification_no TEXT,
            product_classification_name TEXT,
            product_identifier_no TEXT,
            korean_product_name TEXT,
            unit TEXT,
            price_amount INTEGER,
            material_cost INTEGER,
            labor_cost INTEGER,
            general_expense INTEGER,
            price_division TEXT,
            product_field TEXT,
            delivery_condition_name TEXT,
            payment_condition TEXT,
            vat_yn_name TEXT,
            supply_region_name TEXT,
            contract_company_name TEXT,
            investigation_department_name TEXT,
            raw_json TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_refreshed_at DATETIME NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_name, source_row_hash)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_procurement_price_info_product_name ON procurement_price_info(korean_product_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_procurement_price_info_clsfc ON procurement_price_info(product_classification_no)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_procurement_price_info_identifier ON procurement_price_info(product_identifier_no)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_procurement_price_info_category ON procurement_price_info(source_category)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS construction_work_classification (
            work_classification_id INTEGER PRIMARY KEY AUTOINCREMENT,
            endpoint TEXT NOT NULL,
            construction_division_code TEXT,
            construction_division_name TEXT,
            quantity_calc_code TEXT,
            quantity_calc_name TEXT,
            level1_code TEXT,
            level1_name TEXT,
            level2_code TEXT,
            level2_name TEXT,
            level3_code TEXT,
            level3_name TEXT,
            level4_code TEXT,
            level4_name TEXT,
            level5_code TEXT,
            level5_name TEXT,
            spec TEXT,
            unit TEXT,
            raw_json TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_refreshed_at DATETIME NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_name, quantity_calc_code, level1_code, level2_code, level3_code, level4_code, level5_code, spec, unit)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_construction_work_classification_name ON construction_work_classification(quantity_calc_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_construction_work_classification_lvl5 ON construction_work_classification(level5_name)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS construction_resource_classification (
            resource_classification_id INTEGER PRIMARY KEY AUTOINCREMENT,
            endpoint TEXT NOT NULL,
            resource_type_code TEXT,
            resource_code TEXT,
            resource_name TEXT,
            resource_spec_name TEXT,
            unit TEXT,
            product_management_division_name TEXT,
            group_unit_construction_name TEXT,
            level1_no TEXT,
            level1_name TEXT,
            level2_no TEXT,
            level2_name TEXT,
            level3_no TEXT,
            level3_name TEXT,
            level4_no TEXT,
            level4_name TEXT,
            labor_cost INTEGER,
            general_expense_item_group_code TEXT,
            raw_json TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_refreshed_at DATETIME NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_name, resource_code, resource_name, resource_spec_name, unit)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_construction_resource_name ON construction_resource_classification(resource_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_construction_resource_code ON construction_resource_classification(resource_code)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS procurement_price_info_summary (
            summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_classification_no TEXT,
            product_identifier_no TEXT,
            display_product_name TEXT NOT NULL,
            source_categories TEXT NOT NULL,
            price_info_count INTEGER NOT NULL DEFAULT 0,
            min_price_amount INTEGER,
            max_price_amount INTEGER,
            latest_notice_datetime TEXT,
            sample_unit TEXT,
            source_refs TEXT NOT NULL,
            generated_at DATETIME NOT NULL,
            UNIQUE(product_classification_no, product_identifier_no, display_product_name)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_procurement_price_info_summary_name ON procurement_price_info_summary(display_product_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_procurement_price_info_summary_clsfc ON procurement_price_info_summary(product_classification_no)")
    conn.commit()


def fetch_page(endpoint: str, service_key: str, *, page_no: int, per_page: int) -> dict[str, Any]:
    url = f"{BASE_URL}/{endpoint}"
    params = {
        "serviceKey": service_key,
        "pageNo": page_no,
        "numOfRows": per_page,
        "type": "json",
    }
    last_error = ""
    for attempt in range(len(RETRY_DELAYS) + 1):
        try:
            response = requests.get(url, params=params, timeout=45)
            if response.status_code == 200:
                data = response.json()
                header = data.get("response", {}).get("header", {})
                result_code = clean_text(header.get("resultCode"))
                if result_code and result_code not in {"00", "0"}:
                    raise RuntimeError(f"resultCode={result_code} resultMsg={header.get('resultMsg')}")
                return data
            last_error = f"HTTP {response.status_code}: {response.text[:300]}"
        except Exception as exc:  # noqa: BLE001 - log and retry public API failures.
            last_error = f"{type(exc).__name__}: {exc}"
        if attempt < len(RETRY_DELAYS):
            time.sleep(RETRY_DELAYS[attempt])
    raise RuntimeError(last_error)


def extract_items(data: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
    body = data.get("response", {}).get("body", {})
    items = body.get("items") or []
    if isinstance(items, dict):
        items = [items]
    total = normalize_int(body.get("totalCount")) or 0
    return list(items), total


def fetch_all(endpoint: str, service_key: str, *, per_page: int, max_pages: int) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    total_count = 0
    page_no = 1
    while page_no <= max_pages:
        data = fetch_page(endpoint, service_key, page_no=page_no, per_page=per_page)
        items, total = extract_items(data)
        if page_no == 1:
            total_count = total
        rows.extend(items)
        if page_no == 1 and items and len(items) < per_page and total_count > len(items):
            logger.warning(
                "%s requested numOfRows=%s but API returned %s rows; provider may cap page size",
                endpoint,
                per_page,
                len(items),
            )
        logger.info("%s page=%s rows=%s total=%s", endpoint, page_no, len(items), total_count)
        if not items or len(rows) >= total_count:
            break
        page_no += 1
    return rows, total_count


def source_name_for(endpoint: str) -> str:
    return f"pps_price_info_{SUPPORTED_ENDPOINTS[endpoint]}"


def insert_price_rows(conn: sqlite3.Connection, endpoint: str, rows: list[dict[str, Any]], refreshed_at: str) -> int:
    source_name = source_name_for(endpoint)
    category = PRICE_ENDPOINTS[endpoint]
    conn.execute("DELETE FROM procurement_price_info WHERE source_name = ?", (source_name,))
    inserted = 0
    for row in rows:
        raw_json = json.dumps(row, ensure_ascii=False, sort_keys=True)
        row_hash = hashlib.sha256(f"{endpoint}\n{raw_json}".encode("utf-8")).hexdigest()
        conn.execute(
            """
            INSERT OR REPLACE INTO procurement_price_info (
                endpoint, source_category, source_row_hash, price_notice_no, notice_datetime,
                business_division_code, business_division_name,
                product_classification_no, product_classification_name,
                product_identifier_no, korean_product_name, unit, price_amount,
                material_cost, labor_cost, general_expense, price_division,
                product_field, delivery_condition_name, payment_condition,
                vat_yn_name, supply_region_name, contract_company_name,
                investigation_department_name, raw_json, source_name, source_refreshed_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                endpoint,
                category,
                row_hash,
                clean_text(row.get("prceNticeNo")),
                clean_text(row.get("nticeDt")),
                clean_text(row.get("bsnsDivCd")),
                clean_text(row.get("bsnsDivNm")),
                clean_text(row.get("prdctClsfcNo")),
                clean_text(row.get("prdctClsfcNoNm")),
                clean_text(row.get("prdctIdntNo")),
                normalize_space(row.get("krnPrdctNm")),
                clean_text(row.get("unit")),
                normalize_int(row.get("prce")),
                normalize_int(row.get("mtrlcst")),
                normalize_int(row.get("lbrcst")),
                normalize_int(row.get("gnrlexpns")),
                clean_text(row.get("prceDiv")),
                clean_text(row.get("prodctFld")),
                clean_text(row.get("dlvryCndtnNm")),
                clean_text(row.get("payCndtn")),
                clean_text(row.get("vatYnNm")),
                clean_text(row.get("splyJrsdctRgnNm")),
                clean_text(row.get("cntrctCorpNm")),
                clean_text(row.get("invstDeptNm")),
                raw_json,
                source_name,
                refreshed_at,
            ),
        )
        inserted += 1
    return inserted


def insert_work_classification_rows(conn: sqlite3.Connection, endpoint: str, rows: list[dict[str, Any]], refreshed_at: str) -> int:
    source_name = source_name_for(endpoint)
    conn.execute("DELETE FROM construction_work_classification WHERE source_name = ?", (source_name,))
    inserted = 0
    for row in rows:
        conn.execute(
            """
            INSERT OR REPLACE INTO construction_work_classification (
                endpoint, construction_division_code, construction_division_name,
                quantity_calc_code, quantity_calc_name,
                level1_code, level1_name, level2_code, level2_name,
                level3_code, level3_name, level4_code, level4_name,
                level5_code, level5_name, spec, unit, raw_json,
                source_name, source_refreshed_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                endpoint,
                clean_text(row.get("cnstwkDivCd")),
                clean_text(row.get("cnstwkDivNm")),
                clean_text(row.get("qtyCalcCtyclcd")),
                clean_text(row.get("qtyCalcCtyclNm")),
                clean_text(row.get("LvlqtyCalcCtyclCd1")),
                clean_text(row.get("LvlqtyCalcCtyclNm1")),
                clean_text(row.get("LvlqtyCalcCtyclCd2")),
                clean_text(row.get("LvlqtyCalcCtyclNm2")),
                clean_text(row.get("LvlqtyCalcCtyclCd3")),
                clean_text(row.get("LvlqtyCalcCtyclNm3")),
                clean_text(row.get("LvlqtyCalcCtyclCd4")),
                clean_text(row.get("LvlqtyCalcCtyclNm4")),
                clean_text(row.get("LvlqtyCalcCtyclCd5")),
                clean_text(row.get("LvlqtyCalcCtyclNm5")),
                clean_text(row.get("spec")),
                clean_text(row.get("unit")),
                json.dumps(row, ensure_ascii=False, sort_keys=True),
                source_name,
                refreshed_at,
            ),
        )
        inserted += 1
    return inserted


def insert_resource_rows(conn: sqlite3.Connection, endpoint: str, rows: list[dict[str, Any]], refreshed_at: str) -> int:
    source_name = source_name_for(endpoint)
    conn.execute("DELETE FROM construction_resource_classification WHERE source_name = ?", (source_name,))
    inserted = 0
    for row in rows:
        conn.execute(
            """
            INSERT OR REPLACE INTO construction_resource_classification (
                endpoint, resource_type_code, resource_code, resource_name,
                resource_spec_name, unit, product_management_division_name,
                group_unit_construction_name,
                level1_no, level1_name, level2_no, level2_name,
                level3_no, level3_name, level4_no, level4_name,
                labor_cost, general_expense_item_group_code, raw_json,
                source_name, source_refreshed_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                endpoint,
                clean_text(row.get("rsceTyExtrnlCd")),
                clean_text(row.get("netRsceCd")),
                clean_text(row.get("rsceNm")),
                clean_text(row.get("rsceSpecNm")),
                clean_text(row.get("unit")),
                clean_text(row.get("prdctMngDivNm")),
                clean_text(row.get("grpUnitCnstwkExtrnlCdNm")),
                clean_text(row.get("lvlRsceClsfcNo1")),
                clean_text(row.get("lvlRsceClsfcNm1")),
                clean_text(row.get("lvlRsceClsfcNo2")),
                clean_text(row.get("lvlRsceClsfcNm2")),
                clean_text(row.get("lvlRsceClsfcNo3")),
                clean_text(row.get("lvlRsceClsfcNm3")),
                clean_text(row.get("lvlRsceClsfcNo4")),
                clean_text(row.get("lvlRsceClsfcNm4")),
                normalize_int(row.get("lbrcst")),
                clean_text(row.get("gnrlexpnsItemGrpExtrnCd")),
                json.dumps(row, ensure_ascii=False, sort_keys=True),
                source_name,
                refreshed_at,
            ),
        )
        inserted += 1
    return inserted


def refresh_summary(conn: sqlite3.Connection, generated_at: str) -> int:
    conn.execute("DELETE FROM procurement_price_info_summary")
    conn.execute(
        """
        INSERT INTO procurement_price_info_summary (
            product_classification_no, product_identifier_no, display_product_name,
            source_categories, price_info_count, min_price_amount, max_price_amount,
            latest_notice_datetime, sample_unit, source_refs, generated_at
        )
        SELECT
            IFNULL(product_classification_no, ''),
            IFNULL(product_identifier_no, ''),
            korean_product_name,
            GROUP_CONCAT(DISTINCT source_category),
            COUNT(*),
            MIN(price_amount),
            MAX(price_amount),
            MAX(notice_datetime),
            MIN(unit),
            GROUP_CONCAT(DISTINCT source_name),
            ?
        FROM procurement_price_info
        WHERE IFNULL(korean_product_name, '') <> ''
        GROUP BY IFNULL(product_classification_no, ''), IFNULL(product_identifier_no, ''), korean_product_name
        """,
        (generated_at,),
    )
    row = conn.execute("SELECT COUNT(*) FROM procurement_price_info_summary").fetchone()
    return int(row[0] or 0)


def log_etl(
    conn: sqlite3.Connection,
    *,
    job_name: str,
    endpoint: str,
    status: str,
    started_at: str,
    input_count: int,
    inserted_count: int,
    error_count: int = 0,
    error_message: str = "",
) -> None:
    finished_at = now_text()
    source_name = source_name_for(endpoint)
    conn.execute(
        """
        INSERT INTO etl_job_log (
            job_name, source_name, started_at, finished_at, status,
            input_row_count, inserted_count, skipped_count, error_count, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
        """,
        (job_name, source_name, started_at, finished_at, status, input_count, inserted_count, error_count, error_message[:1000]),
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
        (source_name, f"{BASE_URL}/{endpoint}", inserted_count, finished_at, status, error_message[:1000]),
    )


def run_endpoint(conn: sqlite3.Connection, endpoint: str, *, per_page: int, max_pages: int, dry_run: bool) -> dict[str, Any]:
    if endpoint in KNOWN_UNAVAILABLE_ENDPOINTS:
        raise ValueError(f"{endpoint} is known unavailable on {BASE_URL}")
    if endpoint not in SUPPORTED_ENDPOINTS:
        raise ValueError(f"unsupported endpoint: {endpoint}")
    if not SERVICE_KEY:
        raise RuntimeError("SERVICE_KEY or PRICE_INFO_SERVICE_KEY is required")

    started_at = now_text()
    rows, total_count = fetch_all(endpoint, SERVICE_KEY, per_page=per_page, max_pages=max_pages)
    if dry_run:
        return {"endpoint": endpoint, "fetched": len(rows), "total_count": total_count, "inserted": 0, "dry_run": True}

    refreshed_at = now_text()
    try:
        with conn:
            if endpoint in PRICE_ENDPOINTS:
                inserted = insert_price_rows(conn, endpoint, rows, refreshed_at)
            elif endpoint == "getCnsttyClsfcInfoList":
                inserted = insert_work_classification_rows(conn, endpoint, rows, refreshed_at)
            else:
                inserted = insert_resource_rows(conn, endpoint, rows, refreshed_at)
            log_etl(
                conn,
                job_name="import_procurement_price_info_api",
                endpoint=endpoint,
                status="success",
                started_at=started_at,
                input_count=len(rows),
                inserted_count=inserted,
            )
        return {"endpoint": endpoint, "fetched": len(rows), "total_count": total_count, "inserted": inserted, "dry_run": False}
    except Exception as exc:
        with conn:
            log_etl(
                conn,
                job_name="import_procurement_price_info_api",
                endpoint=endpoint,
                status="failed",
                started_at=started_at,
                input_count=len(rows),
                inserted_count=0,
                error_count=1,
                error_message=f"{type(exc).__name__}: {exc}",
            )
        raise


def parse_endpoints(value: str | None) -> list[str]:
    if not value or value == "all":
        return list(SUPPORTED_ENDPOINTS)
    if value == "price":
        return list(PRICE_ENDPOINTS)
    if value == "classification":
        return list(CLASSIFICATION_ENDPOINTS)
    endpoints = [part.strip() for part in value.split(",") if part.strip()]
    return endpoints


def main() -> int:
    parser = argparse.ArgumentParser(description="Import PPS NaraJangteo price information API data.")
    parser.add_argument("--db", default=DB_FILE)
    parser.add_argument("--endpoints", default="all", help="all, price, classification, or comma-separated endpoint names")
    parser.add_argument("--per-page", type=int, default=DEFAULT_PER_PAGE)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--refresh-summary-only", action="store_true")
    args = parser.parse_args()

    endpoints = parse_endpoints(args.endpoints)
    if not endpoints and not args.refresh_summary_only:
        raise SystemExit("no endpoints selected")

    conn = sqlite3.connect(args.db)
    try:
        conn.row_factory = sqlite3.Row
        ensure_schema(conn)
        results = []
        if not args.refresh_summary_only:
            for endpoint in endpoints:
                results.append(run_endpoint(conn, endpoint, per_page=args.per_page, max_pages=args.max_pages, dry_run=args.dry_run))
        summary_count = 0
        if not args.dry_run:
            with conn:
                summary_count = refresh_summary(conn, now_text())
        print(json.dumps({"db": args.db, "results": results, "summary_count": summary_count}, ensure_ascii=False, indent=2))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
