import argparse
import hashlib
import json
import os
import sqlite3
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Any

import requests


TARGET_DB = os.environ.get("CHATBOT_DB", "staging_chatbot_company.db")
SERVICE_KEY = os.environ.get("SHOPPING_MALL_PRDCT_SERVICE_KEY")

API_URL = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getShoppingMallPrdctInfoList"
API_BASE_URL = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/"
SOURCE_NAME = "shopping_mall_product_master_api"
JOB_NAME = "shopping_mall_product_master_api_incremental"
RETRY_STATUS_CODES = {429, 502, 503, 504}
DEFAULT_RETRY_DELAYS = [5, 15, 30]

CONTRACT_ENDPOINTS = {
    "mas": {
        "operation": "getMASCntrctPrdctInfoList",
        "forced_contract_type": "mas",
        "job_name": "shopping_mall_product_master_mas_change_api",
    },
    "general": {
        "operation": "getUcntrctPrdctInfoList",
        "forced_contract_type": "general_unit_price",
        "job_name": "shopping_mall_product_master_general_change_api",
    },
    "third_party": {
        "operation": "getThptyUcntrctPrdctInfoList",
        "forced_contract_type": "third_party_unit_price",
        "job_name": "shopping_mall_product_master_third_party_change_api",
    },
}


def normalize_text(value: str | None) -> str:
    return (value or "").replace(" ", "").strip().lower()


def clean_date(value: str | None) -> str:
    text = (value or "").strip().replace("-", "")
    return text[:8] if len(text) >= 8 else text


def parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def yes_no_to_int(value: str | None) -> int:
    return 1 if str(value or "").strip().upper() == "Y" else 0


def contract_type(method_name: str | None, mas_yn: str | None, excellent_yn: str | None) -> str:
    method = method_name or ""
    if "\uc81c3\uc790" in method:
        return "third_party_unit_price"
    if "\uc77c\ubc18\ub2e8\uac00" in method:
        return "general_unit_price"
    if "\uc6b0\uc218" in method or str(excellent_yn or "").upper() == "Y":
        return "excellent_procurement"
    if "\ub2e4\uc218" in method or str(mas_yn or "").upper() == "Y":
        return "mas"
    return "unknown"


def business_no_hash(value: str | None) -> str | None:
    clean = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not clean:
        return None
    return hashlib.sha256(clean.encode("utf-8")).hexdigest()[:16]


def contract_hash(contract_no: str | None, contract_seq: str | None, product_identifier: str | None) -> str:
    raw = f"{contract_no or ''}|{contract_seq or ''}|{product_identifier or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def get_busan_company_internal_id(conn: sqlite3.Connection, bizno: str | None) -> int | None:
    clean = "".join(ch for ch in str(bizno or "") if ch.isdigit())
    if not clean:
        return None
    try:
        row = conn.execute(
            """
            SELECT ci.company_internal_id
            FROM company_identity ci
            JOIN company_master cm ON cm.company_internal_id = ci.company_internal_id
            WHERE ci.canonical_business_no = ?
              AND IFNULL(cm.is_busan_company, 0) = 1
              AND IFNULL(cm.is_headquarters, 0) = 1
            LIMIT 1
            """,
            (clean,),
        ).fetchone()
    except sqlite3.Error:
        row = None
    return int(row[0]) if row else None


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS pps_shopping_mall_product_master (
            master_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_identifier TEXT,
            product_name TEXT,
            product_name_normalized TEXT,
            product_class_code TEXT,
            product_class_name TEXT,
            detail_product_code TEXT,
            detail_product_name TEXT,
            detail_product_name_normalized TEXT,
            contract_type TEXT NOT NULL DEFAULT 'unknown',
            contract_method_name TEXT,
            is_mas INTEGER DEFAULT 0,
            is_excellent_procurement INTEGER DEFAULT 0,
            is_sme_competition_product INTEGER DEFAULT 0,
            price_amount REAL,
            price_unit TEXT,
            maker_name TEXT,
            delivery_place_name TEXT,
            delivery_condition_name TEXT,
            supply_region_name TEXT,
            delivery_limit_days INTEGER,
            supplier_name TEXT,
            supplier_business_no_hash TEXT,
            busan_company_internal_id INTEGER,
            contract_no_hash TEXT NOT NULL,
            contract_seq TEXT,
            contract_date TEXT,
            contract_start_date TEXT,
            contract_end_date TEXT,
            contract_status TEXT NOT NULL DEFAULT 'unknown',
            product_cert_list TEXT,
            registered_date TEXT,
            changed_date TEXT,
            source_name TEXT NOT NULL,
            source_operation TEXT,
            source_refreshed_at DATETIME NOT NULL,
            raw_payload_json TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_pps_shop_master_unique
        ON pps_shopping_mall_product_master(source_name, contract_no_hash, contract_seq, product_identifier);

        CREATE INDEX IF NOT EXISTS idx_pps_shop_master_detail_code
        ON pps_shopping_mall_product_master(detail_product_code, contract_status, contract_type);

        CREATE INDEX IF NOT EXISTS idx_pps_shop_master_product_name
        ON pps_shopping_mall_product_master(product_name_normalized, detail_product_name_normalized);

        CREATE INDEX IF NOT EXISTS idx_pps_shop_master_busan_company
        ON pps_shopping_mall_product_master(busan_company_internal_id);

        CREATE TABLE IF NOT EXISTS mas_supplier (
            mas_supplier_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_internal_id INTEGER,
            supplier_name TEXT,
            supplier_name_normalized TEXT,
            supplier_business_no_hash TEXT,
            is_busan_company BOOLEAN,
            is_headquarters BOOLEAN,
            source_name TEXT,
            source_refreshed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_mas_supplier_source
        ON mas_supplier(source_name);

        CREATE INDEX IF NOT EXISTS idx_mas_supplier_company
        ON mas_supplier(company_internal_id);

        DROP VIEW IF EXISTS pps_shopping_mall_item_policy_summary;
        CREATE VIEW pps_shopping_mall_item_policy_summary AS
        SELECT
            detail_product_code,
            COALESCE(NULLIF(detail_product_name, ''), MAX(detail_product_name)) AS detail_product_name,
            product_class_code,
            COALESCE(NULLIF(product_class_name, ''), MAX(product_class_name)) AS product_class_name,
            COUNT(*) AS total_registered_count,
            SUM(CASE WHEN contract_status = 'active' THEN 1 ELSE 0 END) AS active_registered_count,
            SUM(CASE WHEN contract_status = 'active' AND contract_type = 'third_party_unit_price' THEN 1 ELSE 0 END) AS active_third_party_count,
            SUM(CASE WHEN contract_status = 'active' AND contract_type = 'mas' THEN 1 ELSE 0 END) AS active_mas_count,
            SUM(CASE WHEN contract_status = 'active' AND contract_type = 'general_unit_price' THEN 1 ELSE 0 END) AS active_general_unit_price_count,
            SUM(CASE WHEN contract_status = 'active' AND contract_type = 'excellent_procurement' THEN 1 ELSE 0 END) AS active_excellent_procurement_count,
            SUM(CASE WHEN contract_status = 'active' AND is_sme_competition_product = 1 THEN 1 ELSE 0 END) AS active_sme_competition_count,
            COUNT(DISTINCT CASE WHEN contract_status = 'active' THEN supplier_business_no_hash END) AS active_supplier_count,
            COUNT(DISTINCT CASE WHEN contract_status = 'active' AND busan_company_internal_id IS NOT NULL THEN busan_company_internal_id END) AS active_busan_supplier_count,
            MIN(CASE WHEN contract_status = 'active' THEN price_amount END) AS active_min_price_amount,
            MAX(CASE WHEN contract_status = 'active' THEN price_amount END) AS active_max_price_amount,
            group_concat(DISTINCT CASE WHEN contract_status = 'active' THEN contract_type END) AS active_contract_types,
            MAX(source_refreshed_at) AS source_refreshed_at
        FROM pps_shopping_mall_product_master
        WHERE detail_product_code IS NOT NULL AND detail_product_code != ''
        GROUP BY detail_product_code, product_class_code;
        """
    )
    existing_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(pps_shopping_mall_product_master)")
    }
    if "changed_date" not in existing_columns:
        conn.execute("ALTER TABLE pps_shopping_mall_product_master ADD COLUMN changed_date TEXT")
    if "source_operation" not in existing_columns:
        conn.execute("ALTER TABLE pps_shopping_mall_product_master ADD COLUMN source_operation TEXT")


def log_job(
    conn: sqlite3.Connection,
    started_at: str,
    status: str,
    input_count: int,
    inserted_count: int,
    skipped_count: int,
    error_count: int,
    message: str,
) -> None:
    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        manifest_row_count = conn.execute("SELECT COUNT(*) FROM pps_shopping_mall_product_master").fetchone()[0]
    except sqlite3.Error:
        manifest_row_count = inserted_count
    try:
        conn.execute(
            """
            INSERT INTO etl_job_log
                (job_name, source_name, started_at, finished_at, status, input_row_count,
                 inserted_count, skipped_count, error_count, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (JOB_NAME, SOURCE_NAME, started_at, finished_at, status, input_count, inserted_count, skipped_count, error_count, message[:500]),
        )
        conn.execute(
            """
            INSERT INTO source_manifest
                (source_name, source_type, source_url_or_file, source_refreshed_at, row_count, status, error_message)
            VALUES (?, 'api_incremental', ?, ?, ?, ?, ?)
            ON CONFLICT(source_name) DO UPDATE SET
                source_url_or_file=excluded.source_url_or_file,
                source_refreshed_at=excluded.source_refreshed_at,
                row_count=excluded.row_count,
                status=excluded.status,
                error_message=excluded.error_message,
                updated_at=CURRENT_TIMESTAMP
            """,
            (SOURCE_NAME, API_URL, finished_at, manifest_row_count, status, message[:500]),
        )
    except sqlite3.Error:
        pass


def fetch_page(url: str, params: dict[str, Any], page: int) -> tuple[requests.Response, int, str]:
    attempts = len(DEFAULT_RETRY_DELAYS) + 1
    last_error = ""
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.get(url, params=params, timeout=60)
            if resp.status_code == 200:
                return resp, attempt - 1, ""
            last_error = f"HTTP {resp.status_code} at page {page}"
            if resp.status_code not in RETRY_STATUS_CODES or attempt == attempts:
                return resp, attempt - 1, last_error
        except requests.exceptions.RequestException as exc:
            last_error = f"{type(exc).__name__} at page {page}"
            if attempt == attempts:
                raise
        delay = DEFAULT_RETRY_DELAYS[attempt - 1]
        print(f"retry page={page} attempt={attempt}/{attempts - 1} after {delay}s reason={last_error}")
        time.sleep(delay)
    raise RuntimeError(last_error)


def item_to_record(
    conn: sqlite3.Connection,
    item: ET.Element,
    now_str: str,
    current_date: str,
    source_operation: str,
    forced_contract_type: str | None = None,
) -> dict[str, Any]:
    product_identifier = item.findtext("prdctIdntNo", "")
    product_class_code = item.findtext("prdctClsfcNo", "")
    product_class_name = item.findtext("prdctClsfcNoNm", "")
    detail_product_code = item.findtext("dtilPrdctClsfcNo", "") or item.findtext("dtlPrdctClsfcNo", "")
    detail_product_name = item.findtext("dtilPrdctClsfcNoNm", "") or item.findtext("dtlPrdctClsfcNoNm", "")
    product_name = product_class_name or detail_product_name or item.findtext("prdctSpecNm", "")
    method_name = item.findtext("cntrctMthdNm", "")
    mas_yn = item.findtext("masYn", "")
    excellent_yn = item.findtext("exclncPrcrmntPrdctYn", "")
    contract_no = item.findtext("shopngCntrctNo", "") or item.findtext("cntrctNo", "")
    contract_seq = item.findtext("shopngCntrctSno", "")
    contract_end = clean_date(item.findtext("cntrctEndDate", "") or item.findtext("cntrctEndDt", ""))
    contract_status = "unknown"
    if contract_end:
        contract_status = "active" if current_date <= contract_end else "expired"
    bizno = item.findtext("cntrctCorpBizno", "") or item.findtext("bizrno", "") or item.findtext("cntrctCorpNo", "")
    raw_payload = {child.tag: child.text for child in item}
    delivery_limit = item.findtext("dlvrTmlmtDaynum", "")
    try:
        delivery_limit_days = int(str(delivery_limit).strip()) if str(delivery_limit).strip() else None
    except ValueError:
        delivery_limit_days = None
    return {
        "product_identifier": product_identifier,
        "product_name": product_name,
        "product_name_normalized": normalize_text(product_name),
        "product_class_code": product_class_code,
        "product_class_name": product_class_name,
        "detail_product_code": detail_product_code,
        "detail_product_name": detail_product_name,
        "detail_product_name_normalized": normalize_text(detail_product_name),
        "contract_type": forced_contract_type or contract_type(method_name, mas_yn, excellent_yn),
        "contract_method_name": method_name,
        "is_mas": yes_no_to_int(mas_yn),
        "is_excellent_procurement": yes_no_to_int(excellent_yn),
        "is_sme_competition_product": yes_no_to_int(item.findtext("smetprCmptProdctYn", "")),
        "price_amount": parse_float(item.findtext("cntrctPrceAmt", "")),
        "price_unit": item.findtext("prdctUnit", ""),
        "maker_name": item.findtext("prdctMakrNm", ""),
        "delivery_place_name": item.findtext("prdctDlvrPlceNm", ""),
        "delivery_condition_name": item.findtext("prdctDlvryCndtnNm", ""),
        "supply_region_name": item.findtext("prdctSplyRgnNm", ""),
        "delivery_limit_days": delivery_limit_days,
        "supplier_name": item.findtext("cntrctCorpNm", ""),
        "supplier_business_no_hash": business_no_hash(bizno),
        "busan_company_internal_id": get_busan_company_internal_id(conn, bizno),
        "contract_no_hash": contract_hash(contract_no, contract_seq, product_identifier),
        "contract_seq": contract_seq,
        "contract_date": clean_date(item.findtext("cntrctDate", "")),
        "contract_start_date": clean_date(item.findtext("cntrctBgnDate", "") or item.findtext("cntrctBgnDt", "")),
        "contract_end_date": contract_end,
        "contract_status": contract_status,
        "product_cert_list": item.findtext("prodctCertList", ""),
        "registered_date": clean_date(item.findtext("rgstDt", "")),
        "changed_date": clean_date(item.findtext("chgDt", "")),
        "source_name": SOURCE_NAME,
        "source_operation": source_operation,
        "source_refreshed_at": now_str,
        "raw_payload_json": json.dumps(raw_payload, ensure_ascii=False, sort_keys=True),
    }


def upsert_record(conn: sqlite3.Connection, record: dict[str, Any]) -> None:
    columns = list(record.keys())
    placeholders = ", ".join("?" for _ in columns)
    update_clause = ", ".join(
        f"{col}=excluded.{col}"
        for col in columns
        if col not in {"source_name", "contract_no_hash", "contract_seq", "product_identifier"}
    )
    conn.execute(
        f"""
        INSERT INTO pps_shopping_mall_product_master ({", ".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(source_name, contract_no_hash, contract_seq, product_identifier)
        DO UPDATE SET {update_clause}, updated_at=CURRENT_TIMESTAMP
        """,
        [record[col] for col in columns],
    )


def refresh_contract_statuses(conn: sqlite3.Connection, current_date: str) -> tuple[int, int]:
    """Refresh active/expired flags for all stored product contracts.

    The API is fetched by registration date, so old rows may not be seen again
    when only their end date passes. This update keeps "currently usable"
    product-route judgments from relying on stale active flags.
    """
    expired = conn.execute(
        """
        UPDATE pps_shopping_mall_product_master
        SET contract_status='expired', updated_at=CURRENT_TIMESTAMP
        WHERE contract_end_date IS NOT NULL
          AND contract_end_date != ''
          AND contract_end_date < ?
          AND contract_status != 'expired'
        """,
        (current_date,),
    ).rowcount
    active = conn.execute(
        """
        UPDATE pps_shopping_mall_product_master
        SET contract_status='active', updated_at=CURRENT_TIMESTAMP
        WHERE contract_end_date IS NOT NULL
          AND contract_end_date != ''
          AND contract_end_date >= ?
          AND contract_status != 'active'
        """,
        (current_date,),
    ).rowcount
    return int(expired or 0), int(active or 0)


def refresh_mas_suppliers_from_master(conn: sqlite3.Connection) -> int:
    conn.execute("DELETE FROM mas_supplier WHERE source_name = 'pps_shopping_mall_product_master'")
    conn.execute(
        """
        INSERT INTO mas_supplier (
            company_internal_id,
            supplier_name,
            supplier_name_normalized,
            supplier_business_no_hash,
            is_busan_company,
            is_headquarters,
            source_name,
            source_refreshed_at,
            created_at,
            updated_at
        )
        SELECT
            busan_company_internal_id,
            supplier_name,
            LOWER(REPLACE(TRIM(IFNULL(supplier_name, '')), ' ', '')),
            supplier_business_no_hash,
            CASE WHEN busan_company_internal_id IS NOT NULL THEN 1 ELSE 0 END,
            CASE WHEN busan_company_internal_id IS NOT NULL THEN 1 ELSE NULL END,
            'pps_shopping_mall_product_master',
            MAX(source_refreshed_at),
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM pps_shopping_mall_product_master
        WHERE contract_status = 'active'
          AND contract_type IN ('mas', 'third_party_unit_price', 'general_unit_price')
          AND IFNULL(supplier_name, '') != ''
        GROUP BY
            busan_company_internal_id,
            supplier_business_no_hash,
            LOWER(REPLACE(TRIM(IFNULL(supplier_name, '')), ' ', ''))
        """
    )
    count = conn.execute(
        "SELECT COUNT(*) FROM mas_supplier WHERE source_name = 'pps_shopping_mall_product_master'"
    ).fetchone()[0]
    conn.execute(
        """
        INSERT INTO source_manifest
            (source_name, source_type, source_url_or_file, source_refreshed_at, row_count, status, error_message)
        VALUES (
            'mas_supplier_from_pps_shopping_mall_master',
            'derived_table',
            'pps_shopping_mall_product_master',
            CURRENT_TIMESTAMP,
            ?,
            'success',
            'derived from active pps shopping mall product master; includes third_party_unit_price, mas, general_unit_price suppliers'
        )
        ON CONFLICT(source_name) DO UPDATE SET
            source_type = excluded.source_type,
            source_url_or_file = excluded.source_url_or_file,
            source_refreshed_at = excluded.source_refreshed_at,
            row_count = excluded.row_count,
            status = excluded.status,
            error_message = excluded.error_message,
            updated_at = CURRENT_TIMESTAMP
        """,
        (int(count or 0),),
    )
    return int(count or 0)


def run_import(target_date: str | None, days: int, max_pages: int, num_rows: int, probe: bool, dry_run: bool) -> int:
    if not SERVICE_KEY:
        print("ERROR: SHOPPING_MALL_PRDCT_SERVICE_KEY is not set.")
        return 2

    conn = sqlite3.connect(TARGET_DB)
    ensure_schema(conn)

    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    now_str = started_at
    end_date = datetime.strptime(target_date, "%Y%m%d") if target_date else datetime.now()
    start_date = end_date - timedelta(days=max(days - 1, 0))
    bgn_dt = start_date.strftime("%Y%m%d")
    end_dt = end_date.strftime("%Y%m%d")
    current_date = datetime.now().strftime("%Y%m%d")

    total_api_items = 0
    total_written = 0
    total_skipped = 0
    retry_count = 0
    error_count = 0
    expired_refreshed = 0
    active_refreshed = 0
    supplier_refreshed = 0
    status = "success"
    error_msg = ""

    print(f"shopping mall product master import: {bgn_dt}~{end_dt}, db={TARGET_DB}, dry_run={dry_run}, probe={probe}")

    try:
        for page in range(1, max_pages + 1):
            params = {
                "ServiceKey": SERVICE_KEY,
                "numOfRows": str(num_rows),
                "pageNo": str(page),
                "inqryDiv": "1",
                "inqryBgnDate": bgn_dt,
                "inqryEndDate": end_dt,
            }
            resp, page_retries, retry_error = fetch_page(API_URL, params, page)
            retry_count += page_retries
            if resp.status_code != 200:
                status = "partial_success" if total_written else "failed"
                error_count += 1
                error_msg = retry_error or f"HTTP {resp.status_code} at page {page}"
                break

            root = ET.fromstring(resp.content)
            result_code = root.findtext(".//resultCode")
            if result_code != "00":
                status = "partial_success" if total_written else "failed"
                error_count += 1
                error_msg = f"API Code {result_code}: {root.findtext('.//resultMsg')}"
                break

            total_count = int(root.findtext(".//totalCount") or "0")
            items = root.findall(".//item")
            if page == 1:
                print(f"totalCount={total_count}, firstPageItems={len(items)}")
                if items:
                    print("fields=" + ",".join(child.tag for child in items[0]))
                if probe:
                    log_job(conn, started_at, "success", total_count, 0, 0, 0, f"probe totalCount={total_count}")
                    conn.commit()
                    return 0

            if not items:
                break

            for item in items:
                total_api_items += 1
                record = item_to_record(conn, item, now_str, current_date, "getShoppingMallPrdctInfoList")
                if not record["product_identifier"] and not record["detail_product_code"]:
                    total_skipped += 1
                    continue
                if not dry_run:
                    upsert_record(conn, record)
                total_written += 1

            if not dry_run:
                conn.commit()

            if total_count and page * num_rows >= total_count:
                break

    except Exception as exc:
        status = "partial_success" if total_written else "failed"
        error_count += 1
        error_msg = f"{type(exc).__name__}: {str(exc)[:300]}"
    finally:
        if not dry_run and not probe:
            try:
                expired_refreshed, active_refreshed = refresh_contract_statuses(conn, current_date)
                supplier_refreshed = refresh_mas_suppliers_from_master(conn)
                conn.commit()
            except sqlite3.Error as exc:
                status = "partial_success" if total_written else "failed"
                error_count += 1
                refresh_msg = f"status refresh failed: {type(exc).__name__}: {str(exc)[:120]}"
                error_msg = f"{error_msg} | {refresh_msg}" if error_msg else refresh_msg
        msg = f"range={bgn_dt}~{end_dt},written={total_written},skipped={total_skipped},retries={retry_count}"
        msg += f",status_refresh_expired={expired_refreshed},status_refresh_active={active_refreshed}"
        msg += f",supplier_refresh={supplier_refreshed}"
        if error_msg:
            msg = f"{error_msg} | {msg}"
        if not dry_run:
            log_job(conn, started_at, status, total_api_items, total_written, total_skipped, error_count, msg)
            conn.commit()
        conn.close()

    print(f"completed status={status}, api_items={total_api_items}, written={total_written}, skipped={total_skipped}, errors={error_count}, retries={retry_count}")
    if error_msg:
        print(f"error={error_msg}")
    return 0 if status in {"success", "partial_success"} else 1


def run_contract_endpoint_import(
    endpoint_key: str,
    target_date: str | None,
    days: int,
    max_pages: int,
    num_rows: int,
    probe: bool,
    dry_run: bool,
) -> int:
    if endpoint_key not in CONTRACT_ENDPOINTS:
        raise ValueError(f"unknown endpoint_key: {endpoint_key}")
    if not SERVICE_KEY:
        print("ERROR: SHOPPING_MALL_PRDCT_SERVICE_KEY is not set.")
        return 2

    endpoint = CONTRACT_ENDPOINTS[endpoint_key]
    operation = endpoint["operation"]
    forced_type = endpoint["forced_contract_type"]
    url = API_BASE_URL + operation

    conn = sqlite3.connect(TARGET_DB)
    ensure_schema(conn)

    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    now_str = started_at
    end_date = datetime.strptime(target_date, "%Y%m%d") if target_date else datetime.now()
    start_date = end_date - timedelta(days=max(days - 1, 0))
    bgn_dt = start_date.strftime("%Y%m%d")
    end_dt = end_date.strftime("%Y%m%d")
    current_date = datetime.now().strftime("%Y%m%d")

    total_api_items = 0
    total_written = 0
    total_skipped = 0
    retry_count = 0
    error_count = 0
    expired_refreshed = 0
    active_refreshed = 0
    supplier_refreshed = 0
    status = "success"
    error_msg = ""

    print(
        f"shopping mall product master contract import: endpoint={endpoint_key}, "
        f"{bgn_dt}~{end_dt}, db={TARGET_DB}, dry_run={dry_run}, probe={probe}"
    )

    try:
        for page in range(1, max_pages + 1):
            params = {
                "ServiceKey": SERVICE_KEY,
                "serviceKey": SERVICE_KEY,
                "numOfRows": str(num_rows),
                "pageNo": str(page),
                "chgDtBgnDt": bgn_dt,
                "chgDtEndDt": end_dt,
            }
            resp, page_retries, retry_error = fetch_page(url, params, page)
            retry_count += page_retries
            if resp.status_code != 200:
                status = "partial_success" if total_written else "failed"
                error_count += 1
                error_msg = retry_error or f"HTTP {resp.status_code} at page {page}"
                break

            root = ET.fromstring(resp.content)
            result_code = root.findtext(".//resultCode")
            if result_code != "00":
                status = "partial_success" if total_written else "failed"
                error_count += 1
                error_msg = f"API Code {result_code}: {root.findtext('.//resultMsg')}"
                break

            total_count = int(root.findtext(".//totalCount") or "0")
            items = root.findall(".//item")
            if page == 1:
                print(f"totalCount={total_count}, firstPageItems={len(items)}")
                if items:
                    print("fields=" + ",".join(child.tag for child in items[0]))
                if probe:
                    log_job(
                        conn,
                        started_at,
                        "success",
                        total_count,
                        0,
                        0,
                        0,
                        f"probe endpoint={endpoint_key},operation={operation},totalCount={total_count}",
                    )
                    conn.commit()
                    return 0

            if not items:
                break

            for item in items:
                total_api_items += 1
                record = item_to_record(
                    conn,
                    item,
                    now_str,
                    current_date,
                    operation,
                    forced_contract_type=forced_type,
                )
                if not record["product_identifier"] and not record["detail_product_code"]:
                    total_skipped += 1
                    continue
                if not dry_run:
                    upsert_record(conn, record)
                total_written += 1

            if not dry_run:
                conn.commit()

            if total_count and page * num_rows >= total_count:
                break

    except Exception as exc:
        status = "partial_success" if total_written else "failed"
        error_count += 1
        error_msg = f"{type(exc).__name__}: {str(exc)[:300]}"
    finally:
        if not dry_run and not probe:
            try:
                expired_refreshed, active_refreshed = refresh_contract_statuses(conn, current_date)
                supplier_refreshed = refresh_mas_suppliers_from_master(conn)
                conn.commit()
            except sqlite3.Error as exc:
                status = "partial_success" if total_written else "failed"
                error_count += 1
                refresh_msg = f"status refresh failed: {type(exc).__name__}: {str(exc)[:120]}"
                error_msg = f"{error_msg} | {refresh_msg}" if error_msg else refresh_msg
        msg = (
            f"endpoint={endpoint_key},operation={operation},range={bgn_dt}~{end_dt},"
            f"written={total_written},skipped={total_skipped},retries={retry_count},"
            f"status_refresh_expired={expired_refreshed},status_refresh_active={active_refreshed}"
            f",supplier_refresh={supplier_refreshed}"
        )
        if error_msg:
            msg = f"{error_msg} | {msg}"
        if not dry_run:
            log_job(conn, started_at, status, total_api_items, total_written, total_skipped, error_count, msg)
            conn.commit()
        conn.close()

    print(
        f"completed endpoint={endpoint_key}, status={status}, api_items={total_api_items}, "
        f"written={total_written}, skipped={total_skipped}, errors={error_count}, retries={retry_count}"
    )
    if error_msg:
        print(f"error={error_msg}")
    return 0 if status in {"success", "partial_success"} else 1


def run_contract_imports(
    endpoint_key: str,
    target_date: str | None,
    days: int,
    max_pages: int,
    num_rows: int,
    probe: bool,
    dry_run: bool,
) -> int:
    keys = list(CONTRACT_ENDPOINTS) if endpoint_key == "all" else [endpoint_key]
    exit_code = 0
    for key in keys:
        rc = run_contract_endpoint_import(key, target_date, days, max_pages, num_rows, probe, dry_run)
        if rc != 0:
            exit_code = rc
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description="Import national PPS shopping-mall product contract master.")
    parser.add_argument("--target-date", default=None, help="YYYYMMDD end date. Defaults to today.")
    parser.add_argument("--days", type=int, default=1, help="Registration-date range length. Default: 1")
    parser.add_argument("--contract-days", type=int, default=None, help="Change-date range length for contract endpoints. Defaults to --days.")
    parser.add_argument("--max-pages", type=int, default=80, help="Maximum pages. Default: 80")
    parser.add_argument("--num-rows", type=int, default=500, help="Rows per page. Default: 500")
    parser.add_argument("--probe", action="store_true", help="Only print totalCount and fields.")
    parser.add_argument("--dry-run", action="store_true", help="Call API and parse records without writing.")
    parser.add_argument("--include-contract-endpoints", action="store_true", help="Also import MAS/general/third-party change-date endpoints.")
    parser.add_argument("--contract-only", action="store_true", help="Import only MAS/general/third-party change-date endpoints.")
    parser.add_argument("--endpoint", choices=["all", "mas", "general", "third_party"], default="all", help="Contract endpoint to import when using contract mode.")
    args = parser.parse_args()
    exit_code = 0
    if not args.contract_only:
        exit_code = run_import(args.target_date, args.days, args.max_pages, args.num_rows, args.probe, args.dry_run)
    if args.include_contract_endpoints or args.contract_only:
        contract_days = args.contract_days if args.contract_days is not None else args.days
        contract_rc = run_contract_imports(
            args.endpoint,
            args.target_date,
            contract_days,
            args.max_pages,
            args.num_rows,
            args.probe,
            args.dry_run,
        )
        if contract_rc != 0:
            exit_code = contract_rc
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
