#!/usr/bin/env python3
"""Import supplemental vendor recommendation reference sources.

This loader handles manually downloaded or ODCloud-converted public sources
that improve local vendor recommendation quality.  It does not replace the
daily G2B company/industry pipeline.  Raw source rows are first loaded into
staging/reference tables; only records that pass validation are eligible for
recommendation-facing supplement tables.

Primary use cases:
- Busan HQ G2B license ledger: detect missing licenses, construction capacity.
- License dictionary/legal basis: improve license search/explanations.
- VentureNara products/orders: add product evidence without treating it as the
  authoritative national venture-company registry.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlencode
from zipfile import ZipFile

import pandas as pd

try:
    import requests
except Exception:  # pragma: no cover - requests is available in production venv
    requests = None


DEFAULT_DB = os.environ.get("CHATBOT_DB", "chatbot_company.db")
VENTURE_NARA_PRODUCT_API_URL = (
    "https://api.odcloud.kr/api/15127733/v1/"
    "uddi:4d326451-9f87-4727-a6e8-b83afcffc021"
)

SOURCE_LICENSE_SNAPSHOT = "busan_hq_license_snapshot_file"
SOURCE_LICENSE_DICTIONARY = "g2b_license_dictionary_file"
SOURCE_VENTURE_NARA_ORDER = "venture_nara_order_transaction_file"
SOURCE_VENTURE_NARA_PRODUCT = "venture_nara_product_api"


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def clean_digits(value: Any, max_len: int | None = None) -> str:
    digits = re.sub(r"\D", "", clean_text(value).replace(".0", ""))
    return digits[:max_len] if max_len else digits


def clean_bizno(value: Any) -> str:
    digits = clean_digits(value)
    return digits if len(digits) == 10 else digits


def clean_date(value: Any) -> str:
    text = clean_text(value).replace(".0", "")
    digits = re.sub(r"\D", "", text)
    return digits[:8] if len(digits) >= 8 else text


def clean_amount(value: Any) -> int | None:
    digits = re.sub(r"[^0-9\-]", "", clean_text(value).replace(".0", ""))
    if not digits or digits == "-":
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def yesno(value: Any) -> int | None:
    text = clean_text(value).upper()
    if text in {"Y", "YES", "1", "TRUE", "대상", "해당", "O"}:
        return 1
    if text in {"N", "NO", "0", "FALSE", "비대상", "미해당", "X"}:
        return 0
    return None


def normalize_name(value: Any) -> str:
    text = clean_text(value)
    text = re.sub(r"\(주\)|㈜|\(유\)|\(합\)", "", text)
    text = re.sub(r"주식회사|유한회사|합자회사", "", text)
    text = re.sub(r"\s+", "", text)
    return text.upper()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip().replace("\n", "").replace(" ", "") for c in out.columns]
    return out.dropna(how="all")


def _xlsx_col_to_idx(cell_ref: str) -> int:
    letters = "".join(ch for ch in cell_ref if ch.isalpha())
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch.upper()) - 64)
    return n - 1


def read_xlsx_xml_table(path: Path, header_row: int = 5) -> pd.DataFrame:
    """Read BI-style XLSX files even when openpyxl fails on malformed blanks."""
    ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with ZipFile(path) as z:
        names = z.namelist()
        shared: list[str] = []
        if "xl/sharedStrings.xml" in names:
            root = ET.fromstring(z.read("xl/sharedStrings.xml"))
            for si in root.findall("a:si", ns):
                shared.append("".join(t.text or "" for t in si.findall(".//a:t", ns)))

        sheet_names = [n for n in names if n.startswith("xl/worksheets/") and n.endswith(".xml")]
        if not sheet_names:
            raise ValueError(f"No worksheet XML found in {path}")
        sheet_name = sorted(sheet_names)[0]

        row_map: dict[int, dict[int, str]] = {}
        with z.open(sheet_name) as sheet:
            for _, elem in ET.iterparse(sheet, events=("end",)):
                if elem.tag.endswith("row"):
                    r_idx = int(elem.attrib.get("r", "0"))
                    vals: dict[int, str] = {}
                    for c in elem:
                        if not c.tag.endswith("c"):
                            continue
                        ci = _xlsx_col_to_idx(c.attrib.get("r", ""))
                        cell_type = c.attrib.get("t")
                        value = ""
                        if cell_type == "inlineStr":
                            value = "".join(tt.text or "" for tt in c.findall(".//a:t", ns))
                        else:
                            v_elem = next((child for child in c if child.tag.endswith("v")), None)
                            if v_elem is not None:
                                raw = v_elem.text or ""
                                if cell_type == "s" and raw.strip().isdigit():
                                    try:
                                        value = shared[int(raw)]
                                    except Exception:
                                        value = raw
                                else:
                                    value = raw
                        vals[ci] = clean_text(value)
                    if vals:
                        row_map[r_idx] = vals
                    elem.clear()

    if header_row not in row_map:
        raise ValueError(f"Header row {header_row} not found in {path}")
    headers = [
        row_map[header_row].get(i, "").strip() or f"col_{i + 1}"
        for i in range(max(row_map[header_row].keys()) + 1)
    ]
    records: list[dict[str, str]] = []
    for r in sorted(row_map):
        if r <= header_row:
            continue
        record = {headers[i]: row_map[r].get(i, "") for i in range(len(headers))}
        if any(v for v in record.values()):
            records.append(record)
    return pd.DataFrame(records)


def read_report_excel(path: Path, header_row_zero_based: int = 4) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix not in {".xlsx", ".xlsm", ".xls"}:
        raise ValueError(f"Unsupported Excel extension: {path}")
    try:
        engine = "xlrd" if suffix == ".xls" else "openpyxl"
        return normalize_columns(pd.read_excel(path, sheet_name=0, header=header_row_zero_based, dtype=str, engine=engine))
    except Exception:
        if suffix == ".xls":
            raise
        return normalize_columns(read_xlsx_xml_table(path, header_row=header_row_zero_based + 1))


def require_min_cols(df: pd.DataFrame, count: int, path: Path) -> None:
    if len(df.columns) < count:
        raise ValueError(f"{path.name}: expected at least {count} columns, got {len(df.columns)}")


def exec_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS company_business_status (
            company_internal_id INTEGER PRIMARY KEY,
            business_status TEXT NOT NULL DEFAULT 'unknown',
            business_status_freshness TEXT NOT NULL DEFAULT 'not_checked',
            tax_type TEXT,
            closed_at TEXT,
            checked_at DATETIME,
            business_status_source TEXT,
            api_result_code TEXT,
            retry_count INTEGER DEFAULT 0,
            last_error_message TEXT,
            last_attempt_at DATETIME,
            checked_by TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS license_dictionary (
            license_dict_id INTEGER PRIMARY KEY AUTOINCREMENT,
            license_category_code TEXT,
            license_category_name TEXT,
            license_code TEXT,
            license_name TEXT NOT NULL,
            license_name_normalized TEXT,
            legal_basis TEXT,
            related_rule TEXT,
            is_active INTEGER,
            restricted_license_code TEXT,
            restricted_license_name TEXT,
            allowed_license_code TEXT,
            allowed_license_name TEXT,
            source_name TEXT NOT NULL,
            source_file_name TEXT,
            source_refreshed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_name, license_code, license_name, legal_basis)
        );

        CREATE INDEX IF NOT EXISTS idx_license_dictionary_name
        ON license_dictionary(license_name_normalized);

        CREATE TABLE IF NOT EXISTS staging_busan_hq_license_snapshot (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            bizno TEXT NOT NULL,
            company_name TEXT,
            company_name_normalized TEXT,
            company_region TEXT,
            hq_branch_type TEXT,
            fax_no TEXT,
            country_name TEXT,
            company_type TEXT,
            license_name TEXT NOT NULL,
            license_name_normalized TEXT,
            is_representative_license INTEGER,
            g2b_registered_date TEXT,
            is_woman_company_flag INTEGER,
            is_disabled_company_flag INTEGER,
            is_social_company_flag INTEGER,
            construction_capacity_amount INTEGER,
            source_name TEXT NOT NULL,
            source_file_name TEXT,
            source_refreshed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_name, bizno, license_name)
        );

        CREATE INDEX IF NOT EXISTS idx_staging_busan_hq_license_bizno
        ON staging_busan_hq_license_snapshot(bizno);
        CREATE INDEX IF NOT EXISTS idx_staging_busan_hq_license_name
        ON staging_busan_hq_license_snapshot(license_name_normalized);

        CREATE TABLE IF NOT EXISTS company_license_validation (
            validation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            bizno TEXT NOT NULL,
            company_internal_id INTEGER,
            company_name TEXT,
            source_company_name TEXT,
            license_name TEXT NOT NULL,
            is_representative_license INTEGER,
            construction_capacity_amount INTEGER,
            matched_company_master INTEGER DEFAULT 0,
            is_current_busan_hq INTEGER DEFAULT 0,
            business_status TEXT DEFAULT 'unknown',
            business_status_freshness TEXT DEFAULT 'not_checked',
            validation_status TEXT NOT NULL,
            validation_reason TEXT,
            source_name TEXT NOT NULL,
            source_refreshed_at DATETIME,
            validated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_name, bizno, license_name)
        );

        CREATE INDEX IF NOT EXISTS idx_company_license_validation_status
        ON company_license_validation(validation_status);
        CREATE INDEX IF NOT EXISTS idx_company_license_validation_bizno
        ON company_license_validation(bizno);

        CREATE TABLE IF NOT EXISTS company_license_supplement (
            supplement_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_internal_id INTEGER NOT NULL,
            bizno TEXT NOT NULL,
            license_name TEXT NOT NULL,
            license_name_normalized TEXT,
            is_representative_license INTEGER,
            license_source TEXT NOT NULL,
            validity_status TEXT DEFAULT 'valid',
            source_refreshed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(company_internal_id, license_name, license_source)
        );

        CREATE TABLE IF NOT EXISTS company_license_construction_capacity (
            capacity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_internal_id INTEGER,
            bizno TEXT NOT NULL,
            license_name TEXT NOT NULL,
            license_name_normalized TEXT,
            construction_capacity_amount INTEGER NOT NULL,
            source_name TEXT NOT NULL,
            source_file_name TEXT,
            source_refreshed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(bizno, license_name, source_name)
        );

        CREATE INDEX IF NOT EXISTS idx_company_license_capacity_company
        ON company_license_construction_capacity(company_internal_id);

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
        );

        CREATE INDEX IF NOT EXISTS idx_venture_nara_product_bizno
        ON venture_nara_product(bizno);
        CREATE INDEX IF NOT EXISTS idx_venture_nara_product_company
        ON venture_nara_product(company_internal_id);

        CREATE TABLE IF NOT EXISTS venture_nara_order_transaction (
            order_tx_id INTEGER PRIMARY KEY AUTOINCREMENT,
            demand_agency_code TEXT,
            demand_agency_name TEXT,
            demand_agency_region TEXT,
            bizno TEXT,
            company_internal_id INTEGER,
            company_name TEXT,
            item_class_no TEXT,
            item_name TEXT,
            detail_product_code TEXT,
            detail_product_name TEXT,
            product_identifier TEXT,
            product_name TEXT,
            venture_product_name TEXT,
            company_region TEXT,
            performance_date TEXT,
            performance_amount INTEGER,
            unit_price INTEGER,
            quantity INTEGER,
            quote_amount INTEGER,
            mulnap_amount INTEGER,
            manual_sale_amount INTEGER,
            order_amount INTEGER,
            source_name TEXT NOT NULL,
            source_file_name TEXT,
            source_refreshed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_name, demand_agency_code, bizno, product_identifier, performance_date, performance_amount)
        );

        CREATE INDEX IF NOT EXISTS idx_venture_order_bizno
        ON venture_nara_order_transaction(bizno);
        CREATE INDEX IF NOT EXISTS idx_venture_order_company
        ON venture_nara_order_transaction(company_internal_id);

        CREATE TABLE IF NOT EXISTS company_policy_evidence (
            evidence_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_internal_id INTEGER,
            bizno TEXT,
            policy_type TEXT NOT NULL,
            evidence_source TEXT NOT NULL,
            evidence_confidence TEXT NOT NULL,
            evidence_text TEXT,
            valid_from TEXT,
            valid_to TEXT,
            source_refreshed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(bizno, policy_type, evidence_source, evidence_text)
        );

        CREATE INDEX IF NOT EXISTS idx_company_policy_evidence_company
        ON company_policy_evidence(company_internal_id);

        CREATE VIEW IF NOT EXISTS venture_nara_product_summary AS
        SELECT
            company_internal_id,
            bizno,
            COUNT(*) AS venture_nara_product_count,
            SUM(CASE WHEN IFNULL(valid_to, '') = '' OR valid_to >= STRFTIME('%Y%m%d', 'now') THEN 1 ELSE 0 END) AS venture_nara_active_product_count,
            MAX(valid_to) AS venture_nara_last_valid_to,
            GROUP_CONCAT(venture_product_name, '|') AS venture_nara_product_names,
            GROUP_CONCAT(DISTINCT category_name) AS venture_nara_categories,
            MAX(CASE WHEN venture_company_flag = 1 THEN 1 ELSE 0 END) AS venture_nara_has_venture_company_mark,
            MAX(CASE WHEN IFNULL(venture_nara_cert_list, '') LIKE '%벤처창업%' THEN 1 ELSE 0 END) AS venture_nara_has_venture_startup_product,
            MAX(CASE WHEN IFNULL(preferential_purchase_cert_list, '') != '' THEN 1 ELSE 0 END) AS venture_nara_has_priority_purchase_cert
        FROM venture_nara_product
        WHERE bizno IS NOT NULL AND bizno != ''
        GROUP BY company_internal_id, bizno;

        CREATE VIEW IF NOT EXISTS venture_nara_order_summary AS
        SELECT
            company_internal_id,
            bizno,
            COUNT(*) AS venture_nara_order_count,
            SUM(IFNULL(performance_amount, 0)) AS venture_nara_total_order_amount,
            MAX(performance_date) AS venture_nara_last_order_date,
            COUNT(DISTINCT product_identifier) AS venture_nara_order_product_count,
            GROUP_CONCAT(DISTINCT detail_product_name) AS venture_nara_order_detail_products
        FROM venture_nara_order_transaction
        WHERE bizno IS NOT NULL AND bizno != ''
        GROUP BY company_internal_id, bizno;
        """
    )
    ensure_columns(
        conn,
        "company_business_status",
        {
            "business_status_freshness": "TEXT NOT NULL DEFAULT 'not_checked'",
            "closed_at": "TEXT",
            "business_status_source": "TEXT",
            "api_result_code": "TEXT",
            "retry_count": "INTEGER DEFAULT 0",
            "last_error_message": "TEXT",
            "last_attempt_at": "DATETIME",
            "checked_by": "TEXT",
            "created_at": "DATETIME DEFAULT CURRENT_TIMESTAMP",
            "updated_at": "DATETIME DEFAULT CURRENT_TIMESTAMP",
        },
    )


def ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, ddl in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def log_success(
    conn: sqlite3.Connection,
    job_name: str,
    source_name: str,
    source_ref: str,
    input_count: int,
    inserted_count: int,
    checksum: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO etl_job_log
            (job_name, source_name, started_at, finished_at, status, input_row_count, inserted_count, updated_count, skipped_count, error_count, error_message)
        VALUES (?, ?, datetime('now'), datetime('now'), 'success', ?, ?, 0, ?, 0, '')
        """,
        (job_name, source_name, input_count, inserted_count, max(input_count - inserted_count, 0)),
    )
    existing = conn.execute("SELECT source_id FROM source_manifest WHERE source_name = ?", (source_name,)).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE source_manifest
            SET source_type = CASE WHEN ? LIKE 'http%' THEN 'odcloud_api_import' ELSE 'manual_file_import' END,
                source_url_or_file = ?,
                source_refreshed_at = datetime('now'),
                row_count = ?,
                checksum = ?,
                status = 'success',
                error_message = '',
                updated_at = CURRENT_TIMESTAMP
            WHERE source_name = ?
            """,
            (source_ref, source_ref, inserted_count, checksum, source_name),
        )
    else:
        conn.execute(
            """
            INSERT INTO source_manifest
                (source_name, source_type, source_url_or_file, source_refreshed_at, row_count, checksum, status, error_message)
            VALUES (?, CASE WHEN ? LIKE 'http%' THEN 'odcloud_api_import' ELSE 'manual_file_import' END, ?, datetime('now'), ?, ?, 'success', '')
            """,
            (source_name, source_ref, source_ref, inserted_count, checksum),
        )


def load_company_identity(conn: sqlite3.Connection) -> dict[str, tuple[int, int, int, str]]:
    rows = conn.execute(
        """
        SELECT
            ci.canonical_business_no,
            ci.company_internal_id,
            IFNULL(cm.is_busan_company, 0) AS is_busan_company,
            IFNULL(cm.is_headquarters, 0) AS is_headquarters,
            IFNULL(cm.company_name, '') AS company_name
        FROM company_identity ci
        JOIN company_master cm ON cm.company_internal_id = ci.company_internal_id
        """
    ).fetchall()
    return {
        clean_bizno(row[0]): (int(row[1]), int(row[2]), int(row[3]), clean_text(row[4]))
        for row in rows
        if clean_bizno(row[0])
    }


def import_license_dictionary(conn: sqlite3.Connection, path: Path) -> int:
    df = read_report_excel(path)
    require_min_cols(df, 10, path)
    ts = now_str()
    conn.execute("DELETE FROM license_dictionary WHERE source_name = ?", (SOURCE_LICENSE_DICTIONARY,))
    rows = []
    for _, row in df.iterrows():
        values = [clean_text(row.iloc[i]) if i < len(row) else "" for i in range(11)]
        if values[0] == "업종분류" or values[2] == "업종" or not values[2] or not values[3]:
            continue
        rows.append(
            (
                clean_digits(values[0]),
                values[1],
                clean_digits(values[2]),
                values[3],
                normalize_name(values[3]),
                values[4],
                values[5],
                1 if values[6].upper() == "Y" else 0 if values[6].upper() == "N" else None,
                clean_digits(values[7]),
                values[8],
                clean_digits(values[9]),
                values[10],
                SOURCE_LICENSE_DICTIONARY,
                path.name,
                ts,
            )
        )
    conn.executemany(
        """
        INSERT OR REPLACE INTO license_dictionary
            (license_category_code, license_category_name, license_code, license_name, license_name_normalized,
             legal_basis, related_rule, is_active, restricted_license_code, restricted_license_name,
             allowed_license_code, allowed_license_name, source_name, source_file_name, source_refreshed_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        rows,
    )
    log_success(conn, "import_license_dictionary", SOURCE_LICENSE_DICTIONARY, str(path), len(df), len(rows), sha256_file(path))
    return len(rows)


def import_busan_hq_license_snapshot(conn: sqlite3.Connection, path: Path) -> int:
    df = read_report_excel(path)
    require_min_cols(df, 14, path)
    ts = now_str()
    conn.execute("DELETE FROM staging_busan_hq_license_snapshot WHERE source_name = ?", (SOURCE_LICENSE_SNAPSHOT,))
    rows = []
    for _, row in df.iterrows():
        values = [clean_text(row.iloc[i]) if i < len(row) else "" for i in range(14)]
        if values[1] == "사업자등록번호":
            continue
        bizno = clean_bizno(values[1])
        license_name = values[7]
        if not bizno or not license_name:
            continue
        rows.append(
            (
                bizno,
                values[0],
                normalize_name(values[0]),
                values[2],
                values[3],
                values[4],
                values[5],
                values[6],
                license_name,
                normalize_name(license_name),
                yesno(values[8]),
                clean_date(values[9]),
                yesno(values[10]),
                yesno(values[11]),
                yesno(values[12]),
                clean_amount(values[13]),
                SOURCE_LICENSE_SNAPSHOT,
                path.name,
                ts,
            )
        )
    conn.executemany(
        """
        INSERT OR REPLACE INTO staging_busan_hq_license_snapshot
            (bizno, company_name, company_name_normalized, company_region, hq_branch_type, fax_no,
             country_name, company_type, license_name, license_name_normalized, is_representative_license,
             g2b_registered_date, is_woman_company_flag, is_disabled_company_flag, is_social_company_flag,
             construction_capacity_amount, source_name, source_file_name, source_refreshed_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        rows,
    )
    log_success(conn, "import_busan_hq_license_snapshot", SOURCE_LICENSE_SNAPSHOT, str(path), len(df), len(rows), sha256_file(path))
    return len(rows)


def refresh_license_validation(conn: sqlite3.Connection, apply_supplement: bool = False) -> dict[str, int]:
    conn.execute("DELETE FROM company_license_validation WHERE source_name = ?", (SOURCE_LICENSE_SNAPSHOT,))
    conn.execute("DELETE FROM company_license_construction_capacity WHERE source_name = ?", (SOURCE_LICENSE_SNAPSHOT,))
    if apply_supplement:
        conn.execute("DELETE FROM company_license_supplement WHERE license_source = ?", (SOURCE_LICENSE_SNAPSHOT,))

    src_rows = conn.execute(
        """
        SELECT
            s.*,
            ci.company_internal_id,
            cm.company_name AS master_company_name,
            IFNULL(cm.is_busan_company, 0) AS is_busan_company,
            IFNULL(cm.is_headquarters, 0) AS is_headquarters,
            IFNULL(cbs.business_status, 'unknown') AS business_status,
            IFNULL(cbs.business_status_freshness, 'not_checked') AS business_status_freshness
        FROM staging_busan_hq_license_snapshot s
        LEFT JOIN company_identity ci ON ci.canonical_business_no = s.bizno
        LEFT JOIN company_master cm ON cm.company_internal_id = ci.company_internal_id
        LEFT JOIN company_business_status cbs ON cbs.company_internal_id = ci.company_internal_id
        WHERE s.source_name = ?
        """,
        (SOURCE_LICENSE_SNAPSHOT,),
    ).fetchall()

    validation_rows = []
    capacity_rows = []
    supplement_rows = []
    status_counts: dict[str, int] = {}
    for r in src_rows:
        matched = 1 if r["company_internal_id"] is not None else 0
        busan_hq = 1 if matched and int(r["is_busan_company"] or 0) == 1 and int(r["is_headquarters"] or 0) == 1 else 0
        business_status = clean_text(r["business_status"]) or "unknown"
        freshness = clean_text(r["business_status_freshness"]) or "not_checked"

        if not matched:
            validation_status = "unmatched_company_master"
            reason = "사업자번호가 chatbot_company.company_identity에 없음"
        elif not busan_hq:
            validation_status = "not_current_busan_hq"
            reason = "현재 company_master 기준 부산 본사가 아님"
        elif business_status in {"closed", "suspended"} and freshness == "fresh":
            validation_status = "excluded_inactive_business"
            reason = f"NTS 상태={business_status}"
        elif business_status == "active" and freshness == "fresh":
            validation_status = "valid"
            reason = "company_master 부산 본사 + NTS active"
        else:
            validation_status = "needs_status_check"
            reason = f"NTS 미검증 또는 신선도 부족(status={business_status}, freshness={freshness})"

        status_counts[validation_status] = status_counts.get(validation_status, 0) + 1
        validation_rows.append(
            (
                r["bizno"],
                r["company_internal_id"],
                r["master_company_name"],
                r["company_name"],
                r["license_name"],
                r["is_representative_license"],
                r["construction_capacity_amount"],
                matched,
                busan_hq,
                business_status,
                freshness,
                validation_status,
                reason,
                SOURCE_LICENSE_SNAPSHOT,
                r["source_refreshed_at"],
            )
        )

        if validation_status == "valid" and r["construction_capacity_amount"]:
            capacity_rows.append(
                (
                    r["company_internal_id"],
                    r["bizno"],
                    r["license_name"],
                    r["license_name_normalized"],
                    r["construction_capacity_amount"],
                    SOURCE_LICENSE_SNAPSHOT,
                    r["source_file_name"],
                    r["source_refreshed_at"],
                )
            )

        if apply_supplement and validation_status == "valid":
            exists = conn.execute(
                """
                SELECT 1
                FROM company_license cl
                WHERE cl.company_internal_id = ? AND cl.license_name = ?
                LIMIT 1
                """,
                (r["company_internal_id"], r["license_name"]),
            ).fetchone()
            if not exists:
                supplement_rows.append(
                    (
                        r["company_internal_id"],
                        r["bizno"],
                        r["license_name"],
                        r["license_name_normalized"],
                        r["is_representative_license"],
                        SOURCE_LICENSE_SNAPSHOT,
                        r["source_refreshed_at"],
                    )
                )

    conn.executemany(
        """
        INSERT OR REPLACE INTO company_license_validation
            (bizno, company_internal_id, company_name, source_company_name, license_name, is_representative_license,
             construction_capacity_amount, matched_company_master, is_current_busan_hq, business_status,
             business_status_freshness, validation_status, validation_reason, source_name, source_refreshed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        validation_rows,
    )
    conn.executemany(
        """
        INSERT OR REPLACE INTO company_license_construction_capacity
            (company_internal_id, bizno, license_name, license_name_normalized, construction_capacity_amount,
             source_name, source_file_name, source_refreshed_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        capacity_rows,
    )
    if apply_supplement:
        conn.executemany(
            """
            INSERT OR REPLACE INTO company_license_supplement
                (company_internal_id, bizno, license_name, license_name_normalized, is_representative_license,
                 license_source, validity_status, source_refreshed_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 'valid', ?, CURRENT_TIMESTAMP)
            """,
            supplement_rows,
        )
    return {
        **status_counts,
        "validation_rows": len(validation_rows),
        "capacity_rows": len(capacity_rows),
        "supplement_rows": len(supplement_rows),
    }


def import_venture_nara_order_file(conn: sqlite3.Connection, path: Path) -> int:
    df = read_report_excel(path)
    require_min_cols(df, 21, path)
    identity = load_company_identity(conn)
    ts = now_str()
    conn.execute("DELETE FROM venture_nara_order_transaction WHERE source_name = ?", (SOURCE_VENTURE_NARA_ORDER,))
    rows = []
    for _, row in df.iterrows():
        values = [clean_text(row.iloc[i]) if i < len(row) else "" for i in range(21)]
        if values[3] == "업체사업자등록번호":
            continue
        bizno = clean_bizno(values[3])
        product_id = clean_digits(values[9])
        if not bizno or not product_id:
            continue
        internal = identity.get(bizno)
        rows.append(
            (
                values[0],
                values[1],
                values[2],
                bizno,
                internal[0] if internal else None,
                values[4],
                clean_digits(values[5]),
                values[6],
                clean_digits(values[7]),
                values[8],
                product_id,
                values[10],
                values[11],
                values[12],
                clean_date(values[13]),
                clean_amount(values[14]),
                clean_amount(values[15]),
                clean_amount(values[16]),
                clean_amount(values[17]),
                clean_amount(values[18]),
                clean_amount(values[19]),
                clean_amount(values[20]),
                SOURCE_VENTURE_NARA_ORDER,
                path.name,
                ts,
            )
        )
    conn.executemany(
        """
        INSERT OR REPLACE INTO venture_nara_order_transaction
            (demand_agency_code, demand_agency_name, demand_agency_region, bizno, company_internal_id,
             company_name, item_class_no, item_name, detail_product_code, detail_product_name,
             product_identifier, product_name, venture_product_name, company_region, performance_date,
             performance_amount, unit_price, quantity, quote_amount, mulnap_amount, manual_sale_amount,
             order_amount, source_name, source_file_name, source_refreshed_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        rows,
    )
    log_success(conn, "import_venture_nara_order_file", SOURCE_VENTURE_NARA_ORDER, str(path), len(df), len(rows), sha256_file(path))
    return len(rows)


def _get_record(record: dict[str, Any], name: str) -> Any:
    return record.get(name)


def import_venture_nara_products(conn: sqlite3.Connection, records: Iterable[dict[str, Any]], source_ref: str) -> int:
    identity = load_company_identity(conn)
    ts = now_str()
    rows = []
    evidence_rows = []
    for record in records:
        bizno = clean_bizno(_get_record(record, "업체사업자등록번호"))
        product_id = clean_digits(_get_record(record, "물품식별번호"))
        if not bizno or not product_id:
            continue
        internal = identity.get(bizno)
        cert_text = clean_text(_get_record(record, "업체인증목록"))
        venture_cert_text = clean_text(_get_record(record, "벤처나라인증목록"))
        venture_company_flag = yesno(_get_record(record, "벤처기업여부"))
        if venture_company_flag is None:
            venture_company_flag = 1 if "벤처기업" in cert_text or "벤처기업" in venture_cert_text else 0
        rows.append(
            (
                product_id,
                clean_text(_get_record(record, "벤처나라물품명")),
                bizno,
                internal[0] if internal else None,
                clean_text(_get_record(record, "업체명")),
                clean_text(_get_record(record, "벤처나라카테고리명")),
                clean_text(_get_record(record, "벤처나라상위카테고리명")),
                clean_amount(_get_record(record, "단가")),
                clean_text(_get_record(record, "단위")),
                clean_text(_get_record(record, "규격")),
                clean_text(_get_record(record, "원산지")),
                clean_text(_get_record(record, "벤처나라상품설명")),
                clean_text(_get_record(record, "벤처나라납품조건")),
                yesno(_get_record(record, "중기간경쟁제품")),
                venture_company_flag,
                yesno(_get_record(record, "주문자위탁생산여부")),
                clean_date(_get_record(record, "벤처나라유효기간시작일자")),
                clean_date(_get_record(record, "벤처나라유효기간종료일자")),
                cert_text,
                clean_text(_get_record(record, "의무구매대상인증목록")),
                clean_text(_get_record(record, "우선구매대상인증목록")),
                venture_cert_text,
                SOURCE_VENTURE_NARA_PRODUCT,
                ts,
            )
        )
        if venture_company_flag:
            evidence_rows.append(
                (
                    internal[0] if internal else None,
                    bizno,
                    "venture_company",
                    SOURCE_VENTURE_NARA_PRODUCT,
                    "derived",
                    f"벤처나라 상품등록자료 인증목록 기준: {cert_text or venture_cert_text}",
                    clean_date(_get_record(record, "벤처나라유효기간시작일자")),
                    clean_date(_get_record(record, "벤처나라유효기간종료일자")),
                    ts,
                )
            )
    conn.execute("DELETE FROM venture_nara_product WHERE source_name = ?", (SOURCE_VENTURE_NARA_PRODUCT,))
    conn.execute("DELETE FROM company_policy_evidence WHERE evidence_source = ?", (SOURCE_VENTURE_NARA_PRODUCT,))
    conn.executemany(
        """
        INSERT OR REPLACE INTO venture_nara_product
            (product_identifier, venture_product_name, bizno, company_internal_id, company_name,
             category_name, parent_category_name, price_amount, price_unit, spec, origin_country,
             description, delivery_condition, is_sme_competition_product, venture_company_flag,
             is_oem, valid_from, valid_to, company_cert_list, mandatory_purchase_cert_list,
             preferential_purchase_cert_list, venture_nara_cert_list, source_name, source_refreshed_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        rows,
    )
    conn.executemany(
        """
        INSERT OR REPLACE INTO company_policy_evidence
            (company_internal_id, bizno, policy_type, evidence_source, evidence_confidence,
             evidence_text, valid_from, valid_to, source_refreshed_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        evidence_rows,
    )
    log_success(conn, "import_venture_nara_product_api", SOURCE_VENTURE_NARA_PRODUCT, source_ref, len(list(records)) if not isinstance(records, list) else len(records), len(rows))
    return len(rows)


def fetch_odcloud_records(api_url: str, service_key: str, per_page: int = 1000, sleep_sec: float = 0.05) -> list[dict[str, Any]]:
    if requests is None:
        raise RuntimeError("requests is not installed")
    records: list[dict[str, Any]] = []
    page = 1
    while True:
        params = {"page": page, "perPage": per_page, "returnType": "JSON", "serviceKey": service_key}
        url = api_url + ("&" if "?" in api_url else "?") + urlencode(params)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("data") or []
        records.extend(batch)
        total_count = int(data.get("totalCount") or len(records))
        current_count = int(data.get("currentCount") or len(batch))
        if current_count == 0 or len(records) >= total_count:
            break
        page += 1
        if sleep_sec:
            time.sleep(sleep_sec)
    return records


def find_file(source_dir: Path, patterns: list[str]) -> Path | None:
    for pattern in patterns:
        matches = sorted(source_dir.glob(pattern))
        if matches:
            return matches[0]
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=DEFAULT_DB, help="Target chatbot_company.db path")
    parser.add_argument("--source-dir", default=".", help="Directory containing manually downloaded files")
    parser.add_argument("--license-snapshot-file", help="Busan HQ G2B license ledger XLSX")
    parser.add_argument("--license-dictionary-file", help="G2B license/legal-basis dictionary XLSX")
    parser.add_argument("--venture-order-file", help="VentureNara order transaction XLSX")
    parser.add_argument("--fetch-venture-product-api", action="store_true", help="Fetch VentureNara product API and import it")
    parser.add_argument("--venture-product-api-url", default=VENTURE_NARA_PRODUCT_API_URL)
    parser.add_argument("--service-key", default=os.environ.get("ODCLOUD_SERVICE_KEY") or os.environ.get("DATA_GO_KR_SERVICE_KEY"))
    parser.add_argument("--per-page", type=int, default=1000)
    parser.add_argument("--apply-license-supplement", action="store_true", help="Copy valid missing license rows to company_license_supplement")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    source_dir = Path(args.source_dir)
    if not db_path.exists():
        print(f"ERROR: DB not found: {db_path}", file=sys.stderr)
        return 2
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    exec_schema(conn)

    license_dict = Path(args.license_dictionary_file) if args.license_dictionary_file else find_file(
        source_dir, ["UI-ADOBAA-001R*.xlsx", "*업종*근거법규*.xlsx"]
    )
    license_snapshot = Path(args.license_snapshot_file) if args.license_snapshot_file else find_file(
        source_dir, ["UI-ADOAAA-010R.*부산 본사*.xlsx", "*면허*부산 본사*.xlsx"]
    )
    venture_order = Path(args.venture_order_file) if args.venture_order_file else find_file(
        source_dir, ["UI-ADOWAA-002R*.xlsx", "*벤처나라*주문거래*.xlsx"]
    )

    summary: dict[str, Any] = {}
    try:
        if license_dict and license_dict.exists():
            summary["license_dictionary_rows"] = import_license_dictionary(conn, license_dict)
        if license_snapshot and license_snapshot.exists():
            summary["license_snapshot_rows"] = import_busan_hq_license_snapshot(conn, license_snapshot)
            summary["license_validation"] = refresh_license_validation(conn, apply_supplement=args.apply_license_supplement)
        if venture_order and venture_order.exists():
            summary["venture_order_rows"] = import_venture_nara_order_file(conn, venture_order)
        if args.fetch_venture_product_api:
            if not args.service_key:
                raise ValueError("--service-key or ODCLOUD_SERVICE_KEY/DATA_GO_KR_SERVICE_KEY is required for ODCloud API")
            records = fetch_odcloud_records(args.venture_product_api_url, args.service_key, per_page=args.per_page)
            summary["venture_product_rows"] = import_venture_nara_products(conn, records, args.venture_product_api_url)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
