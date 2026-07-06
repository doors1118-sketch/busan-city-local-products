#!/usr/bin/env python3
"""Import SMPP product policy reference files into chatbot_company.db.

The source files are downloaded manually from SMPP.  They are policy/reference
data, not company candidate data, so this loader keeps them in ref_* tables and
exposes a product-level summary view keyed by 10-digit detail product code.
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd


DEFAULT_DB = os.environ.get("CHATBOT_DB", "chatbot_company.db")

SOURCES = {
    "construction_material": {
        "filename": "공사용자재 품목.xls",
        "table": "ref_construction_material_product",
        "job": "import_smpp_construction_material_product",
        "source": "smpp_construction_material_product_file",
    },
    "required_note": {
        "filename": "세부품목별_필수특이사항_목록.xls",
        "table": "ref_product_required_note",
        "job": "import_smpp_product_required_note",
        "source": "smpp_product_required_note_file",
    },
    "eligible_coop": {
        "filename": "적격조합 현황.xls",
        "table": "ref_eligible_cooperative",
        "job": "import_smpp_eligible_cooperative",
        "source": "smpp_eligible_cooperative_file",
    },
    "coop_joint_product": {
        "filename": "조합공동사업제품.xls",
        "table": "ref_coop_joint_product",
        "job": "import_smpp_coop_joint_product",
        "source": "smpp_coop_joint_product_file",
    },
}


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def clean_text(value) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def clean_code(value) -> str:
    text = clean_text(value).replace(".0", "")
    digits = re.sub(r"\D", "", text)
    return digits if len(digits) == 10 else ""


def clean_date(value) -> str:
    text = clean_text(value).replace(".0", "")
    digits = re.sub(r"\D", "", text)
    return digits[:8] if len(digits) >= 8 else text


def is_busan_name(value) -> int:
    return 1 if "부산" in clean_text(value) else 0


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().replace("\n", "").replace(" ", "") for c in df.columns]
    return df.dropna(how="all")


def read_excel(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    engine = "xlrd" if suffix == ".xls" else "openpyxl" if suffix in {".xlsx", ".xlsm"} else None
    if engine is None:
        raise ValueError(f"Unsupported source file extension: {path}")
    return normalize_columns(pd.read_excel(path, engine=engine, dtype=str))


def require_cols(df: pd.DataFrame, cols: Iterable[str], path: Path) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{path.name}: required columns missing: {missing}; columns={list(df.columns)}")


def exec_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS ref_sme_competition_product (
            detail_category_code TEXT PRIMARY KEY,
            category_name TEXT,
            detail_category_name TEXT,
            sme_competition_target BOOLEAN DEFAULT 1,
            direct_purchase_target BOOLEAN DEFAULT 0,
            valid_start_date DATE,
            valid_end_date DATE,
            source_name TEXT,
            source_refreshed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS company_product (
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_internal_id INTEGER,
            product_name TEXT,
            product_name_normalized TEXT,
            product_code TEXT,
            g2b_category_code TEXT,
            is_representative_product BOOLEAN DEFAULT 0,
            product_source TEXT,
            source_refreshed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS shopping_mall_product (
            shopping_mall_product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_internal_id INTEGER,
            product_name TEXT,
            product_name_normalized TEXT,
            product_code TEXT,
            detail_product_name TEXT,
            detail_product_code TEXT,
            contract_status TEXT DEFAULT 'unknown',
            source_name TEXT,
            source_refreshed_at DATETIME
        );

        CREATE TABLE IF NOT EXISTS mas_product (
            mas_product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_internal_id INTEGER,
            product_name TEXT,
            product_name_normalized TEXT,
            product_code TEXT,
            detail_product_name TEXT,
            detail_product_code TEXT,
            contract_status TEXT DEFAULT 'unknown',
            source_name TEXT,
            source_refreshed_at DATETIME
        );

        CREATE TABLE IF NOT EXISTS direct_production_certificate (
            direct_production_cert_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_internal_id INTEGER,
            detail_product_name TEXT,
            detail_product_name_normalized TEXT,
            detail_product_code TEXT,
            validity_status TEXT DEFAULT 'unknown',
            source_name TEXT,
            source_refreshed_at DATETIME
        );

        CREATE TABLE IF NOT EXISTS company_procurement_attribute (
            attribute_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_internal_id INTEGER,
            attribute_type TEXT,
            product_name TEXT,
            product_code TEXT,
            detail_product_code TEXT,
            source_name TEXT,
            source_refreshed_at DATETIME
        );

        CREATE TABLE IF NOT EXISTS product_general_certification (
            general_cert_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_internal_id INTEGER,
            product_name TEXT,
            product_code TEXT,
            detail_product_code TEXT,
            source_name TEXT,
            source_refreshed_at DATETIME
        );

        CREATE TABLE IF NOT EXISTS ref_construction_material_product (
            detail_product_code TEXT PRIMARY KEY,
            division_code TEXT,
            industry_group TEXT,
            product_name TEXT,
            product_classification_no TEXT,
            product_classification_name TEXT,
            detail_product_name TEXT,
            special_note TEXT,
            source_name TEXT NOT NULL,
            source_file_name TEXT,
            source_refreshed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS ref_product_required_note (
            detail_product_code TEXT PRIMARY KEY,
            product_classification_name TEXT,
            detail_product_name TEXT,
            required_note TEXT,
            source_name TEXT NOT NULL,
            source_file_name TEXT,
            source_refreshed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS ref_eligible_cooperative (
            eligible_coop_id INTEGER PRIMARY KEY AUTOINCREMENT,
            cooperative_name TEXT NOT NULL,
            responsible_product_name TEXT,
            detail_product_code TEXT NOT NULL,
            detail_product_name TEXT,
            confirmed_date TEXT,
            is_busan_coop INTEGER DEFAULT 0,
            source_name TEXT NOT NULL,
            source_file_name TEXT,
            source_refreshed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_ref_eligible_coop_unique
        ON ref_eligible_cooperative(cooperative_name, detail_product_code, source_name);

        CREATE TABLE IF NOT EXISTS ref_coop_joint_product (
            coop_joint_product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            joint_business_name TEXT,
            cooperative_name TEXT NOT NULL,
            detail_product_name TEXT,
            detail_product_code TEXT NOT NULL,
            cooperative_phone TEXT,
            cooperative_fax TEXT,
            competition_product_label TEXT,
            is_sme_competition_product INTEGER DEFAULT 0,
            is_busan_coop INTEGER DEFAULT 0,
            source_name TEXT NOT NULL,
            source_file_name TEXT,
            source_refreshed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_ref_coop_joint_unique
        ON ref_coop_joint_product(joint_business_name, cooperative_name, detail_product_code, source_name);

        CREATE INDEX IF NOT EXISTS idx_ref_required_note_code ON ref_product_required_note(detail_product_code);
        CREATE INDEX IF NOT EXISTS idx_ref_construction_material_code ON ref_construction_material_product(detail_product_code);
        CREATE INDEX IF NOT EXISTS idx_ref_eligible_coop_code ON ref_eligible_cooperative(detail_product_code);
        CREATE INDEX IF NOT EXISTS idx_ref_coop_joint_code ON ref_coop_joint_product(detail_product_code);
        CREATE INDEX IF NOT EXISTS idx_ref_eligible_coop_busan ON ref_eligible_cooperative(is_busan_coop);
        CREATE INDEX IF NOT EXISTS idx_ref_coop_joint_busan ON ref_coop_joint_product(is_busan_coop);
        CREATE INDEX IF NOT EXISTS idx_company_product_policy_code ON company_product(product_code);
        CREATE INDEX IF NOT EXISTS idx_shopping_mall_policy_code_status ON shopping_mall_product(detail_product_code, contract_status);
        CREATE INDEX IF NOT EXISTS idx_mas_policy_code_status ON mas_product(detail_product_code, contract_status);
        CREATE INDEX IF NOT EXISTS idx_direct_production_policy_code_status ON direct_production_certificate(detail_product_code, validity_status);
        CREATE INDEX IF NOT EXISTS idx_direct_production_policy_name_status ON direct_production_certificate(detail_product_name, validity_status);
        CREATE INDEX IF NOT EXISTS idx_cpa_policy_code ON company_procurement_attribute(detail_product_code);
        CREATE INDEX IF NOT EXISTS idx_pgc_policy_code ON product_general_certification(detail_product_code);
        """
    )
    create_summary_view(conn)


def create_summary_view(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP VIEW IF EXISTS product_policy_summary;
        CREATE VIEW product_policy_summary AS
        WITH codes AS (
            SELECT detail_category_code AS detail_product_code FROM ref_sme_competition_product WHERE detail_category_code IS NOT NULL AND detail_category_code != ''
            UNION SELECT detail_product_code FROM ref_construction_material_product WHERE detail_product_code IS NOT NULL AND detail_product_code != ''
            UNION SELECT detail_product_code FROM ref_product_required_note WHERE detail_product_code IS NOT NULL AND detail_product_code != ''
            UNION SELECT detail_product_code FROM ref_eligible_cooperative WHERE detail_product_code IS NOT NULL AND detail_product_code != ''
            UNION SELECT detail_product_code FROM ref_coop_joint_product WHERE detail_product_code IS NOT NULL AND detail_product_code != ''
            UNION SELECT product_code FROM company_product WHERE product_code IS NOT NULL AND product_code != ''
            UNION SELECT detail_product_code FROM shopping_mall_product WHERE detail_product_code IS NOT NULL AND detail_product_code != ''
            UNION SELECT detail_product_code FROM mas_product WHERE detail_product_code IS NOT NULL AND detail_product_code != ''
            UNION SELECT detail_product_code FROM pps_shopping_mall_item_policy_summary WHERE detail_product_code IS NOT NULL AND detail_product_code != ''
            UNION SELECT detail_product_code FROM company_procurement_attribute WHERE detail_product_code IS NOT NULL AND detail_product_code != ''
            UNION SELECT detail_product_code FROM product_general_certification WHERE detail_product_code IS NOT NULL AND detail_product_code != ''
        ),
        base AS (
            SELECT
                c.detail_product_code,
                COALESCE(
                    (SELECT detail_category_name FROM ref_sme_competition_product r WHERE r.detail_category_code = c.detail_product_code LIMIT 1),
                    (SELECT detail_product_name FROM ref_construction_material_product r WHERE r.detail_product_code = c.detail_product_code LIMIT 1),
                    (SELECT detail_product_name FROM ref_product_required_note r WHERE r.detail_product_code = c.detail_product_code LIMIT 1),
                    (SELECT detail_product_name FROM ref_eligible_cooperative r WHERE r.detail_product_code = c.detail_product_code LIMIT 1),
                    (SELECT detail_product_name FROM ref_coop_joint_product r WHERE r.detail_product_code = c.detail_product_code LIMIT 1),
                    (SELECT detail_product_name FROM pps_shopping_mall_item_policy_summary p WHERE p.detail_product_code = c.detail_product_code AND IFNULL(p.detail_product_name, '') != '' LIMIT 1),
                    (SELECT detail_product_name FROM shopping_mall_product s WHERE s.detail_product_code = c.detail_product_code AND IFNULL(s.detail_product_name, '') != '' LIMIT 1),
                    (SELECT detail_product_name FROM mas_product m WHERE m.detail_product_code = c.detail_product_code AND IFNULL(m.detail_product_name, '') != '' LIMIT 1),
                    (SELECT product_name FROM company_product p WHERE p.product_code = c.detail_product_code AND IFNULL(p.product_name, '') != '' LIMIT 1),
                    ''
                ) AS detail_product_name
            FROM codes c
        ),
        sme AS (
            SELECT detail_category_code AS detail_product_code, 1 AS is_sme_competition_product
            FROM ref_sme_competition_product
            WHERE detail_category_code IS NOT NULL AND detail_category_code != '' AND IFNULL(sme_competition_target, 1) = 1
            GROUP BY detail_category_code
        ),
        construction AS (
            SELECT detail_product_code, 1 AS is_construction_material_direct_purchase, MAX(special_note) AS construction_material_note
            FROM ref_construction_material_product
            GROUP BY detail_product_code
        ),
        required AS (
            SELECT detail_product_code, MAX(required_note) AS required_special_note
            FROM ref_product_required_note
            GROUP BY detail_product_code
        ),
        eligible AS (
            SELECT
                detail_product_code,
                COUNT(*) AS eligible_coop_count,
                SUM(CASE WHEN is_busan_coop = 1 THEN 1 ELSE 0 END) AS busan_eligible_coop_count,
                GROUP_CONCAT(CASE WHEN is_busan_coop = 1 THEN cooperative_name END, '|') AS busan_eligible_coops
            FROM ref_eligible_cooperative
            GROUP BY detail_product_code
        ),
        joint AS (
            SELECT
                detail_product_code,
                COUNT(*) AS coop_joint_product_count,
                SUM(CASE WHEN is_busan_coop = 1 THEN 1 ELSE 0 END) AS busan_coop_joint_product_count,
                GROUP_CONCAT(CASE WHEN is_busan_coop = 1 THEN cooperative_name END, '|') AS busan_coop_joint_product_coops
            FROM ref_coop_joint_product
            GROUP BY detail_product_code
        ),
        mas_counts AS (
            SELECT detail_product_code, COUNT(DISTINCT company_internal_id) AS mas_active_supplier_count
            FROM mas_product
            WHERE contract_status = 'active' AND detail_product_code IS NOT NULL AND detail_product_code != ''
            GROUP BY detail_product_code
        ),
        shopping_counts AS (
            SELECT detail_product_code, COUNT(DISTINCT company_internal_id) AS shopping_mall_active_supplier_count
            FROM shopping_mall_product
            WHERE contract_status = 'active' AND detail_product_code IS NOT NULL AND detail_product_code != ''
            GROUP BY detail_product_code
        ),
        pps_counts AS (
            SELECT
                detail_product_code,
                MAX(active_registered_count) AS pps_active_registered_count,
                MAX(active_third_party_count) AS pps_active_third_party_count,
                MAX(active_mas_count) AS pps_active_mas_count,
                MAX(active_general_unit_price_count) AS pps_active_general_unit_price_count,
                MAX(active_excellent_procurement_count) AS pps_active_excellent_procurement_count,
                MAX(active_sme_competition_count) AS pps_active_sme_competition_count,
                MAX(active_supplier_count) AS pps_active_supplier_count,
                MAX(active_busan_supplier_count) AS pps_active_busan_supplier_count,
                MAX(active_contract_types) AS pps_active_contract_types
            FROM pps_shopping_mall_item_policy_summary
            WHERE detail_product_code IS NOT NULL AND detail_product_code != ''
            GROUP BY detail_product_code
        ),
        company_product_counts AS (
            SELECT product_code AS detail_product_code, COUNT(DISTINCT company_internal_id) AS busan_company_product_count
            FROM company_product
            WHERE product_code IS NOT NULL AND product_code != ''
            GROUP BY product_code
        ),
        direct_counts AS (
            SELECT b.detail_product_code, COUNT(DISTINCT d.company_internal_id) AS direct_production_valid_supplier_count
            FROM base b
            JOIN direct_production_certificate d
              ON d.validity_status = 'valid'
             AND (
                d.detail_product_code = b.detail_product_code
                OR (IFNULL(d.detail_product_code, '') = '' AND d.detail_product_name = b.detail_product_name)
             )
            GROUP BY b.detail_product_code
        )
        SELECT
            b.detail_product_code,
            b.detail_product_name,
            IFNULL(sme.is_sme_competition_product, 0) AS is_sme_competition_product,
            IFNULL(construction.is_construction_material_direct_purchase, 0) AS is_construction_material_direct_purchase,
            required.required_special_note,
            construction.construction_material_note,
            IFNULL(eligible.eligible_coop_count, 0) AS eligible_coop_count,
            IFNULL(eligible.busan_eligible_coop_count, 0) AS busan_eligible_coop_count,
            eligible.busan_eligible_coops,
            IFNULL(joint.coop_joint_product_count, 0) AS coop_joint_product_count,
            IFNULL(joint.busan_coop_joint_product_count, 0) AS busan_coop_joint_product_count,
            joint.busan_coop_joint_product_coops,
            MAX(IFNULL(mas_counts.mas_active_supplier_count, 0), IFNULL(pps_counts.pps_active_mas_count, 0)) AS mas_active_supplier_count,
            MAX(IFNULL(shopping_counts.shopping_mall_active_supplier_count, 0), IFNULL(pps_counts.pps_active_supplier_count, 0)) AS shopping_mall_active_supplier_count,
            IFNULL(company_product_counts.busan_company_product_count, 0) AS busan_company_product_count,
            IFNULL(direct_counts.direct_production_valid_supplier_count, 0) AS direct_production_valid_supplier_count,
            IFNULL(pps_counts.pps_active_registered_count, 0) AS shopping_mall_active_registered_count,
            IFNULL(pps_counts.pps_active_third_party_count, 0) AS shopping_mall_active_third_party_count,
            IFNULL(pps_counts.pps_active_mas_count, 0) AS shopping_mall_active_mas_count,
            IFNULL(pps_counts.pps_active_general_unit_price_count, 0) AS shopping_mall_active_general_unit_price_count,
            IFNULL(pps_counts.pps_active_excellent_procurement_count, 0) AS shopping_mall_active_excellent_procurement_count,
            IFNULL(pps_counts.pps_active_sme_competition_count, 0) AS shopping_mall_active_sme_competition_count,
            IFNULL(pps_counts.pps_active_busan_supplier_count, 0) AS shopping_mall_active_busan_supplier_count,
            pps_counts.pps_active_contract_types AS shopping_mall_active_contract_types,
            (
                CASE WHEN EXISTS (SELECT 1 FROM ref_sme_competition_product r WHERE r.detail_category_code = b.detail_product_code) THEN 'sme_competition|' ELSE '' END ||
                CASE WHEN EXISTS (SELECT 1 FROM ref_construction_material_product r WHERE r.detail_product_code = b.detail_product_code) THEN 'construction_material|' ELSE '' END ||
                CASE WHEN EXISTS (SELECT 1 FROM ref_product_required_note r WHERE r.detail_product_code = b.detail_product_code) THEN 'required_note|' ELSE '' END ||
                CASE WHEN EXISTS (SELECT 1 FROM ref_eligible_cooperative r WHERE r.detail_product_code = b.detail_product_code) THEN 'eligible_coop|' ELSE '' END ||
                CASE WHEN EXISTS (SELECT 1 FROM ref_coop_joint_product r WHERE r.detail_product_code = b.detail_product_code) THEN 'coop_joint_product|' ELSE '' END ||
                CASE WHEN EXISTS (SELECT 1 FROM mas_product m WHERE m.detail_product_code = b.detail_product_code) THEN 'mas|' ELSE '' END ||
                CASE WHEN EXISTS (SELECT 1 FROM shopping_mall_product s WHERE s.detail_product_code = b.detail_product_code) THEN 'shopping_mall|' ELSE '' END ||
                CASE WHEN EXISTS (SELECT 1 FROM pps_shopping_mall_item_policy_summary p WHERE p.detail_product_code = b.detail_product_code) THEN 'pps_shopping_mall_master|' ELSE '' END ||
                CASE WHEN EXISTS (SELECT 1 FROM company_product p WHERE p.product_code = b.detail_product_code) THEN 'company_product|' ELSE '' END
            ) AS source_refs,
            DATETIME('now') AS generated_at
        FROM base b
        LEFT JOIN sme ON sme.detail_product_code = b.detail_product_code
        LEFT JOIN construction ON construction.detail_product_code = b.detail_product_code
        LEFT JOIN required ON required.detail_product_code = b.detail_product_code
        LEFT JOIN eligible ON eligible.detail_product_code = b.detail_product_code
        LEFT JOIN joint ON joint.detail_product_code = b.detail_product_code
        LEFT JOIN mas_counts ON mas_counts.detail_product_code = b.detail_product_code
        LEFT JOIN shopping_counts ON shopping_counts.detail_product_code = b.detail_product_code
        LEFT JOIN pps_counts ON pps_counts.detail_product_code = b.detail_product_code
        LEFT JOIN company_product_counts ON company_product_counts.detail_product_code = b.detail_product_code
        LEFT JOIN direct_counts ON direct_counts.detail_product_code = b.detail_product_code;
        """
    )


def log_success(conn: sqlite3.Connection, job_name: str, source_name: str, source_file: Path, input_count: int, inserted_count: int) -> None:
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
            SET source_type = 'manual_file_import',
                source_url_or_file = ?,
                source_refreshed_at = datetime('now'),
                row_count = ?,
                status = 'success',
                error_message = '',
                updated_at = CURRENT_TIMESTAMP
            WHERE source_name = ?
            """,
            (str(source_file), inserted_count, source_name),
        )
    else:
        conn.execute(
            """
            INSERT INTO source_manifest
                (source_name, source_type, source_url_or_file, source_refreshed_at, row_count, status, error_message)
            VALUES (?, 'manual_file_import', ?, datetime('now'), ?, 'success', '')
            """,
            (source_name, str(source_file), inserted_count),
        )


def import_construction_material(conn: sqlite3.Connection, path: Path) -> int:
    df = read_excel(path)
    require_cols(df, ["구분", "산업군", "제품명", "물품분류번호", "물품분류명", "세부품명번호", "세부품명"], path)
    source = SOURCES["construction_material"]["source"]
    ts = now_str()
    conn.execute("DELETE FROM ref_construction_material_product WHERE source_name = ?", (source,))
    rows = []
    for _, row in df.iterrows():
        code = clean_code(row.get("세부품명번호"))
        if not code:
            continue
        rows.append(
            (
                code,
                clean_text(row.get("구분")),
                clean_text(row.get("산업군")),
                clean_text(row.get("제품명")),
                clean_text(row.get("물품분류번호")).replace(".0", ""),
                clean_text(row.get("물품분류명")),
                clean_text(row.get("세부품명")),
                clean_text(row.get("특이사항")),
                source,
                path.name,
                ts,
            )
        )
    conn.executemany(
        """
        INSERT OR REPLACE INTO ref_construction_material_product
            (detail_product_code, division_code, industry_group, product_name, product_classification_no,
             product_classification_name, detail_product_name, special_note, source_name, source_file_name, source_refreshed_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        rows,
    )
    real_count = conn.execute("SELECT COUNT(*) FROM ref_construction_material_product WHERE source_name = ?", (source,)).fetchone()[0]
    log_success(conn, SOURCES["construction_material"]["job"], source, path, len(df), real_count)
    return real_count


def import_required_note(conn: sqlite3.Connection, path: Path) -> int:
    df = read_excel(path)
    require_cols(df, ["물품분류명", "세부품명번호", "세부품명", "필수특이사항"], path)
    source = SOURCES["required_note"]["source"]
    ts = now_str()
    conn.execute("DELETE FROM ref_product_required_note WHERE source_name = ?", (source,))
    rows = []
    for _, row in df.iterrows():
        code = clean_code(row.get("세부품명번호"))
        if not code:
            continue
        rows.append(
            (
                code,
                clean_text(row.get("물품분류명")),
                clean_text(row.get("세부품명")),
                clean_text(row.get("필수특이사항")),
                source,
                path.name,
                ts,
            )
        )
    conn.executemany(
        """
        INSERT OR REPLACE INTO ref_product_required_note
            (detail_product_code, product_classification_name, detail_product_name, required_note,
             source_name, source_file_name, source_refreshed_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        rows,
    )
    real_count = conn.execute("SELECT COUNT(*) FROM ref_product_required_note WHERE source_name = ?", (source,)).fetchone()[0]
    log_success(conn, SOURCES["required_note"]["job"], source, path, len(df), real_count)
    return real_count


def import_eligible_coop(conn: sqlite3.Connection, path: Path) -> int:
    df = read_excel(path)
    require_cols(df, ["조합명", "소관제품명", "세부품명번호", "세부품명", "확인일자"], path)
    source = SOURCES["eligible_coop"]["source"]
    ts = now_str()
    conn.execute("DELETE FROM ref_eligible_cooperative WHERE source_name = ?", (source,))
    rows = []
    for _, row in df.iterrows():
        code = clean_code(row.get("세부품명번호"))
        coop_name = clean_text(row.get("조합명"))
        if not code or not coop_name:
            continue
        rows.append(
            (
                coop_name,
                clean_text(row.get("소관제품명")),
                code,
                clean_text(row.get("세부품명")),
                clean_date(row.get("확인일자")),
                is_busan_name(coop_name),
                source,
                path.name,
                ts,
            )
        )
    conn.executemany(
        """
        INSERT OR REPLACE INTO ref_eligible_cooperative
            (cooperative_name, responsible_product_name, detail_product_code, detail_product_name,
             confirmed_date, is_busan_coop, source_name, source_file_name, source_refreshed_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        rows,
    )
    real_count = conn.execute("SELECT COUNT(*) FROM ref_eligible_cooperative WHERE source_name = ?", (source,)).fetchone()[0]
    log_success(conn, SOURCES["eligible_coop"]["job"], source, path, len(df), real_count)
    return real_count


def import_coop_joint_product(conn: sqlite3.Connection, path: Path) -> int:
    df = read_excel(path)
    require_cols(df, ["공동사업명", "조합명", "세부품명", "세부품명번호", "조합연락처", "조합팩스번호", "중기간경쟁제품"], path)
    source = SOURCES["coop_joint_product"]["source"]
    ts = now_str()
    conn.execute("DELETE FROM ref_coop_joint_product WHERE source_name = ?", (source,))
    rows = []
    for _, row in df.iterrows():
        code = clean_code(row.get("세부품명번호"))
        coop_name = clean_text(row.get("조합명"))
        if not code or not coop_name:
            continue
        competition_label = clean_text(row.get("중기간경쟁제품"))
        rows.append(
            (
                clean_text(row.get("공동사업명")),
                coop_name,
                clean_text(row.get("세부품명")),
                code,
                clean_text(row.get("조합연락처")),
                clean_text(row.get("조합팩스번호")),
                competition_label,
                1 if competition_label == "경쟁제품" else 0,
                is_busan_name(coop_name),
                source,
                path.name,
                ts,
            )
        )
    conn.executemany(
        """
        INSERT OR REPLACE INTO ref_coop_joint_product
            (joint_business_name, cooperative_name, detail_product_name, detail_product_code,
             cooperative_phone, cooperative_fax, competition_product_label, is_sme_competition_product,
             is_busan_coop, source_name, source_file_name, source_refreshed_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        rows,
    )
    real_count = conn.execute("SELECT COUNT(*) FROM ref_coop_joint_product WHERE source_name = ?", (source,)).fetchone()[0]
    log_success(conn, SOURCES["coop_joint_product"]["job"], source, path, len(df), real_count)
    return real_count


def resolve_source_files(source_dir: Path) -> dict[str, Path]:
    files = {}
    for key, cfg in SOURCES.items():
        path = source_dir / cfg["filename"]
        if not path.exists():
            raise FileNotFoundError(f"missing source file: {path}")
        files[key] = path
    return files


def import_all(db_path: Path, source_dir: Path) -> dict[str, int]:
    files = resolve_source_files(source_dir)
    conn = sqlite3.connect(db_path)
    try:
        exec_schema(conn)
        counts = {
            "construction_material": import_construction_material(conn, files["construction_material"]),
            "required_note": import_required_note(conn, files["required_note"]),
            "eligible_coop": import_eligible_coop(conn, files["eligible_coop"]),
            "coop_joint_product": import_coop_joint_product(conn, files["coop_joint_product"]),
        }
        create_summary_view(conn)
        conn.commit()
        return counts
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def print_verification(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        checks = [
            ("ref_construction_material_product", "SELECT COUNT(*) FROM ref_construction_material_product"),
            ("ref_product_required_note", "SELECT COUNT(*) FROM ref_product_required_note"),
            ("ref_eligible_cooperative", "SELECT COUNT(*), SUM(is_busan_coop) FROM ref_eligible_cooperative"),
            ("ref_coop_joint_product", "SELECT COUNT(*), SUM(is_busan_coop) FROM ref_coop_joint_product"),
            ("product_policy_summary", "SELECT COUNT(*) FROM product_policy_summary"),
            ("summary_sme", "SELECT COUNT(*) FROM product_policy_summary WHERE is_sme_competition_product=1"),
            ("summary_construction_material", "SELECT COUNT(*) FROM product_policy_summary WHERE is_construction_material_direct_purchase=1"),
            ("summary_busan_coop", "SELECT COUNT(*) FROM product_policy_summary WHERE busan_eligible_coop_count>0 OR busan_coop_joint_product_count>0"),
            ("summary_direct_production", "SELECT COUNT(*) FROM product_policy_summary WHERE direct_production_valid_supplier_count>0"),
        ]
        for label, query in checks:
            row = conn.execute(query).fetchone()
            print(label, row)
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Import SMPP product policy reference files.")
    parser.add_argument("--db", default=DEFAULT_DB, help="Target chatbot_company.db path")
    parser.add_argument("--source-dir", required=True, help="Directory containing the four SMPP .xls files")
    parser.add_argument("--verify-only", action="store_true", help="Only print current counts")
    args = parser.parse_args()

    db_path = Path(args.db)
    source_dir = Path(args.source_dir)
    if args.verify_only:
        print_verification(db_path)
        return 0

    counts = import_all(db_path, source_dir)
    print("imported", counts)
    print_verification(db_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
