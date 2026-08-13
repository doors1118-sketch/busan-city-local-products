#!/usr/bin/env python3
"""Build vendor contract history tables for vendor recommendation.

Source:
  /opt/busan/procurement_contracts.db: servc_cntrct, thng_cntrct

Target:
  /opt/busan/chatbot_company.db:
    vendor_contract_history
    vendor_contract_history_summary

This script is intentionally additive. It does not modify procurement source
tables or the core company master tables.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import re
import sqlite3
from collections import defaultdict
from pathlib import Path


CORP_BLOCK_RE = re.compile(r"\[([^\[\]]+)\]")
BUSINESS_NO_RE = re.compile(r"(?<!\d)(\d{10})(?!\d)")


def normalize_text(value: object) -> str:
    text = str(value or "").lower()
    text = re.sub(r"[\(\)\[\]\{\}<>,.;:'\"`~!@#$%^&*_+=|\\/?:·ㆍ]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def compact_text(value: object) -> str:
    return re.sub(r"\s+", "", normalize_text(value))


def parse_amount(value: object) -> int:
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    if not digits:
        return 0
    try:
        return int(digits)
    except ValueError:
        return 0


def parse_date(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    m = re.search(r"(\d{4})[-./]?(\d{2})[-./]?(\d{2})", text)
    if not m:
        return ""
    year, month, day = map(int, m.groups())
    if not (1990 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31):
        return ""
    try:
        return dt.date(year, month, day).isoformat()
    except ValueError:
        return ""


def parse_corp_list(value: object) -> list[dict[str, object]]:
    text = str(value or "")
    vendors: list[dict[str, object]] = []
    blocks = CORP_BLOCK_RE.findall(text)
    if not blocks and "^" in text:
        blocks = [text]
    for block in blocks:
        parts = [part.strip() for part in block.split("^")]
        if len(parts) < 4:
            continue
        bizno = ""
        for part in reversed(parts):
            m = BUSINESS_NO_RE.search(part)
            if m:
                bizno = m.group(1)
                break
        if not bizno:
            continue
        role = parts[2] if len(parts) > 2 else ""
        company_name = parts[3] if len(parts) > 3 else ""
        share = 0.0
        for part in parts:
            try:
                numeric = float(part)
            except ValueError:
                continue
            if 0 <= numeric <= 100:
                share = numeric
        vendors.append(
            {
                "bizno": bizno,
                "company_name": company_name,
                "role": role,
                "share": share,
            }
        )
    return vendors


def history_id(*parts: object) -> str:
    raw = "||".join(str(part or "") for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def load_identity(conn: sqlite3.Connection) -> dict[str, tuple[int, str, str]]:
    rows = conn.execute(
        """
        SELECT i.canonical_business_no, i.company_internal_id, i.company_id, m.company_name
        FROM company_identity i
        JOIN company_master m ON m.company_internal_id = i.company_internal_id
        WHERE m.is_busan_company = 1
          AND COALESCE(m.data_status, 'active') NOT IN ('inactive', 'closed')
        """
    ).fetchall()
    return {str(row[0]): (int(row[1]), str(row[2]), str(row[3] or "")) for row in rows}


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS vendor_contract_history;
        CREATE TABLE vendor_contract_history (
            history_id TEXT PRIMARY KEY,
            company_internal_id INTEGER NOT NULL,
            company_id TEXT NOT NULL,
            canonical_business_no TEXT NOT NULL,
            company_name TEXT,
            contract_type TEXT NOT NULL,
            contract_name TEXT NOT NULL,
            contract_amount INTEGER NOT NULL DEFAULT 0,
            contract_date TEXT,
            agency_name TEXT,
            product_classification_no TEXT,
            product_classification_name TEXT,
            product_mid_classification_name TEXT,
            product_large_classification_name TEXT,
            source_contract_no TEXT,
            contractor_role TEXT,
            contractor_share REAL,
            search_text TEXT,
            source_name TEXT NOT NULL,
            source_refreshed_at TEXT NOT NULL
        );

        CREATE INDEX idx_vendor_contract_history_company
            ON vendor_contract_history(company_id, contract_type, contract_date);
        CREATE INDEX idx_vendor_contract_history_type_date
            ON vendor_contract_history(contract_type, contract_date);
        CREATE INDEX idx_vendor_contract_history_search
            ON vendor_contract_history(search_text);

        DROP TABLE IF EXISTS vendor_contract_history_summary;
        CREATE TABLE vendor_contract_history_summary (
            company_id TEXT NOT NULL,
            contract_type TEXT NOT NULL,
            recent_contract_count INTEGER NOT NULL DEFAULT 0,
            total_contract_amount INTEGER NOT NULL DEFAULT 0,
            last_contract_date TEXT,
            sample_contract_names TEXT,
            sample_agencies TEXT,
            search_text TEXT,
            source_refreshed_at TEXT NOT NULL,
            PRIMARY KEY(company_id, contract_type)
        );
        """
    )


def source_rows(conn: sqlite3.Connection, table: str, contract_type: str, since_year: int):
    date_expr = "COALESCE(NULLIF(cntrctDate, ''), NULLIF(cntrctCnclsDate, ''), NULLIF(rgstDt, ''))"
    cols = """
        untyCntrctNo, dcsnCntrctNo, cntrctNm, corpList,
        thtmCntrctAmt, totCntrctAmt, cntrctDate, cntrctCnclsDate, rgstDt,
        cntrctInsttNm, pubPrcrmntClsfcNo, pubPrcrmntClsfcNm,
        pubPrcrmntMidClsfcNm, pubPrcrmntLrgClsfcNm
    """
    sql = f"""
        SELECT {cols}
        FROM {table}
        WHERE corpList IS NOT NULL
          AND corpList != ''
          AND substr({date_expr}, 1, 4) >= ?
    """
    for row in conn.execute(sql, (str(since_year),)):
        yield contract_type, row


def build(args: argparse.Namespace) -> dict[str, int]:
    source_db = Path(args.source_db)
    target_db = Path(args.target_db)
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    source = sqlite3.connect(str(source_db))
    target = sqlite3.connect(str(target_db))
    try:
        source.row_factory = sqlite3.Row
        target.row_factory = sqlite3.Row
        identity = load_identity(target)
        ensure_schema(target)
        stats = defaultdict(int)
        insert_rows: list[tuple] = []
        for table, contract_type in (("servc_cntrct", "service"), ("thng_cntrct", "goods")):
            for ctype, row in source_rows(source, table, contract_type, args.since_year):
                stats[f"{ctype}_source"] += 1
                contract_name = str(row["cntrctNm"] or "").strip()
                if not contract_name:
                    continue
                contract_date = parse_date(row["cntrctDate"]) or parse_date(row["cntrctCnclsDate"]) or parse_date(row["rgstDt"])
                amount = parse_amount(row["thtmCntrctAmt"]) or parse_amount(row["totCntrctAmt"])
                class_values = [
                    row["pubPrcrmntClsfcNo"],
                    row["pubPrcrmntClsfcNm"],
                    row["pubPrcrmntMidClsfcNm"],
                    row["pubPrcrmntLrgClsfcNm"],
                ]
                search_text = normalize_text(" ".join(str(x or "") for x in [contract_name, *class_values]))
                source_contract_no = str(row["untyCntrctNo"] or row["dcsnCntrctNo"] or "")
                for vendor in parse_corp_list(row["corpList"]):
                    bizno = str(vendor["bizno"])
                    matched = identity.get(bizno)
                    if not matched:
                        stats[f"{ctype}_non_busan_or_unmatched_vendor"] += 1
                        continue
                    company_internal_id, company_id, master_name = matched
                    stats[f"{ctype}_matched_vendor"] += 1
                    insert_rows.append(
                        (
                            history_id(ctype, source_contract_no, bizno, contract_name, contract_date),
                            company_internal_id,
                            company_id,
                            bizno,
                            str(vendor["company_name"] or master_name),
                            ctype,
                            contract_name,
                            amount,
                            contract_date,
                            str(row["cntrctInsttNm"] or ""),
                            str(row["pubPrcrmntClsfcNo"] or ""),
                            str(row["pubPrcrmntClsfcNm"] or ""),
                            str(row["pubPrcrmntMidClsfcNm"] or ""),
                            str(row["pubPrcrmntLrgClsfcNm"] or ""),
                            source_contract_no,
                            str(vendor["role"] or ""),
                            float(vendor["share"] or 0),
                            search_text,
                            f"{table}_history",
                            now,
                        )
                    )
                    if len(insert_rows) >= args.batch_size:
                        target.executemany(
                            """
                            INSERT OR REPLACE INTO vendor_contract_history (
                                history_id, company_internal_id, company_id, canonical_business_no, company_name,
                                contract_type, contract_name, contract_amount, contract_date, agency_name,
                                product_classification_no, product_classification_name,
                                product_mid_classification_name, product_large_classification_name,
                                source_contract_no, contractor_role, contractor_share, search_text,
                                source_name, source_refreshed_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            insert_rows,
                        )
                        target.commit()
                        insert_rows.clear()
        if insert_rows:
            target.executemany(
                """
                INSERT OR REPLACE INTO vendor_contract_history (
                    history_id, company_internal_id, company_id, canonical_business_no, company_name,
                    contract_type, contract_name, contract_amount, contract_date, agency_name,
                    product_classification_no, product_classification_name,
                    product_mid_classification_name, product_large_classification_name,
                    source_contract_no, contractor_role, contractor_share, search_text,
                    source_name, source_refreshed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                insert_rows,
            )
            target.commit()

        target.execute(
            """
            INSERT INTO vendor_contract_history_summary (
                company_id, contract_type, recent_contract_count, total_contract_amount,
                last_contract_date, sample_contract_names, sample_agencies, search_text, source_refreshed_at
            )
            SELECT
                company_id,
                contract_type,
                COUNT(*) AS recent_contract_count,
                SUM(contract_amount) AS total_contract_amount,
                MAX(contract_date) AS last_contract_date,
                (
                    SELECT GROUP_CONCAT(contract_name, ' | ')
                    FROM (
                        SELECT DISTINCT h2.contract_name
                        FROM vendor_contract_history h2
                        WHERE h2.company_id = h.company_id
                          AND h2.contract_type = h.contract_type
                        ORDER BY h2.contract_date DESC
                        LIMIT 5
                    )
                ) AS sample_contract_names,
                (
                    SELECT GROUP_CONCAT(agency_name, ' | ')
                    FROM (
                        SELECT DISTINCT h3.agency_name
                        FROM vendor_contract_history h3
                        WHERE h3.company_id = h.company_id
                          AND h3.contract_type = h.contract_type
                          AND h3.agency_name != ''
                        ORDER BY h3.contract_date DESC
                        LIMIT 5
                    )
                ) AS sample_agencies,
                GROUP_CONCAT(DISTINCT search_text),
                MAX(source_refreshed_at)
            FROM vendor_contract_history h
            GROUP BY company_id, contract_type
            """
        )
        target.commit()
        for key, value in target.execute("SELECT contract_type, COUNT(*) FROM vendor_contract_history GROUP BY contract_type"):
            stats[f"{key}_history_rows"] = int(value)
        for key, value in target.execute("SELECT contract_type, COUNT(*) FROM vendor_contract_history_summary GROUP BY contract_type"):
            stats[f"{key}_summary_rows"] = int(value)
        return dict(stats)
    finally:
        source.close()
        target.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build service/goods vendor contract history for recommendation")
    parser.add_argument("--source-db", default="/opt/busan/procurement_contracts.db")
    parser.add_argument("--target-db", default="/opt/busan/chatbot_company.db")
    parser.add_argument("--since-year", type=int, default=2021)
    parser.add_argument("--batch-size", type=int, default=5000)
    args = parser.parse_args()
    stats = build(args)
    for key in sorted(stats):
        print(f"{key}={stats[key]}")


if __name__ == "__main__":
    main()
