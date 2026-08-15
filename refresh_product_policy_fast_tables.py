#!/usr/bin/env python3
"""Refresh materialized product-policy search tables in chatbot_company.db.

The canonical objects, product_policy_summary and
pps_shopping_mall_item_policy_summary, are views. They are appropriate as the
source of truth but can be too slow for interactive dashboard queries because
SQLite recomputes their aggregate joins on every search. This utility snapshots
those views into *_fast tables and adds lightweight indexes. The advisor API
uses the fast tables when present and falls back to the views for older DBs.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path


DEFAULT_DB = Path("/opt/busan/chatbot_company.db")


TABLES = [
    {
        "source": "product_policy_summary",
        "target": "product_policy_summary_fast",
        "indexes": [
            ("idx_product_policy_summary_fast_code", "detail_product_code"),
            ("idx_product_policy_summary_fast_name", "detail_product_name"),
        ],
    },
    {
        "source": "pps_shopping_mall_item_policy_summary",
        "target": "pps_shopping_mall_item_policy_summary_fast",
        "indexes": [
            ("idx_pps_shopping_mall_item_policy_fast_code", "detail_product_code"),
            ("idx_pps_shopping_mall_item_policy_fast_name", "detail_product_name"),
            ("idx_pps_shopping_mall_item_policy_fast_class", "product_class_name"),
        ],
    },
]


def object_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name = ? AND type IN ('table', 'view') LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def refresh_table(conn: sqlite3.Connection, source: str, target: str, indexes: list[tuple[str, str]]) -> dict[str, object]:
    if not object_exists(conn, source):
        return {"source": source, "target": target, "status": "source_missing", "rows": 0}

    tmp = f"{target}__tmp"
    q_tmp = quote_ident(tmp)
    q_target = quote_ident(target)
    q_source = quote_ident(source)

    conn.execute(f"DROP TABLE IF EXISTS {q_tmp}")
    conn.execute(f"CREATE TABLE {q_tmp} AS SELECT * FROM {q_source}")
    rows = conn.execute(f"SELECT COUNT(*) FROM {q_tmp}").fetchone()[0]

    columns = {row[1] for row in conn.execute(f"PRAGMA table_info({q_tmp})").fetchall()}
    conn.execute(f"DROP TABLE IF EXISTS {q_target}")
    conn.execute(f"ALTER TABLE {q_tmp} RENAME TO {q_target}")

    for index_name, column in indexes:
        if column not in columns:
            continue
        conn.execute(f"DROP INDEX IF EXISTS {quote_ident(index_name)}")
        conn.execute(f"CREATE INDEX {quote_ident(index_name)} ON {q_target}({quote_ident(column)})")

    return {"source": source, "target": target, "status": "success", "rows": rows}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DEFAULT_DB), help="chatbot_company.db path")
    args = parser.parse_args()

    db_path = Path(args.db)
    started = time.time()
    if not db_path.exists():
        print(json.dumps({"status": "db_missing", "db": str(db_path)}, ensure_ascii=False))
        return 2

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        results = []
        with conn:
            for spec in TABLES:
                results.append(refresh_table(conn, spec["source"], spec["target"], spec["indexes"]))
        try:
            conn.execute("PRAGMA optimize")
        except sqlite3.DatabaseError:
            pass
    finally:
        conn.close()

    payload = {
        "status": "success" if all(item["status"] == "success" for item in results) else "partial",
        "db": str(db_path),
        "elapsed_sec": round(time.time() - started, 3),
        "tables": results,
    }
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if payload["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
