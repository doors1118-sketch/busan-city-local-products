#!/usr/bin/env python3
"""Operational regression checks for the Busan monitoring system.

Run on the server:
    /opt/busan/venv/bin/python3 /opt/busan/monitoring_regression_check.py

The checks are intentionally deterministic and local-first:
- SQLite table/view presence and minimum row counts
- cache JSON readability
- systemd service active status
- local HTTP API smoke tests
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent
CHATBOT_DB = ROOT / "chatbot_company.db"
COMPANY_DB = ROOT / "busan_companies_master.db"
API_CACHE = ROOT / "api_cache.json"
MONTHLY_CACHE = ROOT / "monthly_cache.json"
DEFAULT_BASE_URL = "http://127.0.0.1:8000"


CHECKS = []


def record(name: str, ok: bool, detail: str = "", severity: str = "critical") -> None:
    CHECKS.append(
        {
            "name": name,
            "ok": bool(ok),
            "severity": severity,
            "detail": detail,
        }
    )


def table_count(db_path: Path, table: str) -> int:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    finally:
        conn.close()


def check_sqlite_counts() -> None:
    expected = [
        (CHATBOT_DB, "company_master", 46000),
        (CHATBOT_DB, "company_license", 46000),
        (CHATBOT_DB, "certified_product", 6000),
        (CHATBOT_DB, "policy_company_certification", 3000),
        (CHATBOT_DB, "direct_production_certificate", 11000),
        (CHATBOT_DB, "mas_product", 2500),
        (CHATBOT_DB, "shopping_mall_product", 2500),
        (CHATBOT_DB, "ref_sme_competition_product", 600),
        (CHATBOT_DB, "ref_construction_material_product", 300),
        (CHATBOT_DB, "ref_product_required_note", 600),
        (CHATBOT_DB, "ref_eligible_cooperative", 500),
        (CHATBOT_DB, "ref_coop_joint_product", 700),
        (CHATBOT_DB, "product_policy_summary", 4000),
        (COMPANY_DB, "company_master", 46000),
        (COMPANY_DB, "company_industry", 46000),
    ]
    for db_path, table, minimum in expected:
        if not db_path.exists():
            record(f"db.exists:{db_path.name}", False, f"missing {db_path}")
            continue
        try:
            count = table_count(db_path, table)
            record(
                f"db.count:{db_path.name}:{table}",
                count >= minimum,
                f"count={count}, minimum={minimum}",
            )
        except Exception as exc:
            record(f"db.count:{db_path.name}:{table}", False, repr(exc))


def check_product_policy_consistency() -> None:
    queries = [
        (
            "policy.sme_count",
            "SELECT COUNT(*) FROM product_policy_summary WHERE IFNULL(is_sme_competition_product, 0) = 1",
            600,
        ),
        (
            "policy.construction_material_count",
            "SELECT COUNT(*) FROM product_policy_summary WHERE IFNULL(is_construction_material_direct_purchase, 0) = 1",
            300,
        ),
        (
            "policy.busan_coop_related_count",
            "SELECT COUNT(*) FROM product_policy_summary WHERE IFNULL(busan_eligible_coop_count, 0) > 0 OR IFNULL(busan_coop_joint_product_count, 0) > 0",
            40,
        ),
        (
            "policy.direct_production_match_count",
            "SELECT COUNT(*) FROM product_policy_summary WHERE IFNULL(direct_production_valid_supplier_count, 0) > 0",
            400,
        ),
    ]
    conn = sqlite3.connect(f"file:{CHATBOT_DB}?mode=ro", uri=True)
    try:
        for name, query, minimum in queries:
            count = int(conn.execute(query).fetchone()[0])
            record(name, count >= minimum, f"count={count}, minimum={minimum}")
        row = conn.execute(
            """
            SELECT detail_product_code, detail_product_name,
                   is_sme_competition_product,
                   is_construction_material_direct_purchase,
                   direct_production_valid_supplier_count
            FROM product_policy_summary
            WHERE detail_product_code = '3011150501'
            """
        ).fetchone()
        record(
            "policy.sample:ready_mixed_concrete",
            row is not None and int(row[2] or 0) == 1 and int(row[4] or 0) > 0,
            f"row={tuple(row) if row else None}",
        )
    except Exception as exc:
        record("policy.consistency", False, repr(exc))
    finally:
        conn.close()


def check_cache_file(path: Path, minimum_bytes: int) -> None:
    try:
        size = path.stat().st_size
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        generated = data.get("generated_at") if isinstance(data, dict) else None
        record(
            f"cache.json:{path.name}",
            size >= minimum_bytes and isinstance(data, dict),
            f"size={size}, generated_at={generated}",
        )
    except Exception as exc:
        record(f"cache.json:{path.name}", False, repr(exc))


def check_systemd(service: str) -> None:
    try:
        result = subprocess.run(
            ["systemctl", "is-active", service],
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
        status = (result.stdout or result.stderr).strip()
        record(f"systemd:{service}", result.returncode == 0 and status == "active", status)
    except Exception as exc:
        record(f"systemd:{service}", False, repr(exc), severity="warning")


def http_json(base_url: str, path: str, params: dict | None = None) -> tuple[int, dict]:
    url = base_url.rstrip("/") + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as response:
        body = response.read()
        return response.status, json.loads(body.decode("utf-8"))


def check_http(base_url: str) -> None:
    endpoints = [
        ("http.summary", "/api/summary", None),
        ("http.chatbot_health", "/api/chatbot/health", None),
        (
            "http.product_policy_keyword",
            "/api/chatbot/product-policy/search",
            {"keyword": "레미콘", "limit": 5},
        ),
        (
            "http.product_policy_code",
            "/api/chatbot/product-policy/search",
            {"detail_product_code": "3011150501", "limit": 5},
        ),
        ("http.openapi", "/openapi.json", None),
    ]
    for name, path, params in endpoints:
        started = time.time()
        try:
            status, data = http_json(base_url, path, params)
            elapsed_ms = int((time.time() - started) * 1000)
            ok = status == 200 and isinstance(data, dict)
            detail = f"status={status}, elapsed_ms={elapsed_ms}"
            if "product_policy" in name:
                candidates = data.get("candidates") or []
                ok = ok and len(candidates) > 0
                detail += f", candidates={len(candidates)}"
                if name.endswith("_code"):
                    ok = ok and candidates[0].get("detail_product_code") == "3011150501"
                    detail += f", first_code={candidates[0].get('detail_product_code') if candidates else None}"
            if name == "http.chatbot_health":
                ok = ok and data.get("status") == "ok"
                detail += f", app_status={data.get('status')}"
            if name == "http.openapi":
                paths = data.get("paths") or {}
                ok = ok and "/api/chatbot/product-policy/search" in paths
                detail += ", product_policy_in_openapi=" + str("/api/chatbot/product-policy/search" in paths)
            record(name, ok, detail)
        except Exception as exc:
            record(name, False, repr(exc))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--json", action="store_true", help="print JSON result")
    args = parser.parse_args()

    check_sqlite_counts()
    check_product_policy_consistency()
    check_cache_file(API_CACHE, minimum_bytes=1_000_000)
    check_cache_file(MONTHLY_CACHE, minimum_bytes=10_000)
    check_systemd("busan-api.service")
    check_systemd("busan-dashboard.service")
    check_http(args.base_url)

    critical_failures = [c for c in CHECKS if not c["ok"] and c["severity"] == "critical"]
    warning_failures = [c for c in CHECKS if not c["ok"] and c["severity"] == "warning"]
    result = {
        "ok": not critical_failures,
        "critical_failures": len(critical_failures),
        "warning_failures": len(warning_failures),
        "checks": CHECKS,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"ok={result['ok']} critical_failures={len(critical_failures)} warning_failures={len(warning_failures)}")
        for check in CHECKS:
            status = "PASS" if check["ok"] else check["severity"].upper()
            print(f"[{status}] {check['name']} - {check['detail']}")
    return 0 if not critical_failures else 1


if __name__ == "__main__":
    sys.exit(main())
