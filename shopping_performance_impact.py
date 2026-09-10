#!/usr/bin/env python3
"""Build exact before/after dashboard caches without changing production data.

The corrected cache is calculated by exposing a connection-local TEMP VIEW
that unions production ``shopping_cntrct`` rows with the completed shadow
backfill.  The production database is always opened read-only and is never
copied, vacuumed, or modified.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3

import build_api_cache
from shopping_performance_sync import date_key, inclusive_dates


SHOPPING_TABLE = "shopping_cntrct"
SYNC_TABLE = "shopping_sync_log"
COMPLETE_STATUSES = ("complete", "complete_zero")


class ImpactError(RuntimeError):
    pass


def _resolved(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


def _same_path(value, expected: Path) -> bool:
    try:
        return _resolved(str(value)) == expected
    except (OSError, ValueError):
        return False


class ReadonlyProcurementConnections:
    """sqlite3 proxy that makes every procurement DB connection read-only."""

    def __init__(self, production_db: Path, shadow_db: Path | None = None):
        self.production_db = production_db
        self.shadow_db = shadow_db
        self._sqlite3 = sqlite3

    def __getattr__(self, name):
        return getattr(self._sqlite3, name)

    def connect(self, database, *args, **kwargs):
        if not _same_path(database, self.production_db):
            return self._sqlite3.connect(database, *args, **kwargs)

        options = dict(kwargs)
        options["uri"] = True
        conn = self._sqlite3.connect(
            f"file:{self.production_db.as_posix()}?mode=ro", *args, **options
        )
        if self.shadow_db is not None:
            conn.execute(
                "ATTACH DATABASE ? AS shopping_shadow",
                (f"file:{self.shadow_db.as_posix()}?mode=ro",),
            )
            conn.execute(
                f'CREATE TEMP VIEW "{SHOPPING_TABLE}" AS '
                f'SELECT * FROM main."{SHOPPING_TABLE}" '
                f'UNION ALL SELECT * FROM shopping_shadow."{SHOPPING_TABLE}"'
            )
        return conn


def _quick_check(path: Path) -> str:
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    try:
        return str(conn.execute("PRAGMA quick_check").fetchone()[0])
    finally:
        conn.close()


def _production_dates(conn: sqlite3.Connection) -> set[str]:
    result = set()
    for (value,) in conn.execute(
        f'SELECT DISTINCT "dlvrReqRcptDate" FROM "{SHOPPING_TABLE}" '
        'WHERE "dlvrReqRcptDate" IS NOT NULL'
    ):
        normalized = date_key(value)
        if normalized:
            result.add(normalized)
    return result


def validate_backfill_scope(
    production_db: Path, shadow_db: Path, start: str, end: str
) -> dict:
    """Require every production-empty date in the requested range to be complete."""
    prod = sqlite3.connect(f"file:{production_db.as_posix()}?mode=ro", uri=True)
    shadow = sqlite3.connect(f"file:{shadow_db.as_posix()}?mode=ro", uri=True)
    try:
        production_dates = _production_dates(prod)
        status_rows = shadow.execute(
            f"""SELECT target_date, status, source_total, source_received,
                       busan_rows, stored_rows, request_count
                FROM {SYNC_TABLE}"""
        ).fetchall()
        statuses = {str(row[0]): row for row in status_rows}
        missing_dates = [
            value
            for value in inclusive_dates(start, end)
            if value not in production_dates
        ]
        unresolved = [
            value
            for value in missing_dates
            if value not in statuses or statuses[value][1] not in COMPLETE_STATUSES
        ]
        incomplete = [
            value
            for value in missing_dates
            if value in statuses
            and statuses[value][1] in COMPLETE_STATUSES
            and int(statuses[value][2] or 0) != int(statuses[value][3] or 0)
        ]
        summary_rows = [
            statuses[value]
            for value in missing_dates
            if value in statuses and statuses[value][1] in COMPLETE_STATUSES
        ]
        result = {
            "start": start,
            "end": end,
            "production_present_days": sum(
                1 for value in inclusive_dates(start, end) if value in production_dates
            ),
            "production_missing_days": len(missing_dates),
            "complete_missing_days": len(summary_rows),
            "unresolved_dates": unresolved,
            "incomplete_dates": incomplete,
            "source_total": sum(int(row[2] or 0) for row in summary_rows),
            "source_received": sum(int(row[3] or 0) for row in summary_rows),
            "busan_rows": sum(int(row[4] or 0) for row in summary_rows),
            "stored_rows": sum(int(row[5] or 0) for row in summary_rows),
            "request_count": sum(int(row[6] or 0) for row in summary_rows),
        }
        if unresolved or incomplete:
            raise ImpactError(
                "backfill is not complete: "
                f"unresolved={len(unresolved)}, incomplete={len(incomplete)}"
            )
        return result
    finally:
        prod.close()
        shadow.close()


def _build_cache(
    *,
    production_db: Path,
    agency_db: Path,
    company_db: Path,
    output_path: Path,
    shadow_db: Path | None,
) -> str:
    original_sqlite3 = build_api_cache.sqlite3
    original_procurement = build_api_cache.DB_PROCUREMENT
    original_agencies = build_api_cache.DB_AGENCIES
    original_companies = build_api_cache.DB_COMPANIES
    original_cache = build_api_cache.CACHE_FILE
    original_cwd = Path.cwd()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    captured = io.StringIO()
    try:
        build_api_cache.sqlite3 = ReadonlyProcurementConnections(
            production_db, shadow_db
        )
        build_api_cache.DB_PROCUREMENT = str(production_db)
        build_api_cache.DB_AGENCIES = str(agency_db)
        build_api_cache.DB_COMPANIES = str(company_db)
        build_api_cache.CACHE_FILE = str(output_path)
        # core_calc.py intentionally loads operational override files by
        # relative path. Always reproduce the production cache working
        # directory so an SSH caller's home directory cannot change results.
        os.chdir(production_db.parent)
        with contextlib.redirect_stdout(captured):
            build_api_cache.build_cache()
    finally:
        build_api_cache.sqlite3 = original_sqlite3
        build_api_cache.DB_PROCUREMENT = original_procurement
        build_api_cache.DB_AGENCIES = original_agencies
        build_api_cache.DB_COMPANIES = original_companies
        build_api_cache.CACHE_FILE = original_cache
        try:
            os.chdir(original_cwd)
        except OSError:
            # sudo may inherit a caller directory that the target service
            # account cannot traverse. Results remain valid in the production
            # working directory; global module state has already been restored.
            pass
    if not output_path.exists():
        raise ImpactError(f"cache was not created: {output_path}")
    return captured.getvalue()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _metric(cache: dict, section: str, sector: str | None = None) -> dict:
    value = cache[section] if sector is None else cache[section][sector]
    return {
        "order_amount": int(value["발주액"]),
        "local_amount": int(value["수주액"]),
        "rate_percent": float(value["수주율"]),
    }


def _delta(before: dict, after: dict) -> dict:
    return {
        "order_amount": after["order_amount"] - before["order_amount"],
        "local_amount": after["local_amount"] - before["local_amount"],
        "rate_percentage_points": round(
            after["rate_percent"] - before["rate_percent"], 1
        ),
    }


def build_impact_report(args) -> dict:
    production_db = _resolved(args.production_db)
    agency_db = _resolved(args.agency_db)
    company_db = _resolved(args.company_db)
    shadow_db = _resolved(args.shadow_db)
    output_dir = _resolved(args.output_dir)
    for path in (production_db, agency_db, company_db, shadow_db):
        if not path.is_file():
            raise ImpactError(f"required file is missing: {path}")

    before_stat = production_db.stat()
    checks = {
        "production_quick_check": _quick_check(production_db),
        "shadow_quick_check": _quick_check(shadow_db),
    }
    if set(checks.values()) != {"ok"}:
        raise ImpactError(f"database integrity check failed: {checks}")
    scope = validate_backfill_scope(
        production_db, shadow_db, args.start, args.end
    )

    baseline_path = output_dir / "api_cache_baseline.json"
    corrected_path = output_dir / "api_cache_with_shopping_backfill.json"
    baseline_log = _build_cache(
        production_db=production_db,
        agency_db=agency_db,
        company_db=company_db,
        output_path=baseline_path,
        shadow_db=None,
    )
    corrected_log = _build_cache(
        production_db=production_db,
        agency_db=agency_db,
        company_db=company_db,
        output_path=corrected_path,
        shadow_db=shadow_db,
    )

    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    corrected = json.loads(corrected_path.read_text(encoding="utf-8"))
    baseline_overall = _metric(baseline, "1_전체")
    corrected_overall = _metric(corrected, "1_전체")
    baseline_shopping = _metric(baseline, "2_분야별", "쇼핑몰")
    corrected_shopping = _metric(corrected, "2_분야별", "쇼핑몰")

    after_stat = production_db.stat()
    production_unchanged = (
        before_stat.st_size == after_stat.st_size
        and before_stat.st_mtime_ns == after_stat.st_mtime_ns
    )
    if not production_unchanged:
        raise ImpactError("production DB size or mtime changed during read-only analysis")

    report = {
        "mode": "read_only_exact_before_after",
        "scope": scope,
        "integrity": checks,
        "production_unchanged": production_unchanged,
        "overall": {
            "before": baseline_overall,
            "after": corrected_overall,
            "delta": _delta(baseline_overall, corrected_overall),
        },
        "shopping": {
            "before": baseline_shopping,
            "after": corrected_shopping,
            "delta": _delta(baseline_shopping, corrected_shopping),
        },
        "artifacts": {
            "baseline_cache": str(baseline_path),
            "baseline_sha256": _sha256(baseline_path),
            "corrected_cache": str(corrected_path),
            "corrected_sha256": _sha256(corrected_path),
        },
        "build_log_tail": {
            "baseline": baseline_log.splitlines()[-8:],
            "corrected": corrected_log.splitlines()[-8:],
        },
    }
    report_path = output_dir / "shopping_performance_impact.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--production-db", default="procurement_contracts.db")
    parser.add_argument("--agency-db", default="busan_agencies_master.db")
    parser.add_argument("--company-db", default="busan_companies_master.db")
    parser.add_argument("--shadow-db", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    report = build_impact_report(args)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ImpactError as exc:
        print(f"FATAL: {exc}")
        raise SystemExit(2)
