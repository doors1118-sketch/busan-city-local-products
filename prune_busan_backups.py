#!/usr/bin/env python3
"""Prune local Busan monitoring backups with conservative operational rules.

Policy as of 2026-07-06:
- Keep the latest N compressed daily DB backups by filename date.
- Keep manual_contracts audit backups.
- Keep the newest chatbot_company full-copy backup; delete older full-copy DB
  snapshots because chatbot_company.db is rebuilt from upstream sources.
- Delete older transient *.test.db files after a grace period.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


DATE_RE = re.compile(r"(20\d{6})")


def parse_date(path: Path) -> str:
    match = DATE_RE.search(str(path))
    return match.group(1) if match else ""


def file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return 0


def safe_delete(path: Path, root: Path, dry_run: bool) -> None:
    resolved = path.resolve()
    if root not in resolved.parents:
        raise RuntimeError(f"refuse to delete outside root: {resolved}")
    if not dry_run:
        resolved.unlink()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backup-root", default="/opt/busan/backups")
    parser.add_argument("--keep-daily", type=int, default=7)
    parser.add_argument("--keep-chatbot-full", type=int, default=1)
    parser.add_argument("--delete-transient-days", type=int, default=14)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    root = Path(args.backup_root).resolve()
    if not root.is_dir():
        raise SystemExit(f"backup root not found: {root}")

    now = datetime.now(timezone.utc)
    delete: list[Path] = []
    keep: list[str] = []
    reason: dict[str, str] = {}

    daily_groups: dict[str, list[Path]] = defaultdict(list)
    for path in root.glob("*.db.*.gz"):
        daily_groups[path.name.split(".")[0]].append(path)

    for _prefix, files in daily_groups.items():
        files_sorted = sorted(files, key=lambda p: parse_date(p), reverse=True)
        for idx, path in enumerate(files_sorted):
            if idx < args.keep_daily:
                keep.append(str(path))
            else:
                delete.append(path)
                reason[str(path)] = f"older than latest {args.keep_daily} daily backups"

    chatbot_patterns = [
        "chatbot_company*.db",
        "chatbot_company*.db.before",
        "*/chatbot_company*.db",
        "*/chatbot_company*.db.before",
    ]
    chatbot_files: dict[Path, None] = {}
    for pattern in chatbot_patterns:
        for path in root.glob(pattern):
            if not path.is_file():
                continue
            if "manual_contracts" in path.parts:
                keep.append(str(path))
                continue
            chatbot_files[path] = None

    chatbot_sorted = sorted(
        chatbot_files.keys(),
        key=lambda p: (parse_date(p), p.stat().st_mtime),
        reverse=True,
    )
    for idx, path in enumerate(chatbot_sorted):
        if idx < args.keep_chatbot_full:
            keep.append(str(path))
        else:
            delete.append(path)
            reason[str(path)] = f"older chatbot_company full-copy backup; keeping latest {args.keep_chatbot_full}"

    cutoff = now - timedelta(days=args.delete_transient_days)
    for path in root.glob("**/*.test.db"):
        if not path.is_file():
            continue
        mtime = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
        if mtime < cutoff:
            delete.append(path)
            reason[str(path)] = f"transient test DB older than {args.delete_transient_days} days"
        else:
            keep.append(str(path))

    unique_delete: list[Path] = []
    seen: set[Path] = set()
    for path in delete:
        if path in seen or not path.exists():
            continue
        seen.add(path)
        unique_delete.append(path)

    bytes_to_free = sum(file_size(p) for p in unique_delete)
    deleted: list[str] = []
    for path in unique_delete:
        deleted.append(str(path))
        safe_delete(path, root, args.dry_run)

    print(
        json.dumps(
            {
                "status": "dry_run" if args.dry_run else "success",
                "backup_root": str(root),
                "policy": {
                    "keep_daily": args.keep_daily,
                    "keep_chatbot_full": args.keep_chatbot_full,
                    "delete_transient_days": args.delete_transient_days,
                },
                "bytes_to_free": bytes_to_free,
                "deleted": deleted,
                "reasons": {str(p): reason.get(str(p), "") for p in unique_delete},
                "kept_sample_count": len(keep),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
