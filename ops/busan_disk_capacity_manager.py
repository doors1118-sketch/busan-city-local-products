#!/usr/bin/env python3
"""Threshold-based local backup cleanup and verified NCP archival.

Only explicitly listed backup roots are eligible. Active databases, source
files, and the current/previous advisor company-cache targets are never moved.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT_FS = Path("/")
STATE_PATH = Path("/var/lib/busan-capacity-manager/state.json")
ENDPOINT = os.environ.get("NCP_ENDPOINT", "https://kr.object.ncloudstorage.com")
BUCKET = os.environ.get("CAPACITY_ARCHIVE_BUCKET", "busan-procurement-backup")
PREFIX = os.environ.get("CAPACITY_ARCHIVE_PREFIX", "capacity-archive").strip("/")
REMOTE_RETENTION_DAYS = int(os.environ.get("CAPACITY_REMOTE_RETENTION_DAYS", "180"))


@dataclass(frozen=True)
class Policy:
    tag: str
    root: Path
    normal_days: int
    urgent_days: int
    ignored_prefixes: tuple[str, ...] = ()


POLICIES = (
    Policy("busan-deploy", Path("/opt/busan/deploy_backups"), 30, 14),
    Policy("advisor-backup", Path("/opt/advisor/backups"), 30, 14),
    Policy("advisor-deploy", Path("/opt/advisor/deploy_backups"), 30, 14),
    Policy(
        "credit-guarantee-manual",
        Path("/var/backups/credit-guarantee-dashboard"),
        14,
        7,
        ("manifest-",),
    ),
)
SENSITIVE_NAME_MARKERS = (
    ".env",
    "env.",
    "alert_config",
    "secret",
    "credential",
    "authorized_keys",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def disk_percent() -> float:
    stats = os.statvfs(ROOT_FS)
    used = (stats.f_blocks - stats.f_bfree) * stats.f_frsize
    available = stats.f_bavail * stats.f_frsize
    return round(used * 100 / (used + available), 1)


def path_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def make_client():
    access_key = os.environ.get("NCP_ACCESS_KEY", "").strip()
    secret_key = os.environ.get("NCP_SECRET_KEY", "").strip()
    if not access_key or not secret_key:
        raise RuntimeError("NCP_ACCESS_KEY/NCP_SECRET_KEY are not configured")

    os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
    os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        region_name="kr-standard",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def object_matches(client, key: str, path: Path, checksum: str) -> bool:
    head = client.head_object(Bucket=BUCKET, Key=key)
    metadata = {str(k).lower(): str(v) for k, v in head.get("Metadata", {}).items()}
    return (
        int(head.get("ContentLength", -1)) == path.stat().st_size
        and metadata.get("sha256") == checksum
    )


def upload_verified(client, key: str, path: Path, source: Path) -> None:
    checksum = sha256_file(path)
    client.upload_file(
        str(path),
        BUCKET,
        key,
        ExtraArgs={
            "Metadata": {
                "sha256": checksum,
                "source-path-sha256": hashlib.sha256(str(source).encode()).hexdigest(),
            }
        },
    )
    if not object_matches(client, key, path, checksum):
        raise RuntimeError(f"remote verification failed: s3://{BUCKET}/{key}")


def archive_name(policy: Policy, source: Path) -> str:
    suffix = source.name if source.is_file() else f"{source.name}.tar.gz"
    day = datetime.fromtimestamp(source.stat().st_mtime).strftime("%Y/%m/%d")
    return f"{PREFIX}/{policy.tag}/{day}/{suffix}"


def prepare_upload(source: Path):
    if source.is_file():
        return source, None
    temp = tempfile.NamedTemporaryFile(
        prefix=f"capacity-{source.name}-", suffix=".tar.gz", dir="/var/tmp", delete=False
    )
    temp_path = Path(temp.name)
    temp.close()
    try:
        with tarfile.open(temp_path, "w:gz") as archive:
            archive.add(source, arcname=source.name, recursive=True)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    return temp_path, temp_path


def delete_source(source: Path) -> None:
    if source.is_dir():
        shutil.rmtree(source)
    else:
        source.unlink()


def candidates(policy: Policy, age_days: int) -> list[Path]:
    if not policy.root.is_dir():
        return []
    cutoff = datetime.now().timestamp() - age_days * 86400
    result = []
    for path in policy.root.iterdir():
        lowered = path.name.lower()
        if any(marker in lowered for marker in SENSITIVE_NAME_MARKERS):
            continue
        if path.is_dir() and any(
            any(marker in item.name.lower() for marker in SENSITIVE_NAME_MARKERS)
            for item in path.rglob("*")
        ):
            continue
        if path.name.startswith(policy.ignored_prefixes):
            continue
        if policy.tag == "credit-guarantee-manual" and re.fullmatch(
            r"dashboard-\d{8}T\d{6}\.sqlite3\.gz", path.name
        ):
            continue
        if path.is_symlink() or path.stat().st_mtime >= cutoff:
            continue
        result.append(path)
    return sorted(result, key=lambda p: p.stat().st_mtime)


def run_pruners(dry_run: bool) -> list[dict]:
    commands = [
        ["/opt/advisor/scripts/prune_company_cache_archives.py"],
        ["/opt/busan/prune_busan_backups.py"],
    ]
    results = []
    for command in commands:
        if not Path(command[0]).is_file():
            results.append({"command": command[0], "status": "missing"})
            continue
        actual = command + (["--dry-run"] if dry_run else [])
        completed = subprocess.run(actual, text=True, capture_output=True, timeout=300)
        results.append(
            {
                "command": " ".join(actual),
                "returncode": completed.returncode,
                "stdout": completed.stdout[-2000:],
                "stderr": completed.stderr[-2000:],
            }
        )
    return results


def prune_remote(client) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=REMOTE_RETENTION_DAYS)
    deleted = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f"{PREFIX}/"):
        for item in page.get("Contents", []):
            if item["LastModified"] < cutoff:
                client.delete_object(Bucket=BUCKET, Key=item["Key"])
                deleted += 1
    return deleted


def probe(client) -> dict:
    key = f"{PREFIX}/health/probe-{datetime.now().strftime('%Y%m%dT%H%M%S')}.txt"
    body = b"busan-capacity-manager-probe\n"
    checksum = hashlib.sha256(body).hexdigest()
    client.put_object(Bucket=BUCKET, Key=key, Body=body, Metadata={"sha256": checksum})
    head = client.head_object(Bucket=BUCKET, Key=key)
    ok = int(head.get("ContentLength", -1)) == len(body)
    client.delete_object(Bucket=BUCKET, Key=key)
    if not ok:
        raise RuntimeError("Object Storage probe size mismatch")
    return {"bucket": BUCKET, "key": key, "verified": True, "deleted_after_probe": True}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force-level", choices=("normal", "archive", "urgent"))
    parser.add_argument("--probe", action="store_true")
    parser.add_argument("--normal-threshold", type=float, default=80.0)
    parser.add_argument("--archive-threshold", type=float, default=85.0)
    parser.add_argument("--urgent-threshold", type=float, default=90.0)
    parser.add_argument("--max-gib", type=float, default=8.0)
    parser.add_argument("--max-items", type=int, default=50)
    args = parser.parse_args()

    before = disk_percent()
    if args.force_level:
        level = args.force_level
    elif before >= args.urgent_threshold:
        level = "urgent"
    elif before >= args.archive_threshold:
        level = "archive"
    elif before >= args.normal_threshold:
        level = "normal"
    else:
        level = "ok"

    report = {
        "timestamp": datetime.now().astimezone().isoformat(),
        "disk_before_percent": before,
        "level": level,
        "dry_run": args.dry_run,
        "pruners": [],
        "archives": [],
        "errors": [],
    }

    client = None
    try:
        if args.probe:
            client = make_client()
            client.head_bucket(Bucket=BUCKET)
            report["probe"] = probe(client)

        if level in {"normal", "archive", "urgent"}:
            report["pruners"] = run_pruners(args.dry_run)

        if level in {"archive", "urgent"}:
            client = client or make_client()
            client.head_bucket(Bucket=BUCKET)
            limit = int(args.max_gib * 1024**3)
            handled = 0
            handled_items = 0
            queue = []
            for policy in POLICIES:
                age_days = policy.urgent_days if level == "urgent" else policy.normal_days
                for source in candidates(policy, age_days):
                    queue.append((path_size(source), policy, source, age_days))
            for size, policy, source, age_days in sorted(queue, key=lambda row: row[0], reverse=True):
                if handled_items >= args.max_items:
                    break
                if handled + size > limit:
                    continue
                entry = {
                    "source": str(source),
                    "source_bytes": size,
                    "age_days_policy": age_days,
                    "key": archive_name(policy, source),
                }
                if args.dry_run:
                    entry["status"] = "would_archive"
                    handled += size
                    handled_items += 1
                else:
                    upload_path, temporary = prepare_upload(source)
                    try:
                        upload_verified(client, entry["key"], upload_path, source)
                        entry["uploaded_bytes"] = upload_path.stat().st_size
                        delete_source(source)
                        entry["status"] = "verified_and_local_deleted"
                        handled += size
                        handled_items += 1
                    finally:
                        if temporary:
                            temporary.unlink(missing_ok=True)
                report["archives"].append(entry)
            if not args.dry_run:
                report["remote_deleted"] = prune_remote(client)
    except Exception as exc:
        report["errors"].append(f"{type(exc).__name__}: {exc}")

    report["disk_after_percent"] = disk_percent()
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if report["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
