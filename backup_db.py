#!/usr/bin/env python3
"""Create verified SQLite backups and upload them to NCP Object Storage.

The backup is taken through SQLite's online backup API.  A live WAL database
is never copied as a bare main file.  Temporary files are created one database
at a time and guarded by a projected disk-usage ceiling.
"""

from __future__ import annotations

import gzip
import hashlib
import os
import shutil
import sqlite3
import tempfile
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path

try:
    import fcntl
except ImportError:  # Windows-only test environment; production is Linux.
    fcntl = None


DB_DIR = Path(os.environ.get("BUSAN_DB_DIR", "/opt/busan"))
BACKUP_DIR = Path(os.environ.get("BUSAN_BACKUP_DIR", "/opt/busan/backups"))
DB_FILES = (
    "procurement_contracts.db",
    "busan_agencies_master.db",
    "servc_site.db",
    "busan_companies_master.db",
    "procurement_license.db",
)

OPTIONAL_LARGE_DB_FILES = ("chatbot_company.db",)
# The large DB is included by default, but the capacity guard skips it before
# writing if the worst-case snapshot+gzip peak would exceed the ceiling.
INCLUDE_LARGE_DB = os.environ.get("BACKUP_INCLUDE_LARGE_DB", "1") == "1"

LOCAL_RETENTION_DAYS = int(os.environ.get("BACKUP_LOCAL_RETENTION_DAYS", "3"))
OBJECT_STORAGE_RETENTION_DAYS = int(os.environ.get("BACKUP_OBJECT_RETENTION_DAYS", "7"))
MAX_PROJECTED_DISK_PERCENT = float(os.environ.get("BACKUP_MAX_PROJECTED_DISK_PERCENT", "88"))
SAFETY_MARGIN_BYTES = int(os.environ.get("BACKUP_SAFETY_MARGIN_BYTES", str(256 * 1024**2)))
CAPACITY_LOCK = Path(os.environ.get("BACKUP_CAPACITY_LOCK", str(BACKUP_DIR / ".backup.lock")))

NCP_ENDPOINT = "https://kr.object.ncloudstorage.com"
NCP_ACCESS_KEY = os.environ.get("NCP_ACCESS_KEY", "")
NCP_SECRET_KEY = os.environ.get("NCP_SECRET_KEY", "")
BUCKET_NAME = "busan-procurement-backup"

os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")


def has_object_storage_config() -> bool:
    return bool(NCP_ACCESS_KEY and NCP_SECRET_KEY)


def ensure_bucket():
    if not has_object_storage_config():
        raise RuntimeError("NCP_ACCESS_KEY/NCP_SECRET_KEY not configured")
    import boto3
    from botocore.config import Config

    config = Config(
        s3={"addressing_style": "path"},
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )
    client = boto3.client(
        "s3",
        endpoint_url=NCP_ENDPOINT,
        aws_access_key_id=NCP_ACCESS_KEY,
        aws_secret_access_key=NCP_SECRET_KEY,
        config=config,
    )
    client.head_bucket(Bucket=BUCKET_NAME)
    return client


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def object_matches(client, key: str, path: Path) -> bool:
    checksum = sha256_file(path)
    try:
        head = client.head_object(Bucket=BUCKET_NAME, Key=key)
    except Exception:
        return False
    metadata = {str(k).lower(): str(v) for k, v in head.get("Metadata", {}).items()}
    return (
        int(head.get("ContentLength", -1)) == path.stat().st_size
        and metadata.get("sha256") == checksum
    )


def upload_and_verify(client, key: str, path: Path) -> None:
    checksum = sha256_file(path)
    client.upload_file(
        str(path),
        BUCKET_NAME,
        key,
        ExtraArgs={"Metadata": {"sha256": checksum}},
    )
    if not object_matches(client, key, path):
        raise RuntimeError(f"remote size/SHA-256 verification failed: {key}")


def projected_peak_percent(source_size: int, filesystem: Path = BACKUP_DIR) -> float:
    """Worst case: online snapshot + gzip output as large as the source."""
    usage = shutil.disk_usage(filesystem)
    projected_used = usage.used + source_size * 2 + SAFETY_MARGIN_BYTES
    return projected_used * 100 / usage.total


def assert_capacity(source: Path, filesystem: Path = BACKUP_DIR) -> None:
    projected = projected_peak_percent(source.stat().st_size, filesystem)
    if projected >= MAX_PROJECTED_DISK_PERCENT:
        raise RuntimeError(
            f"capacity guard blocked {source.name}: projected peak {projected:.1f}% "
            f">= limit {MAX_PROJECTED_DISK_PERCENT:.1f}%"
        )


def sqlite_quick_check(path: Path) -> None:
    uri = f"file:{path}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=30)) as connection:
        result = connection.execute("PRAGMA quick_check").fetchone()
    if not result or result[0] != "ok":
        raise RuntimeError(f"SQLite quick_check failed for {path.name}: {result!r}")


def atomic_sqlite_gzip_backup(source: Path, destination: Path) -> Path:
    """Back up a live SQLite DB, verify it, gzip it, and publish atomically."""
    assert_capacity(source, destination.parent)
    destination.parent.mkdir(parents=True, exist_ok=True)

    raw_fd, raw_name = tempfile.mkstemp(
        prefix=f".{source.name}.", suffix=".sqlite.tmp", dir=destination.parent
    )
    os.close(raw_fd)
    raw_path = Path(raw_name)
    gzip_path = destination.with_name(f".{destination.name}.tmp-{os.getpid()}")
    try:
        source_uri = f"file:{source}?mode=ro"
        with closing(sqlite3.connect(source_uri, uri=True, timeout=60)) as source_db:
            with closing(sqlite3.connect(raw_path, timeout=60)) as backup_db:
                source_db.backup(backup_db, pages=4096, sleep=0.05)
        sqlite_quick_check(raw_path)

        with raw_path.open("rb") as input_file:
            with gzip.open(gzip_path, "wb", compresslevel=6) as output_file:
                shutil.copyfileobj(input_file, output_file, length=1024 * 1024)
        with gzip.open(gzip_path, "rb") as compressed:
            while compressed.read(1024 * 1024):
                pass
        os.replace(gzip_path, destination)
        return destination
    finally:
        raw_path.unlink(missing_ok=True)
        raw_path.with_name(f"{raw_path.name}-wal").unlink(missing_ok=True)
        raw_path.with_name(f"{raw_path.name}-shm").unlink(missing_ok=True)
        gzip_path.unlink(missing_ok=True)


def acquire_capacity_lock():
    CAPACITY_LOCK.parent.mkdir(parents=True, exist_ok=True)
    handle = CAPACITY_LOCK.open("a+")
    if fcntl is not None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
    return handle


def selected_database_files() -> tuple[str, ...]:
    return DB_FILES + (OPTIONAL_LARGE_DB_FILES if INCLUDE_LARGE_DB else ())


def backup_and_upload() -> bool:
    print(f"DB backup started at {datetime.now():%Y-%m-%d %H:%M}")
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    backup_files: list[Path] = []
    failures: list[str] = []

    lock_handle = acquire_capacity_lock()
    try:
        for database_name in selected_database_files():
            source = DB_DIR / database_name
            if not source.exists():
                print(f"SKIP missing database: {database_name}")
                continue
            destination = BACKUP_DIR / f"{database_name}.{today}.gz"
            try:
                atomic_sqlite_gzip_backup(source, destination)
                print(
                    f"OK local backup: {database_name} "
                    f"source={source.stat().st_size} compressed={destination.stat().st_size}"
                )
                backup_files.append(destination)
            except Exception as exc:
                failures.append(f"{database_name}: {type(exc).__name__}: {exc}")
                print(f"FAIL local backup: {failures[-1]}")
    finally:
        lock_handle.close()

    if not backup_files:
        print("FAIL no database backup was created")
        return False

    client = None
    if has_object_storage_config():
        try:
            client = ensure_bucket()
            for backup_path in backup_files:
                key = f"daily/{backup_path.name}"
                upload_and_verify(client, key, backup_path)
                print(f"OK remote backup: {key}")
        except Exception as exc:
            failures.append(f"Object Storage: {type(exc).__name__}: {exc}")
            print(f"FAIL remote backup: {failures[-1]}")
    else:
        failures.append("Object Storage: credentials not configured")
        print("FAIL remote backup: credentials not configured")

    local_cutoff = datetime.now() - timedelta(days=LOCAL_RETENTION_DAYS)
    for backup_path in BACKUP_DIR.glob("*.gz"):
        if datetime.fromtimestamp(backup_path.stat().st_mtime) >= local_cutoff:
            continue
        key = f"daily/{backup_path.name}"
        if client is not None and object_matches(client, key, backup_path):
            backup_path.unlink()
            print(f"OK local retention delete after remote verification: {backup_path.name}")
        else:
            print(f"KEEP local backup without verified remote copy: {backup_path.name}")

    if client is not None:
        object_cutoff = datetime.now() - timedelta(days=OBJECT_STORAGE_RETENTION_DAYS)
        try:
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix="daily/"):
                for item in page.get("Contents", []):
                    if item["LastModified"].replace(tzinfo=None) < object_cutoff:
                        client.delete_object(Bucket=BUCKET_NAME, Key=item["Key"])
                        print(f"OK remote retention delete: {item['Key']}")
        except Exception as exc:
            failures.append(f"remote retention: {type(exc).__name__}: {exc}")
            print(f"FAIL remote retention: {failures[-1]}")

    print(f"DB backup finished: created={len(backup_files)} failures={len(failures)}")
    return not failures


if __name__ == "__main__":
    raise SystemExit(0 if backup_and_upload() else 1)
