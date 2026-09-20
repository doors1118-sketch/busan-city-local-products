import gzip
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import backup_db


class BackupDbTests(unittest.TestCase):
    def test_online_backup_is_valid_and_preserves_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.db"
            destination = root / "source.db.20260815.gz"
            connection = sqlite3.connect(source)
            try:
                connection.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY, value TEXT)")
                connection.executemany(
                    "INSERT INTO sample(value) VALUES (?)",
                    [("alpha",), ("beta",), ("gamma",)],
                )
                connection.commit()
            finally:
                connection.close()

            with mock.patch.object(backup_db, "assert_capacity"):
                backup_db.atomic_sqlite_gzip_backup(source, destination)

            self.assertTrue(source.exists())
            self.assertTrue(destination.exists())
            restored = root / "restored.db"
            with gzip.open(destination, "rb") as compressed:
                restored.write_bytes(compressed.read())
            backup_db.sqlite_quick_check(restored)
            connection = sqlite3.connect(restored)
            try:
                values = [row[0] for row in connection.execute("SELECT value FROM sample ORDER BY id")]
            finally:
                connection.close()
            self.assertEqual(values, ["alpha", "beta", "gamma"])

    def test_capacity_guard_blocks_before_writing(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "large.db"
            source.write_bytes(b"x" * 1024)
            usage = shutil_usage(total=10_000, used=8_000, free=2_000)
            with mock.patch("backup_db.shutil.disk_usage", return_value=usage):
                with mock.patch.object(backup_db, "SAFETY_MARGIN_BYTES", 0):
                    with mock.patch.object(backup_db, "MAX_PROJECTED_DISK_PERCENT", 85):
                        with self.assertRaisesRegex(RuntimeError, "capacity guard blocked"):
                            backup_db.assert_capacity(source, Path(directory))

    def test_optional_failure_does_not_mark_core_backup_failed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            backup_dir = root / "backups"
            status_file = root / "sync_log" / "backup_status.json"
            (root / "core.db").write_bytes(b"core")
            (root / "optional.db").write_bytes(b"optional")

            def fake_backup(source, destination):
                if source.name == "optional.db":
                    raise RuntimeError("capacity guard blocked optional.db")
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(b"backup")
                return destination

            with mock.patch.multiple(
                backup_db,
                DB_DIR=root,
                BACKUP_DIR=backup_dir,
                STATUS_FILE=status_file,
                CAPACITY_LOCK=backup_dir / ".backup.lock",
                DB_FILES=("core.db",),
                OPTIONAL_LARGE_DB_FILES=("optional.db",),
                INCLUDE_LARGE_DB=True,
            ), mock.patch.object(
                backup_db, "atomic_sqlite_gzip_backup", side_effect=fake_backup
            ), mock.patch.object(
                backup_db, "has_object_storage_config", return_value=True
            ), mock.patch.object(
                backup_db, "ensure_bucket", return_value=object()
            ), mock.patch.object(backup_db, "upload_and_verify"):
                self.assertTrue(backup_db.backup_and_upload())

            status = json.loads(status_file.read_text(encoding="utf-8"))
            self.assertTrue(status["core"]["ok"])
            self.assertFalse(status["optional"]["ok"])
            self.assertEqual(len(status["optional"]["failures"]), 1)

    def test_core_failure_marks_backup_failed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            backup_dir = root / "backups"
            status_file = root / "sync_log" / "backup_status.json"
            (root / "core.db").write_bytes(b"core")

            with mock.patch.multiple(
                backup_db,
                DB_DIR=root,
                BACKUP_DIR=backup_dir,
                STATUS_FILE=status_file,
                CAPACITY_LOCK=backup_dir / ".backup.lock",
                DB_FILES=("core.db",),
                OPTIONAL_LARGE_DB_FILES=("optional.db",),
                INCLUDE_LARGE_DB=False,
            ), mock.patch.object(
                backup_db,
                "atomic_sqlite_gzip_backup",
                side_effect=RuntimeError("simulated core failure"),
            ), mock.patch.object(
                backup_db, "has_object_storage_config", return_value=True
            ), mock.patch.object(backup_db, "ensure_bucket", return_value=object()):
                self.assertFalse(backup_db.backup_and_upload())

            status = json.loads(status_file.read_text(encoding="utf-8"))
            self.assertFalse(status["core"]["ok"])
            self.assertEqual(len(status["core"]["failures"]), 1)


def shutil_usage(total, used, free):
    return type("Usage", (), {"total": total, "used": used, "free": free})()


if __name__ == "__main__":
    unittest.main()
