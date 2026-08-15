import gzip
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


def shutil_usage(total, used, free):
    return type("Usage", (), {"total": total, "used": used, "free": free})()


if __name__ == "__main__":
    unittest.main()
