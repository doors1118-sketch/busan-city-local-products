import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')

tables = conn.execute("SELECT name FROM sqlite_master WHERE name LIKE 'busan_award%'").fetchall()
for t in tables:
    name = t[0]
    cnt = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    print(f"{name}: {cnt:,}건")

conn.close()
