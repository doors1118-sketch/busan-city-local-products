import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

commands = [
    # 1. daily.log 마지막 200줄 (5/23~24 파이프라인 실행 결과)
    "tail -200 /opt/busan/sync_log/daily.log 2>/dev/null",
    # 2. cache_build.log 마지막 50줄
    "tail -50 /opt/busan/sync_log/cache_build.log 2>/dev/null",
    # 3. sync_log 테이블 스키마 + 최근 데이터
    """cd /opt/busan && python3 -c "
import sqlite3
conn = sqlite3.connect('procurement_contracts.db')
cur = conn.cursor()
cur.execute('PRAGMA table_info(sync_log)')
print('SCHEMA:', cur.fetchall())
cur.execute('SELECT * FROM sync_log ORDER BY sync_date DESC LIMIT 10')
for r in cur.fetchall():
    print(r)
conn.close()
" """,
]

for i, cmd in enumerate(commands, 1):
    print(f"\n[{i}] {'='*50}")
    stdin, stdout, stderr = client.exec_command(cmd, timeout=15)
    out = stdout.read().decode('utf-8', 'replace').strip()
    err = stderr.read().decode('utf-8', 'replace').strip()
    if out:
        print(out)
    if err:
        print(f"STDERR: {err}")

client.close()
