import paramiko, time

time.sleep(120)  # 2분 대기

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# 프로세스 상태
stdin, stdout, stderr = client.exec_command("ps aux | grep 'daily_pipeline_sync.py 20260522' | grep -v grep | wc -l", timeout=10)
cnt = stdout.read().decode('utf-8','replace').strip()
print(f"[프로세스 수] {cnt}")

# 임시 로그 전체
stdin, stdout, stderr = client.exec_command("wc -c /tmp/pipeline_test.log && head -50 /tmp/pipeline_test.log", timeout=10)
print("\n[/tmp/pipeline_test.log]")
print(stdout.read().decode('utf-8','replace'))

# sync_log DB 마지막 레코드
stdin, stdout, stderr = client.exec_command(
    "cd /opt/busan && /opt/busan/venv/bin/python3 -c \""
    "import sqlite3; "
    "conn = sqlite3.connect('procurement_contracts.db'); "
    "rows = conn.execute('SELECT * FROM sync_log ORDER BY rowid DESC LIMIT 5').fetchall(); "
    "[print(r) for r in rows]; "
    "conn.close()\"",
    timeout=10
)
print("\n[sync_log]")
print(stdout.read().decode('utf-8','replace'))

client.close()
