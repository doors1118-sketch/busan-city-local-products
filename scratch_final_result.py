import paramiko, time

time.sleep(180)  # 3분 대기

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# 최종 결과
stdin, stdout, stderr = client.exec_command("tail -20 /opt/busan/sync_log/daily.log", timeout=10)
print("[daily.log 마지막 20줄]")
print(stdout.read().decode('utf-8','replace'))

# sync_log 확인
stdin, stdout, stderr = client.exec_command(
    "cd /opt/busan && /opt/busan/venv/bin/python3 -c \""
    "import sqlite3; "
    "conn = sqlite3.connect('procurement_contracts.db'); "
    "rows = conn.execute('SELECT * FROM sync_log ORDER BY sync_date DESC LIMIT 5').fetchall(); "
    "[print(r) for r in rows]; "
    "conn.close()\"",
    timeout=10
)
print("\n[sync_log]")
print(stdout.read().decode('utf-8','replace'))

# 프로세스 상태
stdin, stdout, stderr = client.exec_command("ps aux | grep daily_pipeline | grep -v grep | wc -l", timeout=10)
cnt = stdout.read().decode('utf-8','replace').strip()
print(f"[실행 중 프로세스] {cnt}개")

client.close()
