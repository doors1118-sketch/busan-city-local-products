import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

commands = [
    # 1. sync_log 테이블 최근 기록 확인
    """cd /opt/busan && python3 -c "
import sqlite3
conn = sqlite3.connect('procurement_contracts.db')
cur = conn.cursor()
try:
    cur.execute('SELECT * FROM sync_log ORDER BY sync_date DESC LIMIT 15')
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    print('COLUMNS:', cols)
    for r in rows:
        print(r)
except Exception as e:
    print('ERROR:', e)
conn.close()
" """,
    # 2. 크론탭 전체
    "crontab -l 2>/dev/null",
    # 3. busan-monitor 크론탭
    "crontab -u busan-monitor -l 2>/dev/null || echo 'no busan-monitor crontab'",
    # 4. 5/23~24 관련 cron 실행 흔적
    "grep -i 'daily_pipeline\\|build_api\\|alert_check' /var/log/syslog 2>/dev/null | grep 'May 2[34]' | tail -30 || echo 'no match in syslog'",
    # 5. daily_pipeline_sync.py 최근 실행 결과 (stdout/stderr 로그)
    "cd /opt/busan && ls -lt /var/log/busan* 2>/dev/null || ls -lt /opt/busan/logs/ 2>/dev/null || echo 'no log dir found'",
    # 6. 최근 alert SMS 문자 내용 (alert.log 마지막 100줄)
    "cd /opt/busan && tail -150 alert_log/alert.log 2>/dev/null",
]

for i, cmd in enumerate(commands, 1):
    print(f"\n{'='*60}")
    print(f"[{i}]")
    print('='*60)
    stdin, stdout, stderr = client.exec_command(cmd, timeout=15)
    out = stdout.read().decode('utf-8', 'replace').strip()
    err = stderr.read().decode('utf-8', 'replace').strip()
    if out:
        print(out)
    if err:
        print(f"STDERR: {err}")

client.close()
