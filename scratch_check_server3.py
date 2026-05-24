import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

commands = [
    # 1. sync_log 최근 15일
    """cd /opt/busan && python3 -c "
import sqlite3
conn = sqlite3.connect('procurement_contracts.db')
cur = conn.cursor()
cur.execute('SELECT sync_date, completed_at, status FROM sync_log ORDER BY sync_date DESC LIMIT 15')
for r in cur.fetchall():
    print(r)
conn.close()
" """,
    # 2. crontab (root)
    "crontab -l 2>/dev/null | head -20",
    # 3. crontab (busan-monitor)
    "crontab -u busan-monitor -l 2>/dev/null | head -20",
    # 4. 5/23~24 크론 실행 흔적
    "journalctl --since '2026-05-22 23:00' --until '2026-05-24 12:00' -t CRON --no-pager 2>/dev/null | grep -i 'daily_pipeline\\|build_api\\|alert' | tail -30",
    # 5. 오늘 alert_log 파일 있는지
    "ls -la /opt/busan/alert_log/alert_202605{23,24}* 2>/dev/null || echo 'NO alert log for 5/23 or 5/24'",
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
