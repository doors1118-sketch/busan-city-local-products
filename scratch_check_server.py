import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

commands = [
    # 1. sync_log 테이블 최근 10일
    'cd /opt/busan && sqlite3 procurement_contracts.db "SELECT * FROM sync_log ORDER BY sync_date DESC LIMIT 10;"',
    # 2. 크론탭 확인
    'crontab -l 2>/dev/null | grep -v "^#"',
    # 3. daily_pipeline_sync 최근 실행 로그
    'cd /opt/busan && ls -lt *.log 2>/dev/null | head -5',
    # 4. systemd journal - daily pipeline 최근 에러
    'journalctl -u busan-api --since "2 days ago" --no-pager -n 20 2>/dev/null || echo "no journal"',
    # 5. alert_log 최근 파일
    'cd /opt/busan && ls -lt alert_log/ 2>/dev/null | head -10',
    # 6. 최근 alert_log 내용
    'cd /opt/busan && cat alert_log/$(ls -t alert_log/ 2>/dev/null | head -1) 2>/dev/null | tail -50',
    # 7. pipeline 로그 파일 확인
    'cd /opt/busan && tail -100 pipeline.log 2>/dev/null || tail -100 daily_pipeline.log 2>/dev/null || echo "no pipeline log found"',
    # 8. cron 로그에서 daily_pipeline 관련
    'grep -i "daily_pipeline\|busan" /var/log/syslog 2>/dev/null | tail -20 || grep -i "daily_pipeline\|busan" /var/log/cron 2>/dev/null | tail -20 || echo "no cron log"',
]

for i, cmd in enumerate(commands, 1):
    print(f"\n{'='*60}")
    print(f"[{i}] {cmd[:80]}...")
    print('='*60)
    stdin, stdout, stderr = client.exec_command(cmd, timeout=15)
    out = stdout.read().decode('utf-8', 'replace').strip()
    err = stderr.read().decode('utf-8', 'replace').strip()
    if out:
        print(out)
    if err:
        print(f"STDERR: {err}")

client.close()
