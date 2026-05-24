import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

commands = [
    # 1. 실행 중인 파이프라인 프로세스 확인
    "ps aux | grep daily_pipeline | grep -v grep",
    # 2. 모두 종료
    "pkill -f 'daily_pipeline_sync.py' 2>/dev/null; sleep 1; ps aux | grep daily_pipeline | grep -v grep || echo 'all killed'",
    # 3. 최신 코드로 수동 실행 (foreground에서 처음 20줄만 확인)
    "cd /opt/busan && timeout 60 /opt/busan/venv/bin/python3 daily_pipeline_sync.py 20260522 2>&1 | head -30",
]

for i, cmd in enumerate(commands, 1):
    print(f"\n[{i}] {'='*50}")
    stdin, stdout, stderr = client.exec_command(cmd, timeout=90)
    out = stdout.read().decode('utf-8', 'replace').strip()
    err = stderr.read().decode('utf-8', 'replace').strip()
    if out:
        print(out)
    if err and 'apport' not in err:
        print(f"STDERR: {err}")

client.close()
