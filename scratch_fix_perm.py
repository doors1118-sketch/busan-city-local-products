import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

commands = [
    "ls -la /opt/busan/.env",
    "ls -la /opt/busan/daily_pipeline_sync.py",
    # busan-monitor 유저가 .env를 읽을 수 있는지
    "sudo -u busan-monitor cat /opt/busan/.env 2>&1",
    # .env 소유자를 busan-monitor로 변경
    "chown busan-monitor:busan-monitor /opt/busan/.env && chmod 644 /opt/busan/.env && ls -la /opt/busan/.env",
    # 재확인
    "sudo -u busan-monitor cat /opt/busan/.env 2>&1",
    # busan-monitor로 파이프라인 테스트 (헬스체크만)
    """sudo -u busan-monitor bash -c 'cd /opt/busan && /opt/busan/venv/bin/python3 -c "
import os,sys
_env_path = os.path.join(os.path.dirname(os.path.abspath(\"daily_pipeline_sync.py\")), \".env\")
print(\"env exists:\", os.path.exists(_env_path))
if os.path.exists(_env_path):
    with open(_env_path) as f:
        for line in f:
            line = line.strip()
            if line and \"=\" in line:
                k,v = line.split(\"=\",1)
                os.environ.setdefault(k,v)
key = os.environ.get(\"SERVICE_KEY\",\"\")
print(\"KEY len:\", len(key))
"'""",
]

for i, cmd in enumerate(commands, 1):
    print(f"\n[{i}] {'='*50}")
    stdin, stdout, stderr = client.exec_command(cmd, timeout=15)
    out = stdout.read().decode('utf-8', 'replace').strip()
    err = stderr.read().decode('utf-8', 'replace').strip()
    if out:
        print(out)
    if err and 'apport' not in err:
        print(f"STDERR: {err}")

client.close()
