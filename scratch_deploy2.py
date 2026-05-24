import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# 1. git pull
stdin, stdout, stderr = client.exec_command("cd /opt/busan && git pull origin main --ff-only", timeout=30)
print("[git pull]", stdout.read().decode('utf-8','replace').strip())

# 2. 파이프라인 수동 실행 (환경변수 없이 — .env 자동 로딩 테스트)
stdin, stdout, stderr = client.exec_command(
    "cd /opt/busan && nohup /opt/busan/venv/bin/python3 daily_pipeline_sync.py 20260522 >> /opt/busan/sync_log/daily.log 2>&1 &",
    timeout=10
)
print("[pipeline] 백그라운드 실행 시작")

client.close()
