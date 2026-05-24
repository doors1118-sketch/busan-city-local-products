import paramiko, time

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# Step 1: 기존 프로세스 종료
client.exec_command("pkill -f 'daily_pipeline_sync' 2>/dev/null")
time.sleep(3)

# Step 2: 백그라운드 실행 (임시 로그)
client.exec_command("cd /opt/busan && nohup /opt/busan/venv/bin/python3 daily_pipeline_sync.py 20260522 > /tmp/pipeline_test.log 2>&1 &")
time.sleep(45)

# Step 3: 로그 확인
stdin, stdout, stderr = client.exec_command("head -25 /tmp/pipeline_test.log 2>/dev/null", timeout=10)
print(stdout.read().decode('utf-8', 'replace'))

client.close()
