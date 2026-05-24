import paramiko, time

time.sleep(60)

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# 1. 프로세스 상태
stdin, stdout, stderr = client.exec_command("ps aux | grep daily_pipeline | grep -v grep", timeout=10)
print("[process]", stdout.read().decode('utf-8','replace').strip() or "종료됨")

# 2. 임시 로그
stdin, stdout, stderr = client.exec_command("wc -l /tmp/pipeline_test.log && cat /tmp/pipeline_test.log | head -30", timeout=10)
print("\n[/tmp/pipeline_test.log]")
print(stdout.read().decode('utf-8','replace'))

# 3. daily.log 마지막 30줄
stdin, stdout, stderr = client.exec_command("tail -30 /opt/busan/sync_log/daily.log", timeout=10)
print("\n[daily.log tail -30]")
print(stdout.read().decode('utf-8','replace'))

client.close()
