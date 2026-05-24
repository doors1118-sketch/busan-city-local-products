import paramiko, time

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# 1. 현재 파이프라인 프로세스
stdin, stdout, stderr = client.exec_command("ps aux | grep daily_pipeline | grep -v grep", timeout=10)
print("[프로세스]", stdout.read().decode('utf-8','replace').strip() or "없음")

# 2. /tmp/pipeline_test.log
stdin, stdout, stderr = client.exec_command("ls -la /tmp/pipeline_test.log 2>/dev/null && cat /tmp/pipeline_test.log 2>/dev/null | head -30", timeout=10)
print("[/tmp/pipeline_test.log]", stdout.read().decode('utf-8','replace').strip() or "파일 없음")

# 3. daily.log 마지막 부분 (가장 최근 실행)
stdin, stdout, stderr = client.exec_command("tail -5 /opt/busan/sync_log/daily.log 2>/dev/null", timeout=10)
print("[daily.log tail]", stdout.read().decode('utf-8','replace').strip())

# 4. 직접 간단한 확인: 파이프라인 import 성공하는지
stdin, stdout, stderr = client.exec_command(
    "cd /opt/busan && /opt/busan/venv/bin/python3 -c 'import daily_pipeline_sync; print(\"KEY:\", len(daily_pipeline_sync.SERVICE_KEY))' 2>&1",
    timeout=30
)
print("[import test]", stdout.read().decode('utf-8','replace').strip())

client.close()
