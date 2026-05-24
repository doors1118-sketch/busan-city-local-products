import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# 파이프라인을 임시 로그로 실행 후 결과 확인
cmd = """cd /opt/busan && pkill -f 'daily_pipeline_sync' 2>/dev/null
sleep 2
/opt/busan/venv/bin/python3 daily_pipeline_sync.py 20260522 > /tmp/pipeline_test.log 2>&1 &
PIPELINE_PID=$!
echo "PID: $PIPELINE_PID"
sleep 45
head -20 /tmp/pipeline_test.log
"""

stdin, stdout, stderr = client.exec_command(cmd, timeout=90)
out = stdout.read().decode('utf-8', 'replace').strip()
print(out)
err = stderr.read().decode('utf-8', 'replace').strip()
if err:
    print(f"STDERR: {err}")

client.close()
