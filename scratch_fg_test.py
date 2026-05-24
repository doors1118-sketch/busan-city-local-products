import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# 최소한의 파이프라인 테스트: 기존 프로세스 없는 상태에서 foreground 실행
# head -15로 헬스체크 결과만 확인
cmd = "cd /opt/busan && pkill -f 'daily_pipeline_sync' 2>/dev/null; sleep 2; timeout 30 /opt/busan/venv/bin/python3 daily_pipeline_sync.py 20260522 2>&1 | head -15"

stdin, stdout, stderr = client.exec_command(cmd, timeout=60)
out = stdout.read().decode('utf-8', 'replace').strip()
err = stderr.read().decode('utf-8', 'replace').strip()
print("OUTPUT:")
print(out)
if err:
    print(f"\nSTDERR: {err}")

client.close()
