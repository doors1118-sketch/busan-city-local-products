import paramiko, time

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# 기존 프로세스 전부 종료
client.exec_command("pkill -9 -f 'daily_pipeline_sync' 2>/dev/null")
time.sleep(3)

# 새로운 실행: >> daily.log 에 추가
client.exec_command(
    "cd /opt/busan && nohup /opt/busan/venv/bin/python3 -u daily_pipeline_sync.py 20260522 >> /opt/busan/sync_log/daily.log 2>&1 &"
)
# -u = unbuffered output

# 매 15초마다 로그 마지막 줄 확인
for i in range(12):  # 3분
    time.sleep(15)
    stdin, stdout, stderr = client.exec_command("tail -3 /opt/busan/sync_log/daily.log 2>/dev/null", timeout=10)
    out = stdout.read().decode('utf-8','replace').strip()
    print(f"[{(i+1)*15}s] {out}")
    
    # 성공 또는 실패가 확인되면 종료
    if '수집 성공' in out or '✅ API 정상' in out or '수집 완료' in out:
        print("\n>>> 수집 성공 감지!")
        break
    if '수집 전체 실패' in out and i > 2:
        print("\n>>> 실패 확정")
        break

client.close()
