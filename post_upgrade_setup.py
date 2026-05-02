"""서버 스펙업 후속 조치: SWAP 생성 및 챗봇 워커 최적화"""
import paramiko
import time

def run_cmd(client, cmd, label, timeout=60):
    print(f"\n  [{label}]")
    try:
        stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
        out = stdout.read().decode('utf-8', errors='replace').strip()
        err = stderr.read().decode('utf-8', errors='replace').strip()
        if out: print(out)
        if err and not err.startswith('fallocate:'): print(f"  [STDERR] {err}")
        if not out and not err: print("  (OK)")
        return out
    except Exception as e:
        print(f"  [ERROR] {e}")
        return ""

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

print("=" * 60)
print("  1. 변경된 스펙 확인")
print("=" * 60)
run_cmd(client, "nproc && free -h", "CPU 코어 수 및 메모리 확인")

print("\n" + "=" * 60)
print("  2. SWAP (가상 메모리) 8GB 설정")
print("=" * 60)
run_cmd(client, """
if ! swapon --show | grep -q 'swapfile'; then
    echo 'SWAP 생성 시작 (8GB)...'
    fallocate -l 8G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    echo '/swapfile none swap sw 0 0' >> /etc/fstab
    echo 'SWAP 8GB 설정 완료'
else
    echo 'SWAP이 이미 설정되어 있습니다.'
fi
free -h
""", "SWAP 설정 및 확인")

print("\n" + "=" * 60)
print("  3. 챗봇 API 워커(Worker) 수 최적화 (1개 -> 4개)")
print("=" * 60)
run_cmd(client, """
SERVICE_FILE="/etc/systemd/system/busan-advisor-pilot.service"
if [ -f "$SERVICE_FILE" ]; then
    if grep -q "\-\-workers" "$SERVICE_FILE"; then
        echo "이미 workers 옵션이 존재합니다."
        grep ExecStart "$SERVICE_FILE"
    else
        echo "workers 4 옵션 추가 중..."
        sed -i 's/uvicorn app.api_server:app --host 0.0.0.0 --port 8001/uvicorn app.api_server:app --host 0.0.0.0 --port 8001 --workers 4/g' "$SERVICE_FILE"
        systemctl daemon-reload
        systemctl restart busan-advisor-pilot
        echo "busan-advisor-pilot 서비스 재시작 완료"
    fi
else
    echo "busan-advisor-pilot.service 파일을 찾을 수 없습니다."
fi
""", "FastAPI 워커 수 4개로 증가")

# 상태 확인
time.sleep(3)
run_cmd(client, "systemctl status busan-advisor-pilot | head -10", "API 서비스 상태 확인")
run_cmd(client, "ps aux | grep uvicorn | grep -v grep", "현재 실행 중인 uvicorn 프로세스")

client.close()
print("\n=== 후속 작업 완료 ===")
