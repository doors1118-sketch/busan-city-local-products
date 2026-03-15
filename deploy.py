"""
원클릭 서버 배포 스크립트
========================
로컬에서 수정한 코드를 NCP 서버에 자동 업로드 + 서비스 재시작

사용법: python deploy.py
       python deploy.py --all    (DB 파일 포함 전체 업로드)
"""
import paramiko, os, sys, time
from scp import SCPClient

sys.stdout.reconfigure(encoding='utf-8')

# ─── 설정 ───
HOST = "49.50.133.160"
USER = "root"
PASSWORD = "U7$B%U5843m"
REMOTE_DIR = "/opt/busan"
LOCAL_DIR = os.path.dirname(os.path.abspath(__file__))

# 기본 업로드 파일 (코드만)
CODE_FILES = [
    "dashboard.py",
    "api_server.py",
    "build_api_cache.py",
    "core_calc.py",
    "api_cache.json",
]

# --all 옵션 시 추가 업로드
DB_FILES = [
    "busan_companies_master.db",
    "busan_agencies_master.db",
    "procurement_contracts.db",
    "procurement.db",
    "servc_site.db",
]

def main():
    full = "--all" in sys.argv
    files = CODE_FILES + (DB_FILES if full else [])

    print("=" * 50)
    print("  부산 공공계약 모니터링 - 서버 배포")
    print("=" * 50)
    print(f"  서버: {HOST}")
    print(f"  모드: {'전체 (코드+DB)' if full else '코드만'}")
    print()

    # 1. SSH 접속
    print("[1/4] 서버 접속 중...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(HOST, username=USER, password=PASSWORD, timeout=15)
        print("  ✅ 접속 성공")
    except Exception as e:
        print(f"  ❌ 접속 실패: {e}")
        return

    # 2. 파일 업로드
    print("[2/4] 파일 업로드 중...")
    scp = SCPClient(ssh.get_transport())
    uploaded = 0
    for f in files:
        local_path = os.path.join(LOCAL_DIR, f)
        if os.path.exists(local_path):
            size_mb = os.path.getsize(local_path) / 1e6
            print(f"  📤 {f} ({size_mb:.1f} MB)")
            scp.put(local_path, f"{REMOTE_DIR}/{f}")
            uploaded += 1
        else:
            print(f"  ⏭️  {f} (없음, 건너뜀)")
    scp.close()
    print(f"  ✅ {uploaded}개 파일 업로드 완료")

    # 3. 서비스 재시작
    print("[3/4] 서비스 재시작 중...")
    stdin, stdout, stderr = ssh.exec_command(
        "systemctl restart busan-api && sleep 2 && systemctl restart busan-dashboard",
        timeout=30
    )
    stdout.read()
    time.sleep(3)
    print("  ✅ 서비스 재시작 완료")

    # 4. 상태 확인
    print("[4/4] 상태 확인...")
    stdin, stdout, stderr = ssh.exec_command("systemctl is-active busan-api busan-dashboard")
    statuses = stdout.read().decode().strip().split("\n")
    api_ok = statuses[0] == "active" if statuses else False
    dash_ok = statuses[1] == "active" if len(statuses) > 1 else False

    print(f"  API 서버:   {'✅ 정상' if api_ok else '❌ 오류'}")
    print(f"  대시보드:   {'✅ 정상' if dash_ok else '❌ 오류'}")

    ssh.close()

    print()
    print("=" * 50)
    print(f"  🌐 http://{HOST}:8501")
    print("=" * 50)

if __name__ == "__main__":
    main()
