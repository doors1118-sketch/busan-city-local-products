"""
챗봇 인프라 서버 배포 스크립트
==============================
챗봇 DB + API sync 스크립트 + 최신 api_server.py를 NCP 서버에 업로드하고
busan-api 서비스를 재시작한다.

사용법: python deploy_chatbot.py
       python deploy_chatbot.py --db-only    (DB만 업로드, 서비스 재시작 안 함)
       python deploy_chatbot.py --code-only  (코드만 업로드)
"""
import paramiko, os, sys, time
from scp import SCPClient

sys.stdout.reconfigure(encoding='utf-8')

# ─── 설정 (deploy.py와 동일) ───
HOST = "49.50.133.160"
USER = "root"
PASSWORD = "back9900@@"
REMOTE_DIR = "/opt/busan"
LOCAL_DIR = os.path.dirname(os.path.abspath(__file__))

# 챗봇 API 서버 (모니터링 + 챗봇 통합)
API_FILES = [
    "api_server.py",
]

# 챗봇 전용 DB 구축/동기화 스크립트
CHATBOT_SCRIPTS = [
    "migrate_chatbot_db.py",
    "bootstrap_master_data.py",
    "bootstrap_from_excel.py",
    "import_certified_product_api.py",
    "import_innovation_product_api.py",
    "import_mas_product_api.py",
    "nts_business_status_client.py",
    "nts_batch_sync.py",
    "test_runtime_http_smoke.py",
]

# 챗봇 DB
CHATBOT_DB = "staging_chatbot_company.db"


def main():
    db_only = "--db-only" in sys.argv
    code_only = "--code-only" in sys.argv

    print("=" * 60)
    print("  부산 조달 모니터링 - 챗봇 인프라 배포")
    print("=" * 60)
    print(f"  서버: {HOST}")
    print(f"  모드: {'DB만' if db_only else 'Code만' if code_only else '전체 (Code+DB)'}")
    print(f"  production_deployment: HOLD (변경 없음)")
    print()

    # 파일 목록 결정
    files = []
    if not db_only:
        files += API_FILES + CHATBOT_SCRIPTS
    if not code_only:
        if os.path.exists(os.path.join(LOCAL_DIR, CHATBOT_DB)):
            files.append(CHATBOT_DB)
        else:
            print(f"  ⚠️ {CHATBOT_DB} 파일이 없습니다.")

    if not files:
        print("  업로드할 파일이 없습니다.")
        return

    # 업로드 목록 확인
    print("  📋 업로드 파일 목록:")
    total_size = 0
    for f in files:
        fp = os.path.join(LOCAL_DIR, f)
        if os.path.exists(fp):
            sz = os.path.getsize(fp)
            total_size += sz
            print(f"     {f} ({sz/1e6:.1f} MB)")
        else:
            print(f"     {f} (없음, 건너뜀)")
    print(f"  총 {total_size/1e6:.1f} MB")
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

    # 2. 서버 사전 확인
    print("[2/4] 서버 사전 확인...")
    stdin, stdout, stderr = ssh.exec_command(f"ls -la {REMOTE_DIR}/api_server.py {REMOTE_DIR}/{CHATBOT_DB} 2>/dev/null")
    existing = stdout.read().decode().strip()
    if existing:
        print(f"  기존 파일:")
        for line in existing.split('\n'):
            print(f"    {line}")
    else:
        print("  기존 챗봇 파일 없음 (첫 배포)")

    # 서버 디스크 확인
    stdin, stdout, stderr = ssh.exec_command("df -h /opt/busan | tail -1")
    disk = stdout.read().decode().strip()
    print(f"  디스크: {disk}")

    # 3. 파일 업로드
    print("[3/4] 파일 업로드 중...")
    scp = SCPClient(ssh.get_transport())
    uploaded = 0
    for f in files:
        local_path = os.path.join(LOCAL_DIR, f)
        if os.path.exists(local_path):
            size_mb = os.path.getsize(local_path) / 1e6
            print(f"  📤 {f} ({size_mb:.1f} MB)...", end="", flush=True)
            try:
                scp.put(local_path, f"{REMOTE_DIR}/{f}")
                print(" ✅")
                uploaded += 1
            except Exception as e:
                print(f" ❌ {e}")
        else:
            print(f"  ⏭️  {f} (없음, 건너뜀)")
    scp.close()
    print(f"  ✅ {uploaded}개 파일 업로드 완료")

    # 4. 서비스 재시작 (DB만 올리는 경우 재시작 안 함)
    if db_only:
        print("[4/4] DB만 업로드 — 서비스 재시작 생략")
    else:
        print("[4/4] busan-api 서비스 재시작 중...")
        stdin, stdout, stderr = ssh.exec_command(
            "systemctl restart busan-api && sleep 2 && systemctl is-active busan-api",
            timeout=30
        )
        result = stdout.read().decode().strip()
        api_ok = result.split('\n')[-1] == "active"
        print(f"  API 서버: {'✅ 정상' if api_ok else '❌ 오류'}")

        if not api_ok:
            stdin, stdout, stderr = ssh.exec_command("journalctl -u busan-api --no-pager -n 20")
            logs = stdout.read().decode()
            print(f"  최근 로그:\n{logs}")

    # 배포 후 확인
    print()
    print("=" * 60)
    print("  배포 후 확인")
    print("=" * 60)

    # DB 파일 확인
    stdin, stdout, stderr = ssh.exec_command(f"ls -la {REMOTE_DIR}/{CHATBOT_DB}")
    db_info = stdout.read().decode().strip()
    print(f"  DB: {db_info}")

    # api_server.py 확인
    stdin, stdout, stderr = ssh.exec_command(f"ls -la {REMOTE_DIR}/api_server.py")
    api_info = stdout.read().decode().strip()
    print(f"  API: {api_info}")

    # 헬스체크 (서비스 재시작한 경우에만)
    if not db_only:
        time.sleep(2)
        stdin, stdout, stderr = ssh.exec_command("curl -s http://127.0.0.1:8000/api/chatbot/health 2>/dev/null | python3 -m json.tool 2>/dev/null || echo 'health check failed'")
        health = stdout.read().decode().strip()
        print(f"  Health:\n    {health[:500]}")

        stdin, stdout, stderr = ssh.exec_command("curl -s http://127.0.0.1:8000/api/chatbot/version 2>/dev/null | python3 -m json.tool 2>/dev/null || echo 'version check failed'")
        version = stdout.read().decode().strip()
        print(f"  Version:\n    {version[:500]}")

    ssh.close()

    print()
    print("=" * 60)
    print(f"  🌐 API: http://{HOST}:8000/api/chatbot/health")
    print(f"  production_deployment: HOLD")
    print("=" * 60)


if __name__ == "__main__":
    main()
