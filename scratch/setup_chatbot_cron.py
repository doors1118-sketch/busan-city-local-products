import paramiko, sys
sys.stdout.reconfigure(encoding='utf-8')

HOST = "49.50.133.160"
USER = "root"
PASSWORD = "back9900@@"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, username=USER, password=PASSWORD, timeout=15)

# 챗봇 DB 크론 설정
# 기존 모니터링: 03:00 daily_pipeline_sync, 04:00 cache_build
# 챗봇 크론: 모니터링 완료 후 05:00~06:00 사이에 순차 실행
CRON_LINES = """
# ── 챗봇 DB 자동 갱신 (chatbot_company.db) ──
# 모니터링 DB → 챗봇 DB 마스터 이관 (daily_pipeline_sync 완료 후)
0 5 * * * cd /opt/busan && CHATBOT_DB=/opt/busan/chatbot_company.db /opt/busan/venv/bin/python3 bootstrap_master_data.py >> /opt/busan/sync_log/chatbot_master.log 2>&1

# 기술개발제품 인증 API 수집 (13종)
15 5 * * * cd /opt/busan && CHATBOT_DB=/opt/busan/chatbot_company.db TECH_PRODUCT_SERVICE_KEY=$(grep SHOPPING_MALL /opt/busan/.env | cut -d= -f2) /opt/busan/venv/bin/python3 import_certified_product_api.py >> /opt/busan/sync_log/chatbot_cert.log 2>&1

# 혁신장터 API 수집
30 5 * * * cd /opt/busan && CHATBOT_DB=/opt/busan/chatbot_company.db INNOVATION_SERVICE_KEY=$(grep SHOPPING_MALL /opt/busan/.env | cut -d= -f2) /opt/busan/venv/bin/python3 import_innovation_product_api.py >> /opt/busan/sync_log/chatbot_innovation.log 2>&1

# 종합쇼핑몰 MAS API 증분 수집
45 5 * * * cd /opt/busan && source /opt/busan/.env && CHATBOT_DB=/opt/busan/chatbot_company.db /opt/busan/venv/bin/python3 import_mas_product_api.py >> /opt/busan/sync_log/chatbot_mas.log 2>&1

# 국세청 사업자등록상태 배치 갱신 (주 1회 일요일)
0 6 * * 0 cd /opt/busan && CHATBOT_DB=/opt/busan/chatbot_company.db NTS_SERVICE_KEY=$(grep SHOPPING_MALL /opt/busan/.env | cut -d= -f2) /opt/busan/venv/bin/python3 nts_batch_sync.py >> /opt/busan/sync_log/chatbot_nts.log 2>&1
"""

# 현재 busan-monitor crontab 가져와서 챗봇 크론 추가
stdin, stdout, stderr = ssh.exec_command("crontab -u busan-monitor -l 2>/dev/null")
current_cron = stdout.read().decode()

# 이미 챗봇 크론이 있는지 확인
if 'chatbot_company.db' in current_cron:
    print("⚠️ 챗봇 크론이 이미 등록되어 있습니다. 기존 내용:")
    print(current_cron)
    print("\n덮어쓰시겠습니까? 기존 크론에 추가합니다.")

# 챗봇 크론 라인만 제거 후 다시 추가 (중복 방지)
clean_lines = []
for line in current_cron.split('\n'):
    if 'chatbot_company.db' not in line and 'chatbot_master' not in line and 'chatbot_cert' not in line and 'chatbot_innovation' not in line and 'chatbot_mas' not in line and 'chatbot_nts' not in line and '챗봇 DB' not in line:
        clean_lines.append(line)

new_cron = '\n'.join(clean_lines).rstrip() + '\n' + CRON_LINES

# 크론 설정
stdin, stdout, stderr = ssh.exec_command(f"echo '{new_cron}' | crontab -u busan-monitor -")
err = stderr.read().decode()
if err:
    print(f"❌ 크론 설정 실패: {err}")
else:
    print("✅ 크론 등록 완료")

# 확인
print("\n=== 최종 busan-monitor crontab ===")
stdin, stdout, stderr = ssh.exec_command("crontab -u busan-monitor -l")
print(stdout.read().decode())

# 로그 디렉토리 생성
ssh.exec_command("mkdir -p /opt/busan/sync_log && chown busan-monitor:busan-monitor /opt/busan/sync_log")

# 타임라인 출력
print("=== 크론 타임라인 (일일) ===")
print("  03:00  [모니터링] daily_pipeline_sync.py")
print("  04:00  [모니터링] build_api_cache.py + 서비스 재시작")
print("  05:00  [챗봇] bootstrap_master_data.py (마스터 이관)")
print("  05:15  [챗봇] import_certified_product_api.py (기술개발제품 13종)")
print("  05:30  [챗봇] import_innovation_product_api.py (혁신장터)")
print("  05:45  [챗봇] import_mas_product_api.py (종합쇼핑몰 MAS)")
print("  06:00  [챗봇] nts_batch_sync.py (국세청, 일요일만)")
print("  09:00  [모니터링] alert_check.py (평일)")
print()
print("  DB 분리: 모니터링 → procurement*.db / 챗봇 → chatbot_company.db")
print("  충돌 없음: 시간 분산 + DB 파일 분리")

ssh.close()
