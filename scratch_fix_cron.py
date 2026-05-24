import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# 새 크론탭: 기존 3개 크론에 . .env 추가
new_crontab = """0 3 * * * cd /opt/busan && . /opt/busan/.env && mkdir -p sync_log && /opt/busan/venv/bin/python3 daily_pipeline_sync.py >> /opt/busan/sync_log/daily.log 2>&1
0 4 * * * cd /opt/busan && . /opt/busan/.env && cp api_cache.json api_cache_prev.json && /opt/busan/venv/bin/python3 build_api_cache.py >> /opt/busan/sync_log/cache_build.log 2>&1 && /opt/busan/venv/bin/python3 build_monthly_cache.py >> /opt/busan/sync_log/monthly_build.log 2>&1 && sudo /usr/bin/systemctl restart busan-api
0 9 * * 1-5 cd /opt/busan && . /opt/busan/.env && /opt/busan/venv/bin/python3 alert_check.py >> /opt/busan/alert_log/alert.log 2>&1

# ── 챗봇 DB 자동 갱신 (chatbot_company.db) ──
# 모니터링 DB → 챗봇 DB 마스터 이관
0 5 * * * cd /opt/busan && . /opt/busan/.env && CHATBOT_DB=/opt/busan/chatbot_company.db /opt/busan/venv/bin/python3 bootstrap_master_data.py >> /opt/busan/sync_log/chatbot_master.log 2>&1

# 기술개발제품 인증 API 수집 (13종)
15 5 * * * cd /opt/busan && . /opt/busan/.env && CHATBOT_DB=/opt/busan/chatbot_company.db /opt/busan/venv/bin/python3 import_certified_product_api.py >> /opt/busan/sync_log/chatbot_cert.log 2>&1

# 혁신장터 API 수집
30 5 * * * cd /opt/busan && . /opt/busan/.env && CHATBOT_DB=/opt/busan/chatbot_company.db /opt/busan/venv/bin/python3 import_innovation_product_api.py >> /opt/busan/sync_log/chatbot_innovation.log 2>&1

# 종합쇼핑몰 API 증분 수집 (MAS+일반단가+제3자단가)
45 5 * * * cd /opt/busan && . /opt/busan/.env && CHATBOT_DB=/opt/busan/chatbot_company.db /opt/busan/venv/bin/python3 import_mas_product_api.py >> /opt/busan/sync_log/chatbot_mas.log 2>&1

# 국세청 사업자등록상태 배치 갱신 (주 1회 일요일)
0 6 * * 0 cd /opt/busan && . /opt/busan/.env && CHATBOT_DB=/opt/busan/chatbot_company.db /opt/busan/venv/bin/python3 nts_batch_sync.py >> /opt/busan/sync_log/chatbot_nts.log 2>&1

# Direct production certificate API import (runs after DIRECT_PRODUCTION_SERVICE_KEY is configured)
35 5 * * * cd /opt/busan && . /opt/busan/.env && CHATBOT_DB=/opt/busan/chatbot_company.db /opt/busan/venv/bin/python3 import_direct_production_cert_api.py >> /opt/busan/sync_log/chatbot_direct_production.log 2>&1

# G2B procurement product classification/alias sync (weekly Sunday)
20 6 * * 0 cd /opt/busan && . /opt/busan/.env && CHATBOT_DB=/opt/busan/chatbot_company.db /opt/busan/venv/bin/python3 import_procurement_product_classification_api.py >> /opt/busan/sync_log/chatbot_product_classification.log 2>&1
"""

# 크론탭 갱신
stdin, stdout, stderr = client.exec_command(
    f"echo '{new_crontab}' | crontab -u busan-monitor -",
    timeout=10
)
print("crontab update:", stdout.read().decode('utf-8','replace'))
err = stderr.read().decode('utf-8','replace').strip()
if err:
    print(f"STDERR: {err}")

# 확인
stdin, stdout, stderr = client.exec_command("crontab -u busan-monitor -l | head -5", timeout=10)
print("\n=== Updated crontab (first 5 lines) ===")
print(stdout.read().decode('utf-8','replace'))

# 파이프라인 수동 실행 (보충 수집)
print("\n=== 파이프라인 수동 실행 (20260522~23 보충) ===")
stdin, stdout, stderr = client.exec_command(
    "cd /opt/busan && . /opt/busan/.env && /opt/busan/venv/bin/python3 daily_pipeline_sync.py 20260522 >> /opt/busan/sync_log/daily.log 2>&1 &",
    timeout=10
)
print("파이프라인 백그라운드 실행 시작됨")

client.close()
