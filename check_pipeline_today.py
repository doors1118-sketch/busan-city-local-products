import paramiko
import json
import os
import datetime

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

def run(cmd):
    stdin, stdout, stderr = client.exec_command(cmd)
    out = stdout.read().decode('utf-8').strip()
    return out

print("=== [일일 파이프라인 점검 (오늘 자정 이후)] ===")

# 1. DB 동기화 기록 확인
print("1. 최근 DB 동기화 완료 기록 (sync_log 테이블)")
print(run("sqlite3 /opt/busan/procurement_contracts.db 'SELECT * FROM sync_log ORDER BY sync_date DESC LIMIT 3;'"))

# 2. 캐시 파일 갱신 시간 확인
print("\n2. 대시보드 캐시 파일 갱신 시간 (api_cache.json, monthly_cache.json)")
print(run("stat -c '%y %n' /opt/busan/*_cache.json"))

# 3. 크론 로그 확인 (새벽 4시경 실행 여부)
print("\n3. 오늘 새벽 파이프라인(cron) 실행 이력")
print(run("grep 'daily_pipeline_sync' /var/log/syslog | grep 'May  2' | tail -n 5"))
print(run("grep 'build_monthly_cache' /var/log/syslog | grep 'May  2' | tail -n 5"))

client.close()
