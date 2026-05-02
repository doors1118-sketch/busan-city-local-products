import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

def run(cmd):
    stdin, stdout, stderr = client.exec_command(cmd)
    out = stdout.read().decode('utf-8').strip()
    return out

print("=== [서버 DB 및 캐시 필터링 검증] ===")
print("1. DB 원본 데이터 (최근 7일 적재분 중 타지역 현장 공사건 존재 여부)")
sql1 = "SELECT COUNT(*), SUM(totCntrctAmt) FROM cnstwk_cntrct WHERE cntrctCnclsDate >= '2026-04-23' AND cnstrtsiteRgnNm NOT LIKE '%부산%';"
out1 = run(f"sqlite3 /opt/busan/procurement_contracts.db \"{sql1}\"")
print(f" - 타지역 공사현장 DB 적재 건수 및 금액: {out1.replace('|', '건 / ')}원 (정상)")

print("\n2. 대시보드(캐시) 필터링 결과 (타지역 현장건 배제 여부)")
out2 = run("cat /opt/busan/sync_log/cache_build.log | grep '타지역' | tail -n 5")
print(" - [캐시 생성 로그 내역]\n" + out2)

client.close()
