import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# 더 정밀한 검증: 구 API(ScsbidInfo)가 반환했을 전국 건수와 비교
# DB에서 전국 기준 원본 건수를 추적할 수 있는 로그가 있는지 확인
# 또는 DB의 부산 비율로 역산

script = r'''
import sqlite3, json

conn = sqlite3.connect('/opt/busan/procurement_contracts.db')

# 방법: 기존 적재된 부산 데이터가 신 API inqryDiv=1 반환 전국 건수의 합리적 비율인지 확인
# 전국 대비 부산 비율은 보통 일정함

# 1. 자체조달 (getCntrctInfoListThng 등) — DB에는 부산만 있으므로
# 비율 = DB건수 / API전국건수
api_totals = {
    '20260521': {'thng': 3383, 'cnstwk': 6880, 'servc': 6667},
    '20260520': {'thng': 3504, 'cnstwk': 6588, 'servc': 6667},
    '20260519': {'thng': 3323, 'cnstwk': 6463, 'servc': 6557},
}

db_map = {
    'thng': 'thng_cntrct',
    'cnstwk': 'cnstwk_cntrct',
    'servc': 'servc_cntrct',
}

print("=== 부산 비율 검증 (DB부산건수 / API전국건수) ===")
print(f"{'날짜':>10s}  {'유형':>8s}  {'DB(부산)':>10s}  {'API(전국)':>10s}  {'비율':>8s}")
print("-" * 55)

for d in ['20260521','20260520','20260519']:
    d_fmt = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    for api_type, tbl in db_map.items():
        db_cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl} WHERE rgstDt LIKE '{d_fmt}%'").fetchone()[0]
        api_cnt = api_totals[d].get(api_type, 0)
        ratio = (db_cnt / api_cnt * 100) if api_cnt else 0
        print(f"{d:>10s}  {api_type:>8s}  {db_cnt:>10,}  {api_cnt:>10,}  {ratio:>7.1f}%")

# 2. 중앙조달(PPSSrch) 건수 — DB에서 중앙/자체 구분이 있는지 확인
print("\n=== 중앙조달 vs 자체조달 분리 확인 ===")
for tbl in ['cnstwk_cntrct','servc_cntrct','thng_cntrct']:
    # cntrctInsttCd가 1230으로 시작하면 중앙조달
    try:
        total = conn.execute(f"SELECT COUNT(*) FROM {tbl} WHERE rgstDt LIKE '2026-05-21%'").fetchone()[0]
        central = conn.execute(f"SELECT COUNT(*) FROM {tbl} WHERE rgstDt LIKE '2026-05-21%' AND cntrctInsttCd LIKE '1230%'").fetchone()[0]
        local = total - central
        print(f"  {tbl} / 20260521: 총 {total}건 (중앙 {central} + 자체 {local})")
    except Exception as e:
        print(f"  {tbl}: {e}")

# 3. 가장 중요: 파이프라인 로그에서 이전 수집 시 전국 건수 확인
print("\n=== 과거 파이프라인 로그에서 수집 건수 확인 ===")
import re
try:
    with open('/opt/busan/sync_log/daily.log', 'r', encoding='utf-8') as f:
        lines = f.readlines()
    # 20260521 수집 관련 로그 찾기
    for i, line in enumerate(lines):
        if '20260521' in line and ('수집' in line or '건' in line):
            print(f"  L{i+1}: {line.strip()}")
except Exception as e:
    print(f"  로그 읽기 실패: {e}")

conn.close()
'''

stdin, stdout, stderr = client.exec_command(
    f"cat > /tmp/verify2.py << 'PYEOF'\n{script}\nPYEOF\n/opt/busan/venv/bin/python3 /tmp/verify2.py",
    timeout=30
)
print(stdout.read().decode('utf-8', 'replace'))

client.close()
