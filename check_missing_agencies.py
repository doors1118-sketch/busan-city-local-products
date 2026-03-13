import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

# 1. 마스터DB에서 관련 기관 검색
conn_ag = sqlite3.connect('busan_agencies_master.db')
cur = conn_ag.cursor()

print("🔍 [1] 강서구 관련 기관 (dminsttCd LIKE '336%' 또는 이름에 '강서' 포함)")
cur.execute("SELECT dminsttCd, dminsttNm FROM agency_master WHERE dminsttCd LIKE '336%' OR dminsttNm LIKE '%강서%'")
for r in cur.fetchall():
    print(f"  {r[0]} | {r[1]}")

print()
print("🔍 [2] 서구 관련 기관 (dminsttCd LIKE '326%' 또는 이름에 '서구' 포함)")
cur.execute("SELECT dminsttCd, dminsttNm FROM agency_master WHERE dminsttCd LIKE '326%' OR dminsttNm LIKE '%서구%'")
for r in cur.fetchall():
    print(f"  {r[0]} | {r[1]}")

conn_ag.close()

# 2. 계약DB에서 해당 코드로 검색
conn_pr = sqlite3.connect('procurement_contracts.db')
cur2 = conn_pr.cursor()

print()
print("🔍 [3] 계약DB에서 3360043, 3260021 기관으로 된 종합쇼핑몰 건수")
cur2.execute("SELECT dminsttCd, dminsttNm, COUNT(*) FROM shopping_cntrct WHERE dminsttCd IN ('3360043', '3260021') GROUP BY dminsttCd, dminsttNm")
for r in cur2.fetchall():
    print(f"  {r[0]} | {r[1]} | {r[2]}건")

# 3. 다른 계약유형에서도 등장하는지
for tbl in ['cnstwk_cntrct', 'servc_cntrct', 'thng_cntrct']:
    cur2.execute(f"SELECT cntrctInsttCd, cntrctInsttNm, COUNT(*) FROM {tbl} WHERE cntrctInsttCd IN ('3360043', '3260021') GROUP BY cntrctInsttCd")
    rows = cur2.fetchall()
    if rows:
        for r in rows:
            print(f"  [{tbl}] {r[0]} | {r[1]} | {r[2]}건")

# 4. classify_new_agencies.py에서 이 코드가 처리되었는지 확인
print()
print("🔍 [4] 마스터DB 수집 방식 확인 - 전체 기관 수")
conn_ag = sqlite3.connect('busan_agencies_master.db')
c = conn_ag.cursor()
c.execute("SELECT COUNT(*) FROM agency_master")
print(f"  마스터DB 총 기관 수: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM agency_master WHERE dminsttCd LIKE '336%'")
print(f"  강서구(336%) 기관 수: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM agency_master WHERE dminsttCd LIKE '326%'")
print(f"  서구(326%) 기관 수: {c.fetchone()[0]}")
conn_ag.close()
conn_pr.close()
