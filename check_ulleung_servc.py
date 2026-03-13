import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
c = sqlite3.connect('procurement_contracts.db').cursor()
c.execute("SELECT untyCntrctNo, cntrctNm, cntrctInsttNm, cntrctInsttOfclTelNo, cntrctInsttOfclNm, cntrctInsttChrgDeptNm FROM servc_cntrct WHERE cntrctNm LIKE '%울릉공항%' AND cntrctDate >= '2026-01-01'")
for r in c.fetchall():
    print(f"계약번호: {r[0]}")
    print(f"계약명: {r[1]}")
    print(f"발주기관: {r[2]}")
    print(f"담당자: {r[4]} ({r[5]})")
    print(f"전화번호: {r[3]}")
    print("---")
