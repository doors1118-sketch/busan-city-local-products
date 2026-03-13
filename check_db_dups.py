import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')
c = conn.cursor()

c.execute("SELECT untyCntrctNo, cntrctNm, cntrctRefNo, totCntrctAmt FROM thng_cntrct WHERE cntrctRefNo LIKE '%R26TA01410995%'")
rows = c.fetchall()
print("망미 청소년탐구센터 참조번호(R26TA01410995)를 가진 DB 계약:")
for r in rows:
    print(r)

# 그리고 신교통문화운동추 참조번호(R26TA01355126) 관련 DB 건수도 확인
c.execute("SELECT untyCntrctNo, cntrctNm, cntrctRefNo, totCntrctAmt FROM thng_cntrct WHERE cntrctRefNo LIKE '%R26TA01355126%'")
print("\n신교통문화운동 참조번호(R26TA01355126)를 가진 DB 계약:")
for r in c.fetchall():
    print(r)

conn.close()
