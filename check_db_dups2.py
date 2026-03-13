import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')
c = conn.cursor()

c.execute("SELECT untyCntrctNo, cntrctNm, cntrctRefNo, totCntrctAmt, thtmCntrctAmt, rgstDt, chgDt FROM thng_cntrct WHERE cntrctRefNo LIKE '%R26TA01410995%'")
print("망미 청소년탐구센터 계약 상세:")
for r in c.fetchall():
    print(r)

conn.close()
