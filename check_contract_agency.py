import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')
cursor = conn.cursor()

cursor.execute("SELECT cntrctInsttCd, cntrctInsttNm, dminsttList FROM thng_cntrct WHERE cntrctNm LIKE '%망미 청소년탐구센터 정보통신공사 관급자재_통합배선반%'")
rows = cursor.fetchall()

for row in rows:
    print(f"계약기관코드: {row[0]}")
    print(f"계약기관명: {row[1]}")
    print(f"수요기관목록(dminsttList): {row[2]}")

conn.close()
