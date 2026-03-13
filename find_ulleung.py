import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')

for uno in ['R26TE12591403', 'R26TE12591429']:
    r = conn.execute(f"SELECT dminsttList, cntrctInsttNm, cntrctNm, cntrctInsttCd FROM thng_cntrct WHERE untyCntrctNo='{uno}'").fetchone()
    if r:
        print(f"=== {uno} ===")
        print(f"  계약기관: {r[1]} (코드: {r[3]})")
        print(f"  계약명: {r[2]}")
        print(f"  수요기관목록(dminsttList): {r[0]}")
        print()

conn.close()
