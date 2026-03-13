import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')
c = conn.cursor()

c.execute("SELECT untyCntrctNo, cntrctNm, totCntrctAmt, dcsnCntrctNo, corpList FROM servc_cntrct WHERE untyCntrctNo IN ('R26TE11041526', 'R26TE12012924', 'R26TE11645725')")
rows = c.fetchall()

print("🚨 [용역] 지분율 100% 초과 이상 데이터 분석")
for r in rows:
    no, name, amt, ref, corps = r
    try: amt = float(amt)
    except: amt = 0.0
    print(f"\n▶ 계약번호: {no} (참조: {ref}) | 금액: {amt:,.0f}")
    print(f"▶ 계약명: {name}")
    print(f"▶ 원본 corpList 배열: {corps}")
    
    # 파싱 시뮬레이션
    tot_share = 0
    print("  [파싱 결과]")
    if corps and corps != 'nan':
        for chunk in str(corps).split('[')[1:]:
            parts = chunk.split(']')[0].split('^')
            if len(parts) >= 10:
                share = float(parts[6].strip()) if parts[6].strip() else 100.0
                print(f"   - 업체: {parts[3]} (역할: {parts[1]}, 지분율: {share}%)")
                tot_share += share
    print(f"  총합 지분율: {tot_share}%")

conn.close()
