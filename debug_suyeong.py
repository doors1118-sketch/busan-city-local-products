import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')
conn_cp = sqlite3.connect('busan_companies_master.db')
busan_biznos = set(pd.read_sql("SELECT bizno FROM company_master", conn_cp)['bizno'].dropna().astype(str).str.replace('-','',regex=False).str.strip())
conn_cp.close()

# 수영구 공사 건 corpList 확인
df = pd.read_sql("""
    SELECT untyCntrctNo, cntrctInsttCd, corpList, totCntrctAmt, thtmCntrctAmt
    FROM cnstwk_cntrct 
    WHERE cntrctDate >= '2026-01-01' AND cntrctInsttCd = '3380000'
    LIMIT 5
""", conn)
conn.close()

print("수영구 공사 corpList 샘플:")
for _, r in df.iterrows():
    corps = str(r['corpList'])
    print(f"  {r['untyCntrctNo']} | corpList={corps[:80]}...")
    
    # 파싱 테스트
    if corps and corps != 'nan':
        for part in corps.split('|'):
            segs = part.split(',')
            biz = segs[0].replace('-','').strip() if segs else ''
            share = segs[1] if len(segs) >= 2 else '?'
            is_local = '✅ 부산' if biz in busan_biznos else '❌ 타지역'
            print(f"    사업자={biz} 지분={share} → {is_local}")
