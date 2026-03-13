import pandas as pd
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

EXCEL_PATH = r'C:\Users\COMTREE\Desktop\연습\해운대구용역 계약업체 내역.xlsx'
df = pd.read_excel(EXCEL_PATH, header=7)

conn = sqlite3.connect('procurement_contracts.db')
df_db = pd.read_sql("""
    SELECT untyCntrctNo, cntrctRefNo, cntrctNm, totCntrctAmt, thtmCntrctAmt, dminsttList, cntrctInsttCd
    FROM servc_cntrct WHERE cntrctDate >= '2026-01-01' AND cntrctDate <= '2026-02-28'
""", conn)
conn.close()

mask = (df_db['cntrctInsttCd'].astype(str).str.strip() == '3330000') | df_db['dminsttList'].apply(lambda x: '3330000' in str(x))
df_db_hae = df_db[mask].drop_duplicates(subset=['untyCntrctNo'], keep='last').copy()
db_refs = df_db_hae['cntrctRefNo'].dropna().astype(str).str.strip().tolist()

# 매칭 안 되는 엑셀 건 찾기
unmatched = []
for i, row in df.iterrows():
    ek = str(row['계약납품통합번호']).strip()
    found = any(ek in dr for dr in db_refs)
    if not found:
        unmatched.append(row)

print(f"매칭 안 되는 건: {len(unmatched)}건")
print()
for row in unmatched:
    print(f"  {row['계약납품통합번호']} | {row.get('계약명', '')[:40]} | 총부기={row.get('총부기계약금액', 0):,.0f}")
