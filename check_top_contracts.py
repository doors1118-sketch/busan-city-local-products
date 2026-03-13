import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')

def get_top_contracts(table, limit=5):
    nm_col = 'cnstwkNm' if table == 'cnstwk_cntrct' else 'cntrctNm'
    query = f"""
    SELECT untyCntrctNo, {nm_col} as cntrctNm, cntrctInsttNm, totCntrctAmt, cntrctDate 
    FROM {table} 
    WHERE cntrctDate >= '2026-01-01' AND cntrctDate <= '2026-01-31'
      AND cntrctInsttNm LIKE '%부산%'
    ORDER BY CAST(totCntrctAmt AS REAL) DESC 
    LIMIT {limit}
    """
    return pd.read_sql(query, conn)

print("🚨 [26년 1월 발주액 톱5 계약 점검]")

print("\n🏗️ [공사 Top 5]")
df_c = get_top_contracts('cnstwk_cntrct')
for _, r in df_c.iterrows():
    amt = float(r['totCntrctAmt']) if r['totCntrctAmt'] else 0
    print(f" - {r['cntrctInsttNm'][:15]} | {r['cntrctNm'][:30]}... | {amt:,.0f}원 ({r['cntrctDate']})")

print("\n🤝 [용역 Top 5]")
df_s = get_top_contracts('servc_cntrct')
for _, r in df_s.iterrows():
    amt = float(r['totCntrctAmt']) if r['totCntrctAmt'] else 0
    print(f" - {r['cntrctInsttNm'][:15]} | {r['cntrctNm'][:30]}... | {amt:,.0f}원 ({r['cntrctDate']})")

print("\n📦 [물품 Top 5]")
df_t = get_top_contracts('thng_cntrct')
for _, r in df_t.iterrows():
    amt = float(r['totCntrctAmt']) if r['totCntrctAmt'] else 0
    print(f" - {r['cntrctInsttNm'][:15]} | {r['cntrctNm'][:30]}... | {amt:,.0f}원 ({r['cntrctDate']})")

conn.close()
