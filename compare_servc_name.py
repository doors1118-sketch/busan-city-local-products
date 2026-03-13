import pandas as pd
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

EXCEL_PATH = r'C:\Users\COMTREE\Desktop\연습\부산전체용역 계약업체 내역.xlsx'
df = pd.read_excel(EXCEL_PATH, header=7)

conn = sqlite3.connect('procurement_contracts.db')
df_db = pd.read_sql("""
    SELECT untyCntrctNo, cntrctRefNo, cntrctInsttCd, cntrctNm, dminsttList
    FROM servc_cntrct
    WHERE cntrctDate >= '2026-01-01' AND cntrctDate <= '2026-02-28'
""", conn)
conn.close()

conn_ag = sqlite3.connect('busan_agencies_master.db')
busan_codes = set(pd.read_sql("SELECT dminsttCd FROM agency_master", conn_ag)['dminsttCd'].astype(str).str.strip())
conn_ag.close()

df_db['is_busan'] = df_db.apply(lambda r: str(r['cntrctInsttCd']).strip() in busan_codes or any(c in str(r['dminsttList']) for c in busan_codes), axis=1)
df_db_bs = df_db[df_db['is_busan']].drop_duplicates(subset=['untyCntrctNo'], keep='last').copy()

# 계약명 매칭
excel_names = set(df['계약명'].dropna().astype(str).str.strip())
db_names = set(df_db_bs['cntrctNm'].dropna().astype(str).str.strip())

both_names = excel_names & db_names
only_excel_names = excel_names - db_names
only_db_names = db_names - excel_names

print(f"📌 계약명 매칭:")
print(f"  엑셀 고유 계약명: {len(excel_names)}개")
print(f"  DB 고유 계약명:   {len(db_names)}개")
print(f"  양쪽 모두:        {len(both_names)}개 ({len(both_names)/len(excel_names)*100:.1f}%)")
print(f"  엑셀에만:         {len(only_excel_names)}개")
print(f"  DB에만:           {len(only_db_names)}개")

# 계약번호 미매칭인 건의 계약명 매칭 확인
db_refs = df_db_bs['cntrctRefNo'].dropna().astype(str).str.strip().tolist()
excel_keys = df['계약납품통합번호'].dropna().astype(str).str.strip().unique()

unmatched_keys = [ek for ek in excel_keys if not any(ek in dr for dr in db_refs)]
unmatched_rows = df[df['계약납품통합번호'].astype(str).str.strip().isin(unmatched_keys)]
unmatched_names = set(unmatched_rows['계약명'].dropna().astype(str).str.strip())

rescued_by_name = unmatched_names & db_names
print(f"\n📌 계약번호 미매칭 442건 중 계약명으로 구제:")
print(f"  미매칭 건의 고유 계약명: {len(unmatched_names)}개")
print(f"  이 중 DB 계약명과 일치: {len(rescued_by_name)}개")
print(f"\n  → 최종 미매칭: {len(unmatched_names) - len(rescued_by_name)}개")

# 최종 못찾은 건 샘플
still_missing = unmatched_names - db_names
print(f"\n⚠️ 계약번호+계약명 모두 미매칭 ({len(still_missing)}건 중 상위10):")
for nm in list(still_missing)[:10]:
    row = unmatched_rows[unmatched_rows['계약명'].astype(str).str.strip() == nm].iloc[0]
    key = str(row['계약납품통합번호']).strip()
    amt = pd.to_numeric(row.get('총부기계약금액', 0), errors='coerce')
    print(f"  {key[:20]:20s} | {nm[:50]:50s} | {amt:>15,.0f}원")
