import pandas as pd
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

EXCEL_PATH = r'C:\Users\COMTREE\Desktop\연습\해운대구용역 계약업체 내역.xlsx'
df = pd.read_excel(EXCEL_PATH, header=7)

conn = sqlite3.connect('procurement_contracts.db')
df_db = pd.read_sql("""
    SELECT untyCntrctNo, cntrctRefNo, cntrctInsttCd, cntrctNm,
           totCntrctAmt, thtmCntrctAmt, dminsttList
    FROM servc_cntrct
    WHERE cntrctDate >= '2026-01-01' AND cntrctDate <= '2026-02-28'
""", conn)
conn.close()

# 해운대구 필터
def has_haeundae(dminstt_str):
    return '3330000' in str(dminstt_str) if dminstt_str else False

mask = (df_db['cntrctInsttCd'].astype(str).str.strip() == '3330000') | df_db['dminsttList'].apply(has_haeundae)
df_db_hae = df_db[mask].drop_duplicates(subset=['untyCntrctNo'], keep='last').copy()

# 키 비교
print("📋 엑셀 계약납품통합번호 샘플:")
print(df['계약납품통합번호'].head(5).tolist())
print(f"\n📋 DB untyCntrctNo 샘플:")
print(df_db_hae['untyCntrctNo'].head(5).tolist())
print(f"\n📋 DB cntrctRefNo 샘플:")
print(df_db_hae['cntrctRefNo'].head(5).tolist())

# cntrctRefNo로 매칭 시도
excel_keys = set(df['계약납품통합번호'].dropna().astype(str).str.strip())

# cntrctRefNo에서 앞부분만 추출해서 비교
df_db_hae['refNo_short'] = df_db_hae['cntrctRefNo'].astype(str).str[:15]

# 엑셀의 계약요청접수번호도 확인
if '계약요청접수번호' in df.columns:
    print(f"\n📋 엑셀 계약요청접수번호 샘플:")
    print(df['계약요청접수번호'].dropna().head(5).tolist())

# cntrctRefNo와 엑셀 키 비교
db_ref_keys = set(df_db_hae['cntrctRefNo'].dropna().astype(str).str.strip())
excel_req_keys = set(df['계약요청접수번호'].dropna().astype(str).str.strip()) if '계약요청접수번호' in df.columns else set()

both_ref = excel_keys & db_ref_keys
both_req = excel_req_keys & db_ref_keys

print(f"\n📌 계약납품통합번호 vs DB cntrctRefNo: {len(both_ref)}개 매칭")
print(f"📌 계약요청접수번호 vs DB cntrctRefNo: {len(both_req)}개 매칭")

# 엑셀의 계약납품통합번호 vs DB untyCntrctNo 직접 비교
# 엑셀에서 _5 suffix 제거해보기
excel_keys_trimmed = set(k.split('_')[0] for k in excel_keys)
db_unty_keys = set(df_db_hae['untyCntrctNo'].dropna().astype(str).str.strip())
db_unty_trimmed = set(k.split('_')[0] if '_' in k else k for k in db_unty_keys)

both_trimmed = excel_keys_trimmed & db_unty_trimmed
print(f"\n📌 suffix 제거 후 매칭: {len(both_trimmed)}개")

# 엑셀 계약명 vs DB 계약명으로 비교
excel_names = set(df.iloc[:, 14].dropna().astype(str).str.strip()) if len(df.columns) > 14 else set()
db_names = set(df_db_hae['cntrctNm'].dropna().astype(str).str.strip())
both_names = excel_names & db_names
print(f"📌 계약명 매칭: {len(both_names)}개 / 엑셀 {len(excel_names)}개 / DB {len(db_names)}개")

# 금액 비교
print(f"\n💰 금액 비교:")
for col in ['총부기계약금액', '최초계약금액', '계약금액', '계약지분금액']:
    if col in df.columns:
        val = pd.to_numeric(df[col], errors='coerce').sum()
        print(f"  엑셀 {col}: {val:,.0f}원")

db_thtm = df_db_hae['thtmCntrctAmt'].apply(pd.to_numeric, errors='coerce')
db_tot = df_db_hae['totCntrctAmt'].apply(pd.to_numeric, errors='coerce')
db_amt = db_thtm.where(db_thtm.notna() & (db_thtm != 0), db_tot)
print(f"  DB thtmCntrctAmt우선: {db_amt.sum():,.0f}원")
print(f"  DB totCntrctAmt: {db_tot.sum():,.0f}원")
