import pandas as pd
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

EXCEL_PATH = r'C:\Users\COMTREE\Desktop\연습\해운대구용역 계약업체 내역.xlsx'
df = pd.read_excel(EXCEL_PATH, header=7)

conn = sqlite3.connect('procurement_contracts.db')
df_db = pd.read_sql("""
    SELECT untyCntrctNo, cntrctRefNo, ntceNo, cntrctNm, totCntrctAmt, thtmCntrctAmt, dminsttList, cntrctInsttCd
    FROM servc_cntrct
    WHERE cntrctDate >= '2026-01-01' AND cntrctDate <= '2026-02-28'
""", conn)
conn.close()

mask = (df_db['cntrctInsttCd'].astype(str).str.strip() == '3330000') | df_db['dminsttList'].apply(lambda x: '3330000' in str(x))
df_db_hae = df_db[mask].drop_duplicates(subset=['untyCntrctNo'], keep='last').copy()

# 1. 엑셀 계약납품통합번호의 R로 시작하는 것 vs DB cntrctRefNo
excel_r_keys = [str(k).strip() for k in df['계약납품통합번호'].dropna() if str(k).startswith('R')]
db_ref = df_db_hae['cntrctRefNo'].dropna().astype(str).str.strip().tolist()

print("🔍 [1] 엑셀 R-키 vs DB cntrctRefNo 매칭 시도")
print(f"  엑셀 R-키 샘플: {excel_r_keys[:3]}")
print(f"  DB cntrctRefNo 샘플: {db_ref[:3]}")

# cntrctRefNo 앞부분이 엑셀 키와 매칭되는지
match_count = 0
for ek in excel_r_keys:
    for dr in db_ref:
        if dr.startswith(ek):
            match_count += 1
            break
print(f"  cntrctRefNo.startswith(엑셀키) 매칭: {match_count}건")

# 2. 입찰공고번호 매칭
if '입찰공고번호' in df.columns:
    excel_ntce = set(df['입찰공고번호'].dropna().astype(str).str.strip())
    db_ntce = set(df_db_hae['ntceNo'].dropna().astype(str).str.strip())
    both = excel_ntce & db_ntce
    print(f"\n🔍 [2] 입찰공고번호 vs DB ntceNo")
    print(f"  엑셀: {len(excel_ntce)}개 / DB: {len(db_ntce)}개 / 매칭: {len(both)}개")

# 3. cntrctRefNo 자체를 엑셀 키에서 찾기 (엑셀 키 뒤에 00 붙인 형태)
excel_keys_00 = set(str(k).strip() + '00' for k in df['계약납품통합번호'].dropna() if str(k).startswith('R'))
both_00 = excel_keys_00 & set(db_ref)
print(f"\n🔍 [3] 엑셀키+'00' vs DB cntrctRefNo: {len(both_00)}개 매칭")

# 좀 더 유연하게: 엑셀키가 cntrctRefNo에 포함되는지
match_contains = 0
matched_pairs = []
for ek in excel_r_keys:
    for dr in db_ref:
        if ek in dr:
            match_contains += 1
            matched_pairs.append((ek, dr))
            break
print(f"\n🔍 [4] 엑셀키 in DB cntrctRefNo (부분매칭): {match_contains}건")
if matched_pairs:
    print(f"  매칭 샘플: {matched_pairs[:3]}")
