import pandas as pd
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

EXCEL_PATH = r'C:\Users\COMTREE\Desktop\연습\부산전체용역 계약업체 내역.xlsx'

# 헤더 찾기
df_raw = pd.read_excel(EXCEL_PATH, header=None, nrows=15)
header_row = None
for i, row in df_raw.iterrows():
    vals = [str(v) for v in row.values if str(v) != 'nan']
    if any('계약납품통합번호' == str(v).strip() for v in vals):
        header_row = i
        break
    if any('계약납품' in str(v) for v in vals):
        header_row = i

print(f"헤더 행: {header_row}")
df = pd.read_excel(EXCEL_PATH, header=header_row)
print(f"📊 엑셀 행수: {len(df):,}")
print(f"컬럼: {list(df.columns[:8])}")

# 금액 컬럼
for col in ['총부기계약금액', '최초계약금액', '계약금액', '계약지분금액']:
    if col in df.columns:
        val = pd.to_numeric(df[col], errors='coerce').sum()
        print(f"  엑셀 {col}: {val:,.0f}원")

# 변경차수 중복 확인
if '계약납품통합번호' in df.columns and '계약납품통합변경차수' in df.columns:
    uni = df.drop_duplicates(subset=['계약납품통합번호'])
    print(f"\n  엑셀 전체 행: {len(df)} / 고유 계약납품통합번호: {len(uni)}")
    print(f"  변경차수 분포: {df["계약납품통합변경차수"].value_counts().head(5).to_dict()}")

# DB 비교
conn = sqlite3.connect('procurement_contracts.db')
df_db = pd.read_sql("""
    SELECT untyCntrctNo, cntrctRefNo, cntrctInsttCd, cntrctNm,
           totCntrctAmt, thtmCntrctAmt, dminsttList
    FROM servc_cntrct
    WHERE cntrctDate >= '2026-01-01' AND cntrctDate <= '2026-02-28'
""", conn)

# 부산 기관 목록
conn_ag = sqlite3.connect('busan_agencies_master.db')
df_ag = pd.read_sql("SELECT dminsttCd FROM agency_master", conn_ag)
conn_ag.close()
busan_codes = set(df_ag['dminsttCd'].astype(str).str.strip())

# DB에서 부산 관련 건 필터
def is_busan(row):
    if str(row['cntrctInsttCd']).strip() in busan_codes:
        return True
    dminstt = str(row['dminsttList']) if row['dminsttList'] else ''
    return any(c in dminstt for c in busan_codes)

df_db['is_busan'] = df_db.apply(is_busan, axis=1)
df_db_busan = df_db[df_db['is_busan']].drop_duplicates(subset=['untyCntrctNo'], keep='last').copy()

print(f"\n📋 DB 용역 (26.1~2월 전체): {len(df_db):,}건")
print(f"   DB 부산 관련: {len(df_db_busan):,}건")

db_refs = df_db_busan['cntrctRefNo'].dropna().astype(str).str.strip().tolist()

# thtmCntrctAmt / totCntrctAmt
df_db_busan['thtm'] = pd.to_numeric(df_db_busan['thtmCntrctAmt'], errors='coerce')
df_db_busan['tot'] = pd.to_numeric(df_db_busan['totCntrctAmt'], errors='coerce')
df_db_busan['amt'] = df_db_busan['thtm'].where(df_db_busan['thtm'].notna() & (df_db_busan['thtm'] != 0), df_db_busan['tot'])

print(f"   DB thtmCntrctAmt우선 합계: {df_db_busan['amt'].sum():,.0f}원")
print(f"   DB totCntrctAmt 합계: {df_db_busan['tot'].sum():,.0f}원")

# 매칭: 엑셀 계약납품통합번호 vs DB cntrctRefNo (부분매칭)
excel_keys = df['계약납품통합번호'].dropna().astype(str).str.strip().unique()

matched = 0
unmatched_list = []
for ek in excel_keys:
    if any(ek in dr for dr in db_refs):
        matched += 1
    else:
        unmatched_list.append(ek)

print(f"\n📌 매칭 결과:")
print(f"  엑셀 고유 계약번호: {len(excel_keys)}개")
print(f"  DB cntrctRefNo 매칭: {matched}개 ({matched/len(excel_keys)*100:.1f}%)")
print(f"  미매칭: {len(unmatched_list)}개")

# 미매칭 상세
if unmatched_list:
    print(f"\n⚠️ 미매칭 건 (계약번호 형식별):")
    r_type = [k for k in unmatched_list if k.startswith('R')]
    num_type = [k for k in unmatched_list if not k.startswith('R')]
    print(f"  R-형식: {len(r_type)}개 (DB 미수집 가능)")
    print(f"  숫자형식: {len(num_type)}개 (과거연도 장기계속)")

conn.close()
