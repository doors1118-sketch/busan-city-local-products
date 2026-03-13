import pandas as pd
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

EXCEL_PATH = r'C:\Users\COMTREE\Desktop\연습\부산환경공단 종합쇼핑몰 납품요구 물품 내역{20년 1월이후자료(조회속도향상)}.xlsx'

# 헤더 찾기
df_raw = pd.read_excel(EXCEL_PATH, header=None, nrows=15)
header_row = None
for i, row in df_raw.iterrows():
    vals = [str(v) for v in row.values if str(v) != 'nan']
    if vals:
        short = vals[0][:15] if vals else ''
        print(f"  행{i:2d}: {short}... ({len(vals)}컬럼)")
        if any('납품요구' in str(v) for v in vals):
            header_row = i

print(f"\n✅ 헤더 행: {header_row}")

df = pd.read_excel(EXCEL_PATH, header=header_row)
print(f"전체 행수: {len(df):,}")
print(f"컬럼: {list(df.columns[:6])}")

# 수요기관코드
if '수요기관코드' in df.columns:
    print(f"\n수요기관코드: {df['수요기관코드'].unique()}")
    print(f"수요기관명: {df['수요기관명'].unique()}")
    agency_codes = df['수요기관코드'].dropna().astype(str).str.strip().unique()
else:
    print("수요기관코드 컬럼 없음")
    agency_codes = []

# 금액 컬럼
amt_cols = [c for c in df.columns if '금액' in str(c) or '단가' in str(c)]
print(f"\n💰 금액 컬럼:")
for col in amt_cols:
    vals = pd.to_numeric(df[col], errors='coerce')
    print(f"  - {col}: 합계={vals.sum():,.0f}")

# DB 비교
conn = sqlite3.connect('procurement_contracts.db')

for ac in agency_codes:
    df_db = pd.read_sql(f"""
        SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, dminsttCd, prdctAmt, dlvrReqAmt,
               corpNm, cntrctCorpBizno, dlvrReqRcptDate
        FROM shopping_cntrct 
        WHERE dlvrReqRcptDate >= '2026-01-01' AND dlvrReqRcptDate <= '2026-02-28'
          AND dminsttCd = '{ac}'
    """, conn)
    
    df_db.sort_values('dlvrReqChgOrd', ascending=False, inplace=True)
    df_db_dedup = df_db.drop_duplicates(subset=['dlvrReqNo', 'prdctSno'], keep='first').copy()
    
    print(f"\n📋 기관코드 {ac}:")
    print(f"  DB(중복제거): {len(df_db_dedup):,}건")
    
    df_db_dedup['prdctAmt_num'] = pd.to_numeric(df_db_dedup['prdctAmt'], errors='coerce')
    print(f"  DB prdctAmt 합계: {df_db_dedup['prdctAmt_num'].sum():,.0f}원")

# 매칭
if '납품요구번호' in df.columns and len(agency_codes) > 0:
    excel_reqnos = set(df['납품요구번호'].dropna().astype(str).str.strip())
    
    all_codes = "','".join(agency_codes)
    df_db_all = pd.read_sql(f"""
        SELECT DISTINCT dlvrReqNo FROM shopping_cntrct
        WHERE dlvrReqRcptDate >= '2026-01-01' AND dlvrReqRcptDate <= '2026-02-28'
          AND dminsttCd IN ('{all_codes}')
    """, conn)
    db_reqnos = set(df_db_all['dlvrReqNo'].astype(str).str.strip())
    
    only_excel = excel_reqnos - db_reqnos
    only_db = db_reqnos - excel_reqnos
    
    print(f"\n📌 납품요구번호 매칭:")
    print(f"  엑셀: {len(excel_reqnos):,}개 / DB: {len(db_reqnos):,}개")
    print(f"  양쪽 모두: {len(excel_reqnos & db_reqnos):,}개")
    print(f"  엑셀에만: {len(only_excel):,}개")
    print(f"  DB에만: {len(only_db):,}개")

conn.close()
