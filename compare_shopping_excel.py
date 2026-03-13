import pandas as pd
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

EXCEL_PATH = r'C:\Users\COMTREE\Desktop\연습\부산광역시 종합쇼핑몰 납품요구 물품 내역{20년 1월이후자료(조회속도향상)}.xlsx'

# 헤더 행 = 10
df = pd.read_excel(EXCEL_PATH, header=10)
print(f"📊 엑셀 컬럼명:\n  {list(df.columns)}")
print(f"전체 행수: {len(df):,}")

# 금액 관련 컬럼
amt_cols = [c for c in df.columns if '금액' in str(c) or '단가' in str(c) or '수량' in str(c)]
print(f"\n💰 금액/단가/수량 컬럼: {amt_cols}")

for col in amt_cols:
    vals = pd.to_numeric(df[col], errors='coerce')
    print(f"  - {col}: 합계={vals.sum():,.0f} / 평균={vals.mean():,.0f}")

# 주요 컬럼 통계
print(f"\n📋 샘플 데이터 (처음 3건):")
print(df.head(3).to_string())

# 2. DB 비교
print("\n" + "=" * 70)
print("🔍 DB(shopping_cntrct)와 비교")
print("=" * 70)

conn = sqlite3.connect('procurement_contracts.db')

# 부산 기관(코드 6260000 = 부산광역시)
df_db = pd.read_sql("""
    SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, prdctAmt, dlvrReqAmt, 
           prdctUprc, prdctQty, dminsttCd, dminsttNm, dlvrReqRcptDate,
           cntrctCnclsStleNm, corpNm, cntrctCorpBizno
    FROM shopping_cntrct 
    WHERE dlvrReqRcptDate >= '2026-01-01' AND dlvrReqRcptDate <= '2026-03-05'
      AND dminsttCd = '6260000'
""", conn)
conn.close()

print(f"\n엑셀: {len(df):,}건 (부산광역시 수요기관코드=6260000)")
print(f" DB:   {len(df_db):,}건 (dminsttCd=6260000, 26.1.1~3.5)")

# 금액 비교
df_db['prdctAmt_num'] = pd.to_numeric(df_db['prdctAmt'], errors='coerce')
df_db['dlvrReqAmt_num'] = pd.to_numeric(df_db['dlvrReqAmt'], errors='coerce')

print(f"\n DB prdctAmt 합계:   {df_db['prdctAmt_num'].sum():,.0f}원")
print(f" DB dlvrReqAmt 합계: {df_db['dlvrReqAmt_num'].sum():,.0f}원")

for col in amt_cols:
    vals = pd.to_numeric(df[col], errors='coerce')
    print(f" 엑셀 {col} 합계: {vals.sum():,.0f}원")

# 매칭 비교: dlvrReqNo (납품요구번호) 기준
excel_reqnos = set(df['납품요구번호'].dropna().astype(str).str.strip()) if '납품요구번호' in df.columns else set()
db_reqnos = set(df_db['dlvrReqNo'].dropna().astype(str).str.strip())

print(f"\n 엑셀 납품요구번호 고유값: {len(excel_reqnos):,}개")
print(f" DB 납품요구번호 고유값: {len(db_reqnos):,}개")

only_excel = excel_reqnos - db_reqnos
only_db = db_reqnos - excel_reqnos
both = excel_reqnos & db_reqnos

print(f" 엑셀에만: {len(only_excel):,}개")
print(f" DB에만: {len(only_db):,}개")
print(f" 양쪽 모두: {len(both):,}개")

if only_excel:
    print(f"\n 📌 엑셀에만 있는 납품요구번호 (최대 5개):")
    for r in list(only_excel)[:5]:
        print(f"   {r}")
