import pandas as pd
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

EXCEL_PATH = r'C:\Users\COMTREE\Desktop\연습\부산광역시 종합쇼핑몰 납품요구 물품 내역{20년 1월이후자료(조회속도향상)}.xlsx'

df = pd.read_excel(EXCEL_PATH, header=10)

conn = sqlite3.connect('procurement_contracts.db')
df_db = pd.read_sql("""
    SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, prdctAmt, dlvrReqAmt, 
           prdctUprc, prdctQty, dminsttCd, cntrctCnclsStleNm, corpNm
    FROM shopping_cntrct 
    WHERE dlvrReqRcptDate >= '2026-01-01' AND dlvrReqRcptDate <= '2026-03-05'
      AND dminsttCd = '6260000'
""", conn)
conn.close()

# 키 생성: 납품요구번호 + 변경차수 + 물품순번
df['key'] = df['납품요구번호'].astype(str) + '_' + df['납품요구변경차수'].astype(str) + '_' + df['납품요구물품순번'].astype(str)
df_db['key'] = df_db['dlvrReqNo'].astype(str) + '_' + df_db['dlvrReqChgOrd'].astype(str) + '_' + df_db['prdctSno'].astype(str)

excel_keys = set(df['key'])
db_keys = set(df_db['key'])

only_db = db_keys - excel_keys
only_excel = excel_keys - db_keys

print(f"엑셀 키 수: {len(excel_keys)}")
print(f"DB 키 수:   {len(db_keys)}")
print(f"DB에만 있는 건: {len(only_db)}")
print(f"엑셀에만 있는 건: {len(only_excel)}")

if only_db:
    print("\n📌 DB에만 있는 건 상세:")
    for k in only_db:
        row = df_db[df_db['key'] == k].iloc[0]
        print(f"  {k} | {row['corpNm']} | prdctAmt={row['prdctAmt']} | {row['cntrctCnclsStleNm']}")

if only_excel:
    print("\n📌 엑셀에만 있는 건 상세:")
    for k in only_excel:
        row = df[df['key'] == k].iloc[0]
        print(f"  {k} | {row['업체명']} | 납품금액={row['납품금액']}")

# 금액 차이 분석
print("\n" + "=" * 70)
print("💰 금액 차이 상세")
db_total = pd.to_numeric(df_db['prdctAmt'], errors='coerce').sum()
excel_total = pd.to_numeric(df['납품금액'], errors='coerce').sum()
print(f"DB prdctAmt 합계:  {db_total:,.0f}원")
print(f"엑셀 납품금액 합계: {excel_total:,.0f}원")
print(f"차이: {db_total - excel_total:,.0f}원")

# DB에만 있는 건의 금액
if only_db:
    db_only_amt = df_db[df_db['key'].isin(only_db)]['prdctAmt'].astype(float).sum()
    print(f"\nDB에만 있는 건 금액: {db_only_amt:,.0f}원")
    print(f"차이 설명: DB총합 - 엑셀총합 = {db_total - excel_total:,.0f}원 ≈ DB에만있는건 {db_only_amt:,.0f}원")
