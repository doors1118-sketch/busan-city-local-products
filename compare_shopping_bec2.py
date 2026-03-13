import pandas as pd
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

EXCEL_PATH = r'C:\Users\COMTREE\Desktop\연습\부산환경공단 종합쇼핑몰 납품요구 물품 내역{20년 1월이후자료(조회속도향상)}.xlsx'
df = pd.read_excel(EXCEL_PATH, header=2)

# 부산환경공단 기관코드 목록
bec_codes = df['수요기관코드'].dropna().astype(str).str.strip().unique()
bec_names = dict(zip(df['수요기관코드'].astype(str).str.strip(), df['수요기관명']))

# 1. 마스터DB에 등록 여부 확인
conn_ag = sqlite3.connect('busan_agencies_master.db')
df_ag = pd.read_sql("SELECT dminsttCd, cate_lrg, cate_mid FROM agency_master", conn_ag)
conn_ag.close()
master_codes = set(df_ag['dminsttCd'].astype(str).str.strip())

print("📋 [1] 부산환경공단 사업소 마스터DB 등록 확인")
print("-" * 60)
for code in sorted(bec_codes):
    name = bec_names.get(code, '?')
    in_master = '✅' if code in master_codes else '❌ 미등록!'
    if code in master_codes:
        row = df_ag[df_ag['dminsttCd'].astype(str).str.strip() == code].iloc[0]
        print(f"  {in_master} {code} | {name:20s} | {row['cate_lrg']} > {row['cate_mid']}")
    else:
        print(f"  {in_master} {code} | {name}")

# 2. 금액 차이 원인 분석 (행 수준 비교)
print("\n" + "=" * 60)
print("💰 [2] 금액 차이 원인 분석 (행 수준 비교)")
print("=" * 60)

# 엑셀 키 생성
df['key'] = df['납품요구번호'].astype(str).str.strip() + '_' + df['납품요구변경차수'].astype(str).str.strip() + '_' + df['납품요구물품순번'].astype(str).str.strip()
df['납품금액_num'] = pd.to_numeric(df['납품금액'], errors='coerce').fillna(0)

# DB
conn = sqlite3.connect('procurement_contracts.db')
all_codes_str = "','".join(bec_codes)
df_db = pd.read_sql(f"""
    SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, prdctAmt, dminsttCd
    FROM shopping_cntrct 
    WHERE dlvrReqRcptDate >= '2026-01-01' AND dlvrReqRcptDate <= '2026-02-28'
      AND dminsttCd IN ('{all_codes_str}')
""", conn)
conn.close()

df_db['key'] = df_db['dlvrReqNo'].astype(str).str.strip() + '_' + df_db['dlvrReqChgOrd'].astype(str).str.strip() + '_' + df_db['prdctSno'].astype(str).str.strip()
df_db['prdctAmt_num'] = pd.to_numeric(df_db['prdctAmt'], errors='coerce').fillna(0)

# 키-금액 비교
excel_dict = df.set_index('key')['납품금액_num'].to_dict()
db_dict = df_db.set_index('key')['prdctAmt_num'].to_dict()

all_keys = set(excel_dict.keys()) | set(db_dict.keys())
diff_rows = []
for k in all_keys:
    e_amt = excel_dict.get(k, None)
    d_amt = db_dict.get(k, None)
    if e_amt is None:
        diff_rows.append({'key': k, 'excel': 0, 'db': d_amt, 'diff': d_amt, 'reason': 'DB에만 존재'})
    elif d_amt is None:
        diff_rows.append({'key': k, 'excel': e_amt, 'db': 0, 'diff': -e_amt, 'reason': '엑셀에만 존재'})
    elif abs(e_amt - d_amt) > 1:
        diff_rows.append({'key': k, 'excel': e_amt, 'db': d_amt, 'diff': d_amt - e_amt, 'reason': '금액 불일치'})

if diff_rows:
    df_diff = pd.DataFrame(diff_rows).sort_values('diff', key=abs, ascending=False)
    print(f"\n차이 발생 건수: {len(df_diff)}")
    print(f"차이 합계: {df_diff['diff'].sum():,.0f}원")
    print(f"\n상위 10건:")
    for _, r in df_diff.head(10).iterrows():
        print(f"  {r['key']} | 엑셀={r['excel']:,.0f} | DB={r['db']:,.0f} | 차이={r['diff']:,.0f} | {r['reason']}")
else:
    print("차이 없음! 완벽 일치!")
