import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn_pr = sqlite3.connect('procurement_contracts.db')
conn_ag = sqlite3.connect('busan_agencies_master.db')
conn_cp = sqlite3.connect('busan_companies_master.db')

busan_codes = set(pd.read_sql("SELECT dminsttCd FROM agency_master", conn_ag)['dminsttCd'].astype(str).str.strip())
busan_biznos = set(pd.read_sql("SELECT bizno FROM company_master", conn_cp)['bizno'].dropna().astype(str).str.replace('-','',regex=False).str.strip())
conn_ag.close(); conn_cp.close()

# 1월 종합쇼핑몰 부산 기관 데이터 (중복제거)
df = pd.read_sql("""
    SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, dminsttCd, prdctAmt, 
           cntrctCorpBizno, prdctClsfcNoNm, dtilPrdctClsfcNoNm
    FROM shopping_cntrct
    WHERE dlvrReqRcptDate >= '2026-01-01' AND dlvrReqRcptDate <= '2026-01-31'
""", conn_pr)
conn_pr.close()

df.sort_values('dlvrReqChgOrd', ascending=False, inplace=True)
df.drop_duplicates(subset=['dlvrReqNo','prdctSno'], keep='first', inplace=True)

# 부산 기관만
df['dminsttCd'] = df['dminsttCd'].astype(str).str.strip()
df = df[df['dminsttCd'].isin(busan_codes)]
df['amt'] = pd.to_numeric(df['prdctAmt'], errors='coerce').fillna(0)
df['biz'] = df['cntrctCorpBizno'].astype(str).str.replace('-','',regex=False).str.strip()
df['is_local'] = df['biz'].isin(busan_biznos)

print(f"📊 부산 기관 종합쇼핑몰 1월 (중복제거): {len(df):,}건")
print(f"   총 발주액: {df['amt'].sum():,.0f}원")
print(f"   지역업체: {df[df['is_local']]['amt'].sum():,.0f}원 ({df[df['is_local']]['amt'].sum()/df['amt'].sum()*100:.1f}%)")
print(f"   지역외:   {df[~df['is_local']]['amt'].sum():,.0f}원 ({df[~df['is_local']]['amt'].sum()/df['amt'].sum()*100:.1f}%)")

# 품목별 집계
grouped = df.groupby('prdctClsfcNoNm').agg(
    총액=('amt', 'sum'),
    건수=('amt', 'count'),
).reset_index()

# 지역외 금액
outflow = df[~df['is_local']].groupby('prdctClsfcNoNm')['amt'].sum().reset_index()
outflow.columns = ['prdctClsfcNoNm', '지역외금액']

result = grouped.merge(outflow, on='prdctClsfcNoNm', how='left')
result['지역외금액'] = result['지역외금액'].fillna(0)
result['유출비중'] = result['지역외금액'] / result['총액'] * 100

# 유출금액 기준 상위 10
top10 = result.sort_values('지역외금액', ascending=False).head(10)
print(f"\n🔴 지역외 유출 금액 TOP 10 품목:")
print("-" * 80)
print(f"{'순위':>3} | {'품목분류명':30s} | {'총발주액':>15s} | {'유출금액':>15s} | {'유출비중':>6s} | 건수")
print("-" * 80)
for rank, (_, r) in enumerate(top10.iterrows(), 1):
    print(f"{rank:3d} | {str(r['prdctClsfcNoNm'])[:30]:30s} | {r['총액']:>15,.0f} | {r['지역외금액']:>15,.0f} | {r['유출비중']:>5.1f}% | {r['건수']:,}")
