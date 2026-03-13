import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')

# 1. 쇼핑몰에서 기관명에 '강서' 또는 '서구'가 포함된 전체 기관 조회
print("=" * 80)
print("🔍 [1] 종합쇼핑몰 dminsttNm에 '강서' 포함된 기관 (전국)")
print("=" * 80)

df_gs = pd.read_sql("""
    SELECT dminsttCd, dminsttNm, COUNT(*) as cnt, 
           SUM(CAST(prdctAmt AS REAL)) as total_amt
    FROM shopping_cntrct
    WHERE dlvrReqRcptDate >= '2026-01-01' AND dlvrReqRcptDate <= '2026-02-28'
      AND dminsttNm LIKE '%강서%'
    GROUP BY dminsttCd, dminsttNm
    ORDER BY total_amt DESC
""", conn)

for _, r in df_gs.iterrows():
    is_busan = '부산' in str(r['dminsttNm'])
    marker = '✅ 부산' if is_busan else '⚠️ 타지역'
    print(f"  {marker} | {r['dminsttCd']:10s} | {r['dminsttNm']:40s} | {r['cnt']:,}건 | {r['total_amt']:>15,.0f}원")

print(f"\n" + "=" * 80)
print("🔍 [2] 종합쇼핑몰 dminsttNm에 '서구'가 포함된 기관 (전국)")
print("=" * 80)

df_sg = pd.read_sql("""
    SELECT dminsttCd, dminsttNm, COUNT(*) as cnt, 
           SUM(CAST(prdctAmt AS REAL)) as total_amt
    FROM shopping_cntrct
    WHERE dlvrReqRcptDate >= '2026-01-01' AND dlvrReqRcptDate <= '2026-02-28'
      AND dminsttNm LIKE '%서구%'
    GROUP BY dminsttCd, dminsttNm
    ORDER BY total_amt DESC
""", conn)

for _, r in df_sg.iterrows():
    is_busan = '부산' in str(r['dminsttNm'])
    marker = '✅ 부산' if is_busan else '⚠️ 타지역'
    print(f"  {marker} | {r['dminsttCd']:10s} | {r['dminsttNm']:40s} | {r['cnt']:,}건 | {r['total_amt']:>15,.0f}원")

# 3. 마스터DB에서 '강서' '서구' 관련 기관 분류 확인
print(f"\n" + "=" * 80)
print("🔍 [3] 마스터DB에서 '강서' 관련 기관 (부산 아닌 것이 부산으로 분류된 게 있는지)")
print("=" * 80)

conn_ag = sqlite3.connect('busan_agencies_master.db')
df_master = pd.read_sql("""
    SELECT dminsttCd, dminsttNm, cate_lrg, cate_sml 
    FROM agency_master 
    WHERE dminsttNm LIKE '%강서%' OR dminsttNm LIKE '%서구%'
    ORDER BY dminsttNm
""", conn_ag)

for _, r in df_master.iterrows():
    is_busan = '부산' in str(r['dminsttNm'])
    marker = '✅' if is_busan else '🚨 오분류!'
    print(f"  {marker} | {r['dminsttCd']:10s} | {r['dminsttNm']:40s} | {r['cate_lrg']:20s} | {r['cate_sml']}")

conn_ag.close()
conn.close()
