import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')

# 1. 변경차수 중복 확인
print("🔍 [1] 변경차수 중복 확인 (26년 1월)")
df = pd.read_sql("""
    SELECT dlvrReqNo, prdctSno, COUNT(DISTINCT dlvrReqChgOrd) as chg_cnt,
           GROUP_CONCAT(DISTINCT dlvrReqChgOrd) as chg_ords,
           SUM(CAST(prdctAmt AS REAL)) as total_amt
    FROM shopping_cntrct 
    WHERE dlvrReqRcptDate >= '2026-01-01' AND dlvrReqRcptDate <= '2026-01-31'
    GROUP BY dlvrReqNo, prdctSno
    HAVING COUNT(DISTINCT dlvrReqChgOrd) > 1
""", conn)

print(f"변경차수 2개 이상인 건: {len(df)}건")
if len(df) > 0:
    print(f"\n상위 샘플:")
    for _, r in df.head(10).iterrows():
        print(f"  {r['dlvrReqNo']} 물품순번={r['prdctSno']} | 변경차수={r['chg_ords']} | 합산금액={r['total_amt']:,.0f}")

# 2. 전체 규모 확인 (중복 포함/제외 비교)
print("\n" + "=" * 70)
print("🔍 [2] 중복 포함/제외 금액 비교 (26년 1월)")

# 중복 포함 (현재 방식)
df_all = pd.read_sql("""
    SELECT CAST(prdctAmt AS REAL) as amt FROM shopping_cntrct
    WHERE dlvrReqRcptDate >= '2026-01-01' AND dlvrReqRcptDate <= '2026-01-31'
""", conn)
print(f"  전체 행 합계 (중복 포함): {df_all['amt'].sum():,.0f}원 ({len(df_all):,}건)")

# 최신 변경차수만 (중복 제거)
df_dedup = pd.read_sql("""
    SELECT a.dlvrReqNo, a.prdctSno, a.dlvrReqChgOrd, CAST(a.prdctAmt AS REAL) as amt
    FROM shopping_cntrct a
    INNER JOIN (
        SELECT dlvrReqNo, prdctSno, MAX(dlvrReqChgOrd) as max_chg
        FROM shopping_cntrct
        WHERE dlvrReqRcptDate >= '2026-01-01' AND dlvrReqRcptDate <= '2026-01-31'
        GROUP BY dlvrReqNo, prdctSno
    ) b ON a.dlvrReqNo = b.dlvrReqNo AND a.prdctSno = b.prdctSno AND a.dlvrReqChgOrd = b.max_chg
    WHERE a.dlvrReqRcptDate >= '2026-01-01' AND a.dlvrReqRcptDate <= '2026-01-31'
""", conn)
print(f"  최신 변경차수만 (중복 제거): {df_dedup['amt'].sum():,.0f}원 ({len(df_dedup):,}건)")

diff = df_all['amt'].sum() - df_dedup['amt'].sum()
print(f"\n  🚨 차이: {diff:,.0f}원 ({diff/df_all['amt'].sum()*100:.2f}%)")

conn.close()
