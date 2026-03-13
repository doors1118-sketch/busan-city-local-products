import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')

# 아까 뽑았던 부산교통공사 사상-하단선 공사 건을 기준으로 totCntrctAmt vs thtmCntrctAmt 비교
print("🔍 [totCntrctAmt vs thtmCntrctAmt 비교 분석]")
print("="*70)

tables = [
    ('cnstwk_cntrct', 'cnstwkNm', '공사'),
    ('servc_cntrct', 'cntrctNm', '용역'),
    ('thng_cntrct', 'cntrctNm', '물품'),
]

for table, nm_col, label in tables:
    df = pd.read_sql(f"""
        SELECT untyCntrctNo, {nm_col} as cntrctNm, cntrctInsttNm, 
               totCntrctAmt, thtmCntrctAmt, cntrctDate
        FROM {table}
        WHERE cntrctDate >= '2026-01-01' AND cntrctDate <= '2026-01-31'
    """, conn)
    
    df['totCntrctAmt'] = pd.to_numeric(df['totCntrctAmt'], errors='coerce').fillna(0)
    df['thtmCntrctAmt'] = pd.to_numeric(df['thtmCntrctAmt'], errors='coerce').fillna(0)
    
    # totCntrctAmt 와 thtmCntrctAmt 이 다른 건 = 장기계속계약 가능성 높음
    diff_mask = (df['totCntrctAmt'] != df['thtmCntrctAmt']) & (df['thtmCntrctAmt'] > 0)
    df_diff = df[diff_mask]
    
    tot_sum = df['totCntrctAmt'].sum()
    thtm_sum_all = df.apply(lambda r: r['thtmCntrctAmt'] if r['thtmCntrctAmt'] > 0 else r['totCntrctAmt'], axis=1).sum()
    
    print(f"\n📌 [{label}] 26년 1월 (총 {len(df):,}건)")
    print(f"   - totCntrctAmt(총계약금액) 합계:  {tot_sum:,.0f}원")
    print(f"   - thtmCntrctAmt 우선 적용 합계:   {thtm_sum_all:,.0f}원")
    print(f"   - 차이금:                          {tot_sum - thtm_sum_all:,.0f}원")
    print(f"   - 두 금액이 다른 건수:             {len(df_diff):,}건 (전체 중 {len(df_diff)/len(df)*100:.1f}%)")
    
    # 차이 금액 최대인 건 5개 출력
    if not df_diff.empty:
        df_diff = df_diff.copy()
        df_diff['gap'] = df_diff['totCntrctAmt'] - df_diff['thtmCntrctAmt']
        top5 = df_diff.nlargest(3, 'gap')
        print(f"   [차이 최대 3건 예시]")
        for _, r in top5.iterrows():
            print(f"   ▶ {r['cntrctNm'][:25]}... | 총액:{r['totCntrctAmt']:,.0f} → 당회차:{r['thtmCntrctAmt']:,.0f} (차이:{r['gap']:,.0f})")

conn.close()
