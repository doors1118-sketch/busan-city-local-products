"""
종합쇼핑몰 지역외 유출 품목 분석 (Shopping Mall Local Leakage Analyzer)
=====================================================================
부산 소재 수요기관의 종합쇼핑몰 구매 중, 부산 외 업체에 발주된
(=지역외 유출) 금액이 큰 품목을 물품분류 / 세부품명 기준으로 추출

사용법:
  python analyze_shopping_leakage.py              # 전체 분석
  python analyze_shopping_leakage.py --top 20     # 상위 20위
  python analyze_shopping_leakage.py --group 부산광역시  # 부산시 소속만
"""
import sqlite3, pandas as pd, sys, argparse

sys.stdout.reconfigure(encoding='utf-8')

DB_PROCUREMENT = 'procurement_contracts.db'
DB_AGENCIES = 'busan_agencies_master.db'
DB_COMPANIES = 'busan_companies_master.db'

def fmt(val):
    if abs(val) >= 1e12: return f"{val/1e12:,.1f}조"
    if abs(val) >= 1e8:  return f"{val/1e8:,.0f}억"
    if abs(val) >= 1e4:  return f"{val/1e4:,.0f}만"
    return f"{val:,.0f}"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--top', type=int, default=10, help='상위 N위 (기본 10)')
    parser.add_argument('--group', type=str, default=None, help='대분류 필터 (예: 부산광역시)')
    args = parser.parse_args()
    
    # 1. 마스터 로드
    conn_ag = sqlite3.connect(DB_AGENCIES)
    master = pd.read_sql("SELECT dminsttCd, cate_lrg, cate_mid, cate_sml FROM agency_master", conn_ag)
    conn_ag.close()
    master['dminsttCd'] = master['dminsttCd'].astype(str).str.strip()
    master_dict = dict(zip(master['dminsttCd'], master['cate_lrg']))
    master_codes = set(master['dminsttCd'])
    
    conn_cp = sqlite3.connect(DB_COMPANIES)
    df_cp = pd.read_sql("SELECT bizno FROM company_master", conn_cp)
    conn_cp.close()
    busan_biznos = set(df_cp['bizno'].dropna().astype(str).str.replace('-','',regex=False).str.strip())
    
    # 2. 쇼핑몰 데이터
    conn = sqlite3.connect(DB_PROCUREMENT)
    df = pd.read_sql("""
        SELECT dminsttCd, prdctAmt, prdctClsfcNoNm, dtilPrdctClsfcNoNm,
               cntrctCorpBizno, dlvrReqNo, prdctSno, dlvrReqChgOrd, corpNm
        FROM shopping_cntrct
    """, conn)
    conn.close()
    
    # 중복 제거
    df['dlvrReqChgOrd_n'] = pd.to_numeric(df['dlvrReqChgOrd'], errors='coerce').fillna(0)
    df.sort_values('dlvrReqChgOrd_n', ascending=False, inplace=True)
    df.drop_duplicates(subset=['dlvrReqNo', 'prdctSno'], keep='first', inplace=True)
    
    # 부산 수요기관 필터
    df['_cd'] = df['dminsttCd'].astype(str).str.strip()
    df = df[df['_cd'].isin(master_codes)].copy()
    df['amt'] = pd.to_numeric(df['prdctAmt'], errors='coerce').fillna(0)
    
    # 대분류 필터
    df['_grp'] = df['_cd'].map(master_dict)
    if args.group:
        df = df[df['_grp'].str.contains(args.group, na=False)]
        print(f"  🔍 필터: '{args.group}' → {len(df):,}건\n")
    
    # 지역/비지역 판별
    df['_biz'] = df['cntrctCorpBizno'].astype(str).str.replace('-','',regex=False).str.strip()
    df['is_local'] = df['_biz'].isin(busan_biznos)
    df['local_amt'] = df['amt'] * df['is_local'].astype(int)
    df['leak_amt'] = df['amt'] * (~df['is_local']).astype(int)
    
    total_amt = df['amt'].sum()
    total_local = df['local_amt'].sum()
    total_leak = df['leak_amt'].sum()
    local_rate = total_local / total_amt * 100 if total_amt > 0 else 0
    
    print("=" * 90)
    print("  📊 종합쇼핑몰 지역외 유출 품목 분석")
    print("=" * 90)
    print(f"  총 구매액: {fmt(total_amt)} | 지역수주: {fmt(total_local)} ({local_rate:.1f}%) | 유출: {fmt(total_leak)} ({100-local_rate:.1f}%)")
    
    # ============================================================
    # A. 물품분류(중분류) 기준 Top N
    # ============================================================
    grp_cls = df.groupby('prdctClsfcNoNm').agg(
        총액=('amt', 'sum'),
        지역=('local_amt', 'sum'),
        유출=('leak_amt', 'sum'),
        건수=('amt', 'count')
    ).reset_index()
    grp_cls['유출율'] = (grp_cls['유출'] / grp_cls['총액'] * 100).round(1)
    grp_cls.sort_values('유출', ascending=False, inplace=True)
    
    print(f"\n{'='*90}")
    print(f"  🏷️ [물품분류 기준] 지역외 유출 Top {args.top}")
    print(f"{'='*90}")
    print(f"  {'No':>3s} {'물품분류':30s} {'총액':>10s} {'유출액':>10s} {'유출율':>6s} {'건수':>6s}")
    print(f"  {'-'*80}")
    for i, (_, r) in enumerate(grp_cls.head(args.top).iterrows(), 1):
        bar = '█' * int(r['유출율'] / 5)
        print(f"  {i:3d} {str(r['prdctClsfcNoNm'])[:30]:30s} {fmt(r['총액']):>10s} {fmt(r['유출']):>10s} {r['유출율']:>5.1f}% {r['건수']:>5,}건 {bar}")
    
    # ============================================================
    # B. 세부품명 기준 Top N
    # ============================================================
    grp_dtl = df.groupby('dtilPrdctClsfcNoNm').agg(
        총액=('amt', 'sum'),
        지역=('local_amt', 'sum'),
        유출=('leak_amt', 'sum'),
        건수=('amt', 'count')
    ).reset_index()
    grp_dtl['유출율'] = (grp_dtl['유출'] / grp_dtl['총액'] * 100).round(1)
    grp_dtl.sort_values('유출', ascending=False, inplace=True)
    
    print(f"\n{'='*90}")
    print(f"  📦 [세부품명 기준] 지역외 유출 Top {args.top}")
    print(f"{'='*90}")
    print(f"  {'No':>3s} {'세부품명':30s} {'총액':>10s} {'유출액':>10s} {'유출율':>6s} {'건수':>6s}")
    print(f"  {'-'*80}")
    for i, (_, r) in enumerate(grp_dtl.head(args.top).iterrows(), 1):
        bar = '█' * int(r['유출율'] / 5)
        print(f"  {i:3d} {str(r['dtilPrdctClsfcNoNm'])[:30]:30s} {fmt(r['총액']):>10s} {fmt(r['유출']):>10s} {r['유출율']:>5.1f}% {r['건수']:>5,}건 {bar}")
    
    # ============================================================
    # C. 유출율이 높은 품목 (금액 1억 이상 + 유출율 80% 이상)
    # ============================================================
    high_leak = grp_cls[(grp_cls['총액'] >= 1e8) & (grp_cls['유출율'] >= 80)].sort_values('유출율', ascending=False)
    
    if len(high_leak) > 0:
        print(f"\n{'='*90}")
        print(f"  🚨 [경고] 유출율 80% 이상 + 총액 1억 이상 품목 ({len(high_leak)}건)")
        print(f"{'='*90}")
        print(f"  {'물품분류':30s} {'총액':>10s} {'유출액':>10s} {'유출율':>6s}")
        print(f"  {'-'*66}")
        for _, r in high_leak.head(20).iterrows():
            print(f"  {str(r['prdctClsfcNoNm'])[:30]:30s} {fmt(r['총액']):>10s} {fmt(r['유출']):>10s} {r['유출율']:>5.1f}%")
    
    # ============================================================
    # D. 대분류별 유출 요약
    # ============================================================
    grp_lrg = df.groupby('_grp').agg(
        총액=('amt', 'sum'),
        유출=('leak_amt', 'sum'),
    ).reset_index()
    grp_lrg['유출율'] = (grp_lrg['유출'] / grp_lrg['총액'] * 100).round(1)
    
    print(f"\n{'='*90}")
    print(f"  📁 대분류별 유출 요약")
    print(f"{'='*90}")
    for _, r in grp_lrg.sort_values('유출', ascending=False).iterrows():
        bar = '█' * int(r['유출율'] / 3)
        print(f"  {str(r['_grp']):25s} 총액 {fmt(r['총액']):>10s} | 유출 {fmt(r['유출']):>10s} ({r['유출율']:.1f}%) {bar}")

if __name__ == '__main__':
    main()
