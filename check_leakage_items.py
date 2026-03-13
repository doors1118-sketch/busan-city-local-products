"""
유출 품목별 부산 지역업체 현황 분석
===================================
Top 유출 품목에 대해:
1. 해당 품목을 이미 공급하고 있는 부산 업체가 있는지
2. 있다면 몇 개나 되고, 얼마나 공급했는지 
3. 왜 타지역 업체로 유출되었는지 (가격? 물량? 업체 부재?)
"""
import sqlite3, pandas as pd, sys
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')

DB_PROCUREMENT = 'procurement_contracts.db'
DB_AGENCIES = 'busan_agencies_master.db'
DB_COMPANIES = 'busan_companies_master.db'

conn = sqlite3.connect(DB_PROCUREMENT)
conn_ag = sqlite3.connect(DB_AGENCIES)
conn_cp = sqlite3.connect(DB_COMPANIES)

# 마스터 로드
busan_cds = set(pd.read_sql("SELECT dminsttCd FROM agency_master", conn_ag)['dminsttCd'].astype(str).str.strip())
biznos = set(pd.read_sql("SELECT bizno FROM company_master", conn_cp)['bizno']
            .dropna().astype(str).str.replace('-','',regex=False).str.strip())

# 업체명 dict
corp_names = dict(conn_cp.execute("SELECT bizno, corpNm FROM company_master").fetchall())

# 전체 쇼핑몰 데이터 로드
df = pd.read_sql("""SELECT prdctClsfcNoNm, dtilPrdctClsfcNoNm, prdctAmt, 
    cntrctCorpBizno, corpNm, dminsttCd, dlvrReqNo, prdctSno, dlvrReqChgOrd
    FROM shopping_cntrct""", conn)

# 중복 제거
df['dlvrReqChgOrd_n'] = pd.to_numeric(df['dlvrReqChgOrd'], errors='coerce').fillna(0)
df.sort_values('dlvrReqChgOrd_n', ascending=False, inplace=True)
df.drop_duplicates(subset=['dlvrReqNo','prdctSno'], keep='first', inplace=True)

# 부산 수요기관만
df['_cd'] = df['dminsttCd'].astype(str).str.strip()
df = df[df['_cd'].isin(busan_cds)].copy()
df['amt'] = pd.to_numeric(df['prdctAmt'], errors='coerce').fillna(0)
df['_biz'] = df['cntrctCorpBizno'].astype(str).str.replace('-','',regex=False).str.strip()
df['is_local'] = df['_biz'].isin(biznos)

print("=" * 100)
print("  📊 유출 품목별 부산 지역업체 공급 현황 분석")
print("=" * 100)
print(f"  분석 대상: 부산 수요기관 쇼핑몰 {len(df):,}건\n")

# ===== 1단계: 물품분류별 유출/지역 집계 =====
item_stats = df.groupby('prdctClsfcNoNm').agg(
    총액=('amt', 'sum'),
    건수=('amt', 'count'),
    지역액=('amt', lambda x: x[df.loc[x.index, 'is_local']].sum()),
    유출액=('amt', lambda x: x[~df.loc[x.index, 'is_local']].sum()),
).reset_index()
item_stats['유출율'] = (item_stats['유출액'] / item_stats['총액'] * 100).round(1)

# 유출액 Top 15
top_leak = item_stats.sort_values('유출액', ascending=False).head(15)

print(f"{'─'*100}")
print(f"  🏷️  유출액 Top 15 품목 — 부산 공급업체 현황")
print(f"{'─'*100}\n")

for rank, (_, row) in enumerate(top_leak.iterrows(), 1):
    item_nm = row['prdctClsfcNoNm']
    tot = row['총액']
    leak = row['유출액']
    lr = row['유출율']
    
    # 해당 품목 부산업체 vs 타지역 업체 현황
    item_df = df[df['prdctClsfcNoNm'] == item_nm]
    
    # 부산 업체 목록 (고유)
    local_corps = item_df[item_df['is_local']].groupby('_biz').agg(
        공급액=('amt', 'sum'), 건수=('amt', 'count')
    ).reset_index().sort_values('공급액', ascending=False)
    
    # 타지역 업체 목록 (고유)
    nonlocal_corps = item_df[~item_df['is_local']].groupby(['_biz', 'corpNm']).agg(
        공급액=('amt', 'sum'), 건수=('amt', 'count')
    ).reset_index().sort_values('공급액', ascending=False)
    
    # 부산 업체명 매핑
    local_corps['업체명'] = local_corps['_biz'].apply(
        lambda x: corp_names.get(x, item_df[item_df['_biz']==x]['corpNm'].iloc[0] if len(item_df[item_df['_biz']==x]) > 0 else ''))
    
    n_local = len(local_corps)
    n_nonlocal = len(nonlocal_corps)
    local_total = local_corps['공급액'].sum()
    
    # 출력
    status = "🔴 부산업체 없음" if n_local == 0 else f"🟢 부산 {n_local}개사" if lr < 50 else f"🟡 부산 {n_local}개사 (유출 {lr}%)"
    
    print(f"  [{rank:2d}] {item_nm}")
    print(f"       총액 {tot/1e8:,.0f}억 | 유출 {leak/1e8:,.0f}억 ({lr}%) | "
          f"부산 {n_local}개사 {local_total/1e8:,.0f}억 | 타지역 {n_nonlocal}개사 | {status}")
    
    # 부산 주요 공급업체 Top 3
    if n_local > 0:
        print(f"       ├─ 부산 주요: ", end="")
        for i, (_, lc) in enumerate(local_corps.head(3).iterrows()):
            nm = str(lc['업체명'])[:15] if lc['업체명'] else lc['_biz'][:10]
            print(f"{nm}({lc['공급액']/1e8:.1f}억/{lc['건수']}건)", end="  ")
        print()
    
    # 타지역 주요 공급업체 Top 3
    if n_nonlocal > 0:
        print(f"       └─ 타지역 주요: ", end="")
        for i, (_, nc) in enumerate(nonlocal_corps.head(3).iterrows()):
            nm = str(nc['corpNm'])[:15] if nc['corpNm'] else nc['_biz'][:10]
            print(f"{nm}({nc['공급액']/1e8:.1f}억/{nc['건수']}건)", end="  ")
        print()
    
    print()

# ===== 2단계: 유출 원인 분석 요약 =====
print(f"\n{'='*100}")
print(f"  📋 유출 원인 유형 분류")
print(f"{'='*100}\n")

no_local = []     # 부산업체 전혀 없음
has_local = []    # 부산업체 있는데도 유출
low_local = []    # 부산업체 1-2개뿐

for _, row in top_leak.iterrows():
    item_nm = row['prdctClsfcNoNm']
    item_df = df[df['prdctClsfcNoNm'] == item_nm]
    n_local = item_df[item_df['is_local']]['_biz'].nunique()
    
    if n_local == 0:
        no_local.append((item_nm, row['유출액']))
    elif n_local <= 2:
        low_local.append((item_nm, row['유출액'], n_local))
    else:
        has_local.append((item_nm, row['유출액'], n_local))

if no_local:
    print(f"  🔴 부산 공급업체 전무 ({len(no_local)}개 품목):")
    for nm, amt in no_local:
        print(f"     → {nm} (유출 {amt/1e8:,.0f}억)")

if low_local:
    print(f"\n  🟡 부산 공급업체 부족 (1~2개사, {len(low_local)}개 품목):")
    for nm, amt, n in low_local:
        print(f"     → {nm} ({n}개사, 유출 {amt/1e8:,.0f}억)")

if has_local:
    print(f"\n  🟢 부산 공급업체 있음에도 유출 ({len(has_local)}개 품목):")
    for nm, amt, n in has_local:
        print(f"     → {nm} ({n}개사, 유출 {amt/1e8:,.0f}억)")
    print(f"\n     ⓘ 가능 원인: 물량 부족, 가격 경쟁력, 납기 이슈, 특수 사양 요구")

conn.close()
conn_ag.close()
conn_cp.close()
print(f"\n{'='*100}")
print("완료.")
