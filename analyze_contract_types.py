"""
계약유형 분석 도구 (Contract Type Analyzer)
============================================
부산 관내 수요기관별 / 물품유형별 계약유형(수의계약, 제한경쟁, 일반경쟁 등) 분석

사용법:
  python analyze_contract_types.py                  # 전체 분석 (기관그룹별 + 물품유형별)
  python analyze_contract_types.py --agency 해운대구  # 특정 기관 상세
  python analyze_contract_types.py --sector 공사      # 특정 분야만
"""
import sqlite3
import pandas as pd
import sys
import argparse

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'procurement_contracts.db'
AGENCY_DB_PATH = 'busan_agencies_master.db'

# 낙찰정보 테이블 → 분야 매핑
AWARD_TABLE_MAP = {
    '공사': 'busan_award_cnstwk',
    '용역': 'busan_award_servc',
    '물품': 'busan_award_thng',
}

def load_agency_master():
    """수요기관 마스터 로드 (대/중/소 분류 포함)"""
    conn = sqlite3.connect(AGENCY_DB_PATH)
    df = pd.read_sql_query("SELECT dminsttCd, dminsttNm, cate_lrg, cate_mid, cate_sml FROM agency_master", conn)
    conn.close()
    return df

def load_busan_award_ntce_nos():
    """낙찰정보 테이블에서 부산 지역제한 공고번호 집합을 로드"""
    conn = sqlite3.connect(DB_PATH)
    award_sets = {}
    for sector, table_name in AWARD_TABLE_MAP.items():
        try:
            df = pd.read_sql_query(f"SELECT DISTINCT bidNtceNo FROM {table_name} WHERE bidNtceNo IS NOT NULL AND bidNtceNo != ''", conn)
            award_sets[sector] = set(df['bidNtceNo'].astype(str).str.strip())
        except Exception:
            award_sets[sector] = set()
    conn.close()
    return award_sets

def refine_contract_method(df, sector_name, award_sets):
    """제한경쟁을 '지역제한경쟁' / '제한경쟁(비지역)'으로 세분화"""
    if sector_name not in award_sets or 'ntceNo' not in df.columns:
        return df
    
    ntce_set = award_sets[sector_name]
    mask_limited = df['cntrctCnclsMthdNm'] == '제한경쟁'
    
    if mask_limited.sum() == 0:
        return df
    
    # ntceNo가 낙찰정보에 있으면 지역제한경쟁
    is_regional = mask_limited & df['ntceNo'].fillna('').str.strip().isin(ntce_set)
    is_non_regional = mask_limited & ~df['ntceNo'].fillna('').str.strip().isin(ntce_set)
    
    df.loc[is_regional, 'cntrctCnclsMthdNm'] = '지역제한경쟁'
    df.loc[is_non_regional, 'cntrctCnclsMthdNm'] = '제한경쟁(비지역)'
    
    return df

def load_contracts(sector=None):
    """계약 데이터 로드"""
    conn = sqlite3.connect(DB_PATH)
    results = {}
    
    # 낙찰정보 로드
    award_sets = load_busan_award_ntce_nos()
    
    if sector is None or sector == '공사':
        df = pd.read_sql_query("""
            SELECT dminsttCd as cntrctInsttCd, dminsttNm_req as cntrctInsttNm,
                   cntrctCnclsMthdNm, 
                   thtmCntrctAmt, totCntrctAmt, cntrctCnclsDate, ntceNo,
                   cntrctInsttOfclTelNo, untyCntrctNo
            FROM cnstwk_cntrct
        """, conn)
        df['amt'] = pd.to_numeric(df['thtmCntrctAmt'], errors='coerce').fillna(0)
        mask = df['amt'] == 0
        df.loc[mask, 'amt'] = pd.to_numeric(df.loc[mask, 'totCntrctAmt'], errors='coerce').fillna(0)
        df.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
        df = refine_contract_method(df, '공사', award_sets)
        results['공사'] = df
    
    if sector is None or sector == '용역':
        df = pd.read_sql_query("""
            SELECT dminsttCd as cntrctInsttCd, dminsttNm_req as cntrctInsttNm,
                   cntrctCnclsMthdNm,
                   thtmCntrctAmt, totCntrctAmt, cntrctCnclsDate,
                   pubPrcrmntLrgClsfcNm, ntceNo, cntrctInsttOfclTelNo, untyCntrctNo
            FROM servc_cntrct
        """, conn)
        df['amt'] = pd.to_numeric(df['thtmCntrctAmt'], errors='coerce').fillna(0)
        mask = df['amt'] == 0
        df.loc[mask, 'amt'] = pd.to_numeric(df.loc[mask, 'totCntrctAmt'], errors='coerce').fillna(0)
        df.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
        df = refine_contract_method(df, '용역', award_sets)
        results['용역'] = df
    
    if sector is None or sector == '물품':
        df = pd.read_sql_query("""
            SELECT dminsttCd as cntrctInsttCd, dminsttNm_req as cntrctInsttNm,
                   cntrctCnclsMthdNm,
                   thtmCntrctAmt, totCntrctAmt, cntrctCnclsDate, ntceNo,
                   cntrctInsttOfclTelNo, untyCntrctNo
            FROM thng_cntrct
        """, conn)
        df['amt'] = pd.to_numeric(df['thtmCntrctAmt'], errors='coerce').fillna(0)
        mask = df['amt'] == 0
        df.loc[mask, 'amt'] = pd.to_numeric(df.loc[mask, 'totCntrctAmt'], errors='coerce').fillna(0)
        df.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
        df = refine_contract_method(df, '물품', award_sets)
        results['물품'] = df
    
    if sector is None or sector == '쇼핑몰':
        df = pd.read_sql_query("""
            SELECT dminsttCd, dminsttNm, cntrctCnclsStleNm as cntrctCnclsMthdNm,
                   prdctAmt, dlvrReqRcptDate as cntrctCnclsDate,
                   prdctClsfcNoNm, dlvrReqNo, prdctSno, dlvrReqChgOrd,
                   corpEntrprsDivNmNm, dminsttRgnNm
            FROM shopping_cntrct
        """, conn)
        # 쇼핑몰 중복 제거 (최신 변경차수만)
        df['dlvrReqChgOrd'] = pd.to_numeric(df['dlvrReqChgOrd'], errors='coerce').fillna(0)
        df.sort_values('dlvrReqChgOrd', ascending=False, inplace=True)
        df.drop_duplicates(subset=['dlvrReqNo', 'prdctSno'], keep='first', inplace=True)
        df['amt'] = pd.to_numeric(df['prdctAmt'], errors='coerce').fillna(0)
        df.rename(columns={'dminsttCd': 'cntrctInsttCd', 'dminsttNm': 'cntrctInsttNm'}, inplace=True)
        results['쇼핑몰'] = df
    
    conn.close()
    return results

def _classify_agency(nm):
    """기관명 키워드 기반 자동 분류 (대분류, 중분류, 소분류)"""
    nm = nm or ''
    # 부산광역시 소속
    if nm.startswith('부산광역시') or nm.startswith('부산시 '):
        if any(k in nm for k in ['교육청', '학교', '유치원']):
            return '부산광역시 및 소속기관', '부산광역시 교육청', '각급학교'
        elif any(k in nm for k in ['구 ', '군 ']):
            return '부산광역시 및 소속기관', '자치구군', nm.split()[1] if len(nm.split()) > 1 else nm
        else:
            return '부산광역시 및 소속기관', '부산광역시', '부산광역시 본청'
    # 대학교
    if any(k in nm for k in ['대학교', '대학', '폴리텍']):
        return '정부 및 국가공공기관', '고등교육기관', '대학'
    # 공단/공사/공기업
    if any(k in nm for k in ['공단', '공사', '공기업', '진흥원']):
        return '정부 및 국가공공기관', '국가공공기관', '국가공단'
    # 연구원/연구소
    if any(k in nm for k in ['연구원', '연구소']):
        return '정부 및 국가공공기관', '국가공공기관', '국가출연기관'
    # 재단/위원회
    if any(k in nm for k in ['재단', '위원회']):
        return '정부 및 국가공공기관', '국가공공기관', '국가출연기관'
    # 병원
    if '병원' in nm:
        return '정부 및 국가공공기관', '국가공공기관', '국가출연기관'
    # 중앙행정기관 키워드
    if any(k in nm for k in ['국토교통부', '해양수산부', '환경부', '경찰', '소방', '세무', '검찰', '법원',
                              '보훈', '고용노동부', '병무청', '관세청', '국세청', '해군', '육군', '부대']):
        return '정부 및 국가공공기관', '중앙행정기관', nm.split()[0] if nm.split() else nm
    # 복지/사회복지
    if any(k in nm for k in ['복지', '어린이집', '요양']):
        return '민간 및 기타기관', '복지기관', '복지시설'
    # 조합
    if any(k in nm for k in ['조합', '협회']):
        return '민간 및 기타기관', '민간조합', '산업조합'
    # 기본값
    return '정부 및 국가공공기관', '국가공공기관', '국가출연기관'

def _enrich_master_db(new_agencies):
    """발견된 신규 부산 기관을 마스터 DB에 자동 분류 후 추가 (확인필요 플래그)"""
    if not new_agencies:
        return
    import sqlite3 as _sqlite3
    conn = _sqlite3.connect(AGENCY_DB_PATH)
    for cd, nm in new_agencies:
        lrg, mid, sml = _classify_agency(nm)
        conn.execute("""
            INSERT OR IGNORE INTO agency_master (dminsttCd, dminsttNm, cate_lrg, cate_mid, cate_sml, cate_dtl)
            VALUES (?, ?, ?, ?, ?, '확인필요')
        """, (cd, nm, lrg, mid, sml))
    conn.commit()
    conn.close()

def filter_busan(contracts, agency_master, sector_name, verbose=True):
    """부산 관내 계약 필터링
    - 공사/용역/물품: 1차 기관코드 + 2차 전화번호(051) 보완
    - 쇼핑몰: 1차 기관코드 + 2차 주소(dminsttRgnNm) 보완
    """
    df = contracts.copy()
    master_codes = set(agency_master['dminsttCd'].astype(str).str.strip())
    
    # 1차: agency_master 코드 매칭
    mask_code = df['cntrctInsttCd'].astype(str).str.strip().isin(master_codes)
    result = df[mask_code].copy()
    result['_filter'] = '기관코드'
    
    extra = pd.DataFrame()
    missed = df[~mask_code].copy()
    
    if sector_name == '쇼핑몰' and 'dminsttRgnNm' in df.columns:
        # 2차(쇼핑몰): 주소 보완 필터
        mask_addr = missed['dminsttRgnNm'].fillna('').str.contains('부산')
        extra = missed[mask_addr].copy()
        extra['_filter'] = '주소보완'
        filter_label = '주소 보완'
        
    elif sector_name in ('공사', '용역', '물품') and 'cntrctInsttOfclTelNo' in df.columns:
        # 2차(공사/용역/물품): 전화번호 051 보완 필터
        tel = missed['cntrctInsttOfclTelNo'].fillna('').str.replace('-', '')
        mask_tel = tel.str.startswith('051')
        candidates = missed[mask_tel].copy()
        
        # 기관명에 비부산 지역 키워드가 있으면 제외 (051 구 지역 포함 문제)
        NOT_BUSAN = ['울산', '양산', '김해', '경남', '경상남도', '밀양', '창원', '거제', '통영', '진주']
        if len(candidates) > 0:
            nm_col = candidates['cntrctInsttNm'].fillna('')
            mask_exclude = nm_col.apply(lambda x: any(kw in x for kw in NOT_BUSAN))
            extra = candidates[~mask_exclude].copy()
        else:
            extra = candidates
        
        extra['_filter'] = '전화보완'
        filter_label = '전화번호(051) 보완'
        
        # 마스터 DB에 자동 보강
        if len(extra) > 0:
            new_agencies = extra[['cntrctInsttCd', 'cntrctInsttNm']].drop_duplicates().values.tolist()
            _enrich_master_db(new_agencies)
    
    if len(extra) > 0 and verbose:
        extra_agencies = extra.groupby(['cntrctInsttCd', 'cntrctInsttNm']).agg(
            건수=('amt', 'count'), 금액=('amt', 'sum')
        ).reset_index().sort_values('금액', ascending=False)
        print(f"\n  ⚠️ [{sector_name}] {filter_label} 필터로 추가 포착된 부산 기관 ({len(extra_agencies)}개 기관, {len(extra):,}건):")
        for _, row in extra_agencies.head(15).iterrows():
            print(f"    {row['cntrctInsttCd']} | {row['cntrctInsttNm']:30s} | {row['건수']:>5,}건 {fmt_amt(row['금액']):>8s}")
        if len(extra_agencies) > 15:
            print(f"    ... 외 {len(extra_agencies)-15}개 기관")
    
    combined = pd.concat([result, extra], ignore_index=True)
    return combined

def fmt_amt(val):
    """금액을 억 원 단위로 포맷"""
    if val >= 1e12:
        return f"{val/1e12:,.1f}조"
    elif val >= 1e8:
        return f"{val/1e8:,.0f}억"
    elif val >= 1e4:
        return f"{val/1e4:,.0f}만"
    else:
        return f"{val:,.0f}"

def analyze_by_agency_group(contracts, agency_master, sector_name):
    """수요기관 그룹별 계약유형 분석"""
    df = contracts.copy()
    
    # 수요기관 마스터와 JOIN
    merged = df.merge(agency_master, left_on='cntrctInsttCd', right_on='dminsttCd', how='inner', suffixes=('', '_master'))
    
    if len(merged) == 0:
        return
    
    print(f"\n{'='*90}")
    print(f"  📊 [{sector_name}] 수요기관 대분류(그룹)별 계약유형 분석 (부산 관내)")
    print(f"{'='*90}")
    
    # 대분류별 집계
    group = merged.groupby(['cate_lrg', 'cntrctCnclsMthdNm']).agg(
        건수=('amt', 'count'),
        금액=('amt', 'sum')
    ).reset_index()
    
    for lrg in sorted(group['cate_lrg'].dropna().unique()):
        sub = group[group['cate_lrg'] == lrg]
        total_cnt = sub['건수'].sum()
        total_amt = sub['금액'].sum()
        print(f"\n  🔹 {lrg} (총 {total_cnt:,}건, {fmt_amt(total_amt)})")
        print(f"  {'─'*70}")
        for _, row in sub.sort_values('금액', ascending=False).iterrows():
            pct_cnt = row['건수'] / total_cnt * 100 if total_cnt > 0 else 0
            pct_amt = row['금액'] / total_amt * 100 if total_amt > 0 else 0
            bar = '█' * int(pct_amt / 3)
            print(f"    {row['cntrctCnclsMthdNm']:>8s}: {row['건수']:>6,}건 ({pct_cnt:5.1f}%)  금액 {fmt_amt(row['금액']):>8s} ({pct_amt:5.1f}%) {bar}")

def analyze_by_agency_detail(contracts, agency_master, sector_name, agency_keyword):
    """특정 수요기관 상세 계약유형 분석"""
    df = contracts.copy()
    merged = df.merge(agency_master, left_on='cntrctInsttCd', right_on='dminsttCd', how='inner', suffixes=('', '_master'))
    
    # 키워드로 기관 검색
    mask = merged['cate_sml'].fillna('').str.contains(agency_keyword) | merged['cntrctInsttNm'].fillna('').str.contains(agency_keyword)
    filtered = merged[mask]
    
    if len(filtered) == 0:
        return
    
    print(f"\n{'='*90}")
    print(f"  🔍 [{sector_name}] \"{agency_keyword}\" 관련 기관 계약유형 상세")
    print(f"{'='*90}")
    
    # 기관명별 계약유형
    for instt_nm in sorted(filtered['cntrctInsttNm'].unique())[:20]:
        sub = filtered[filtered['cntrctInsttNm'] == instt_nm]
        total_cnt = len(sub)
        total_amt = sub['amt'].sum()
        if total_cnt < 1:
            continue
        print(f"\n  📌 {instt_nm} (총 {total_cnt:,}건, {fmt_amt(total_amt)})")
        
        grp = sub.groupby('cntrctCnclsMthdNm').agg(건수=('amt', 'count'), 금액=('amt', 'sum')).reset_index()
        for _, row in grp.sort_values('금액', ascending=False).iterrows():
            pct = row['금액'] / total_amt * 100 if total_amt > 0 else 0
            print(f"    {row['cntrctCnclsMthdNm']:>10s}: {row['건수']:>5,}건  금액 {fmt_amt(row['금액']):>8s} ({pct:5.1f}%)")

def analyze_by_product_type(contracts, agency_master, sector_name):
    """물품유형별 계약유형 분석"""
    df = contracts.copy()
    
    # 부산 관내만 필터
    merged = df.merge(agency_master[['dminsttCd']], left_on='cntrctInsttCd', right_on='dminsttCd', how='inner')
    
    if sector_name == '용역' and 'pubPrcrmntLrgClsfcNm' in merged.columns:
        type_col = 'pubPrcrmntLrgClsfcNm'
        type_label = '용역 대분류'
    elif sector_name == '쇼핑몰' and 'prdctClsfcNoNm' in merged.columns:
        type_col = 'prdctClsfcNoNm'
        type_label = '물품 분류'
    else:
        return
    
    print(f"\n{'='*90}")
    print(f"  📦 [{sector_name}] {type_label}별 계약유형 분석 (부산 관내, 금액 상위 15개)")
    print(f"{'='*90}")
    
    # 유형별 금액 합계로 상위 추출
    top_types = merged.groupby(type_col)['amt'].sum().nlargest(15).index.tolist()
    
    for ptype in top_types:
        if not ptype or ptype.strip() == '':
            continue
        sub = merged[merged[type_col] == ptype]
        total_cnt = len(sub)
        total_amt = sub['amt'].sum()
        
        print(f"\n  📋 {ptype} (총 {total_cnt:,}건, {fmt_amt(total_amt)})")
        print(f"  {'─'*70}")
        
        grp = sub.groupby('cntrctCnclsMthdNm').agg(건수=('amt', 'count'), 금액=('amt', 'sum')).reset_index()
        for _, row in grp.sort_values('금액', ascending=False).iterrows():
            pct = row['금액'] / total_amt * 100 if total_amt > 0 else 0
            bar = '█' * int(pct / 3)
            print(f"    {row['cntrctCnclsMthdNm']:>12s}: {row['건수']:>6,}건  금액 {fmt_amt(row['금액']):>8s} ({pct:5.1f}%) {bar}")

def analyze_shopping_product_by_agency(contracts, agency_master):
    """쇼핑몰 물품분류별 × 수요기관 그룹별 교차 분석"""
    df = contracts.copy()
    
    # 부산 관내 + 기관 마스터 JOIN
    merged = df.merge(agency_master, left_on='cntrctInsttCd', right_on='dminsttCd', how='inner', suffixes=('', '_master'))
    
    if len(merged) == 0 or 'prdctClsfcNoNm' not in merged.columns:
        return
    
    print(f"\n{'='*90}")
    print(f"  🛒 [쇼핑몰] 물품분류별 × 수요기관 그룹별 교차 분석 (부산 관내, 금액 상위 20개 품목)")
    print(f"{'='*90}")
    
    # 금액 상위 20개 품목
    top_products = merged.groupby('prdctClsfcNoNm')['amt'].sum().nlargest(20)
    
    for ptype, total_amt in top_products.items():
        if not ptype or ptype.strip() == '':
            continue
        sub = merged[merged['prdctClsfcNoNm'] == ptype]
        total_cnt = len(sub)
        
        print(f"\n  📋 {ptype} (총 {total_cnt:,}건, {fmt_amt(total_amt)})")
        print(f"  {'─'*80}")
        
        # 수요기관 대분류별 비중
        grp = sub.groupby('cate_lrg').agg(건수=('amt', 'count'), 금액=('amt', 'sum')).reset_index()
        for _, row in grp.sort_values('금액', ascending=False).iterrows():
            pct = row['금액'] / total_amt * 100 if total_amt > 0 else 0
            bar = '▓' * int(pct / 4)
            print(f"    {row['cate_lrg']:>22s}: {row['건수']:>5,}건  {fmt_amt(row['금액']):>8s} ({pct:5.1f}%) {bar}")
    
    # 기업구분별 요약 (중소기업/대기업/중견기업)
    if 'corpEntrprsDivNmNm' in df.columns:
        busan = df.merge(agency_master[['dminsttCd']], left_on='cntrctInsttCd', right_on='dminsttCd', how='inner')
        
        print(f"\n{'='*90}")
        print(f"  🏢 [쇼핑몰] 물품분류별 × 기업규모별 분석 (부산 관내, 금액 상위 15개 품목)")
        print(f"{'='*90}")
        
        top15 = busan.groupby('prdctClsfcNoNm')['amt'].sum().nlargest(15)
        
        for ptype, total_amt in top15.items():
            if not ptype or ptype.strip() == '':
                continue
            sub = busan[busan['prdctClsfcNoNm'] == ptype]
            total_cnt = len(sub)
            
            print(f"\n  📋 {ptype} (총 {total_cnt:,}건, {fmt_amt(total_amt)})")
            print(f"  {'─'*80}")
            
            grp = sub.groupby('corpEntrprsDivNmNm').agg(건수=('amt', 'count'), 금액=('amt', 'sum')).reset_index()
            for _, row in grp.sort_values('금액', ascending=False).iterrows():
                pct = row['금액'] / total_amt * 100 if total_amt > 0 else 0
                bar = '▓' * int(pct / 4)
                label = row['corpEntrprsDivNmNm'] if row['corpEntrprsDivNmNm'] else '(미분류)'
                print(f"    {label:>16s}: {row['건수']:>5,}건  {fmt_amt(row['금액']):>8s} ({pct:5.1f}%) {bar}")

def print_overall_summary(all_contracts, agency_master):
    """전체 요약 (전국 vs 부산)"""
    print(f"\n{'='*90}")
    print(f"  🌟 전체 계약유형 현황 요약 (2026년 1분기)")
    print(f"{'='*90}")
    
    for sector, df in all_contracts.items():
        total_cnt = len(df)
        
        # 부산 필터 (쇼핑몰은 주소 보완 필터 포함)
        busan = filter_busan(df, agency_master, sector, verbose=True)
        busan_cnt = len(busan)
        busan_amt = busan['amt'].sum()
        
        code_cnt = len(busan[busan['_filter'] == '기관코드']) if '_filter' in busan.columns else busan_cnt
        addr_cnt = len(busan[busan['_filter'] == '주소보완']) if '_filter' in busan.columns else 0
        suffix = f" (코드:{code_cnt:,} + 주소보완:{addr_cnt:,})" if addr_cnt > 0 else ""
        
        print(f"\n  📊 [{sector}] 부산 관내: {busan_cnt:,}건{suffix} / 전국: {total_cnt:,}건  (총 {fmt_amt(busan_amt)})")
        
        grp = busan.groupby('cntrctCnclsMthdNm').agg(건수=('amt', 'count'), 금액=('amt', 'sum')).reset_index()
        for _, row in grp.sort_values('금액', ascending=False).iterrows():
            pct_cnt = row['건수'] / busan_cnt * 100 if busan_cnt > 0 else 0
            pct_amt = row['금액'] / busan_amt * 100 if busan_amt > 0 else 0
            bar = '█' * int(pct_amt / 2.5)
            print(f"    {row['cntrctCnclsMthdNm']:>12s}: {row['건수']:>7,}건 ({pct_cnt:5.1f}%)  금액 {fmt_amt(row['금액']):>8s} ({pct_amt:5.1f}%) {bar}")

def main():
    parser = argparse.ArgumentParser(description='계약유형 분석 도구')
    parser.add_argument('--agency', type=str, help='특정 기관명 키워드로 상세 분석 (예: 해운대구)')
    parser.add_argument('--sector', type=str, choices=['공사', '용역', '물품', '쇼핑몰'], help='특정 분야만 분석')
    args = parser.parse_args()
    
    print("📂 데이터 로딩 중...")
    agency_master = load_agency_master()
    all_contracts = load_contracts(args.sector)
    print(f"   ✅ 수요기관 마스터: {len(agency_master):,}건")
    for k, v in all_contracts.items():
        print(f"   ✅ {k}: {len(v):,}건")
    
    # 1. 전체 요약
    print_overall_summary(all_contracts, agency_master)
    
    # 2. 수요기관 그룹별 분석
    for sector_name, df in all_contracts.items():
        if args.agency:
            analyze_by_agency_detail(df, agency_master, sector_name, args.agency)
        else:
            analyze_by_agency_group(df, agency_master, sector_name)
    
    # 3. 물품유형별 분석 (용역 대분류 + 쇼핑몰 물품분류)
    for sector_name, df in all_contracts.items():
        if sector_name in ('용역', '쇼핑몰'):
            analyze_by_product_type(df, agency_master, sector_name)
    
    # 4. 쇼핑몰 물품분류 × 기관그룹 / 기업규모 교차 분석
    if '쇼핑몰' in all_contracts:
        analyze_shopping_product_by_agency(all_contracts['쇼핑몰'], agency_master)
    
    print(f"\n{'='*90}")
    print("🎉 분석 완료!")
    print(f"{'='*90}")

if __name__ == '__main__':
    main()
