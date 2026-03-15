"""
core_calc.py — 수주율 계산 공통 모듈
====================================
rate_calc_db.py, build_api_cache.py 등에서 공통으로 사용하는
필터링/지분계산/수주율 산출 로직을 한 곳에 모아둠.
수정 시 모든 곳에 자동 반영됨.
"""
import pandas as pd
import numpy as np
import json
import re
from collections import defaultdict

# ============================================================
# 1. 상수
# ============================================================
NON_BUSAN_KEYWORDS = [
    # 광역시/특별시
    '서울', '인천', '대구', '대전', '광주광역', '울산',
    # 특별자치시/도
    '세종', '제주',
    # 도 (약칭 + 정식명)
    '경기', '경기도',
    '강원', '강원도', '강원특별',
    '충북', '충청북도', '충남', '충청남도',
    '전북', '전라북도', '전북특별', '전남', '전라남도',
    '경북', '경상북도', '경남', '경상남도',
    # 특정 지명
    '울릉', '독도',
    # 시/군 단위 (부산 외 주요 도시)
    '포항', '경주', '김천', '안동', '구미', '영주', '영천', '상주', '문경', '예천',
    '경산', '군위', '의성', '청송', '영양', '영덕', '봉화', '울진', '청도', '고령', '성주', '칠곡',
    '창원', '진주', '통영', '사천', '김해', '밀양', '거제', '양산', '고성',
    '의령', '함안', '창녕', '합천', '산청', '함양', '하동', '남해',
    '광양', '순천', '여수', '목포', '나주', '무안',
    '천안', '아산', '당진', '서산', '논산', '공주',
    '청주', '충주', '제천',
    '전주', '익산', '군산', '정읍', '남원',
    '춘천', '원주', '강릉', '속초', '동해', '삼척', '태백',
    '수원', '성남', '용인', '화성', '평택', '안산', '안양', '파주',
    '새만금',
    # 특정 비부산 프로젝트 지명
    '웅상', '삼자현',
    # 서울 구 이름 (부산 구와 겹치지 않는 것)
    '관악', '동작', '강남', '송파', '강서', '강동', '마포', '영등포',
    '종로', '용산', '성북', '도봉', '노원', '은평', '서대문', '양천',
    '구로', '금천', '광진', '성동', '중랑',
]

# 부산 관할 세무서 사업자번호 앞 3자리 (보조 판별용)
# 601~629: 부산지방국세청 관할 세무서 코드
BUSAN_BIZNO_PREFIXES = {str(i) for i in range(601, 630)}

# 부산 지명과 겹치는 키워드 예외
BUSAN_EXCEPTIONS = {
    '대구': ['해운대구'],
    '동해': ['동해선', '동해남부'],  # 부산 동해선 공사
    '김해': ['김해공항'],           # 부산 김해공항
    '양산': ['양산단층'],           # 부산 관련 지질
    '고성': ['고성동'],             # 부산 고성동 (없지만 안전장치)
    '남해': ['남해안', '남해고속'],   # 부산 남해안 관련 공사
}


# ============================================================
# 2. 유틸리티 함수
# ============================================================
def parse_corp_shares(cl):
    """corpList 문자열에서 [(사업자번호, 지분율)] 추출 + 정규화"""
    biz_list = []
    cl = str(cl or '')
    if not cl or cl in ('nan','None',''): return biz_list
    for chunk in cl.split('[')[1:]:
        chunk = chunk.split(']')[0]
        parts = chunk.split('^')
        if len(parts) >= 10:
            bno = str(parts[9]).replace('-','').strip()
            try: share = float(parts[6]) if parts[6].strip() else 0.0
            except: share = 0.0
            biz_list.append([bno, share])
    if biz_list:
        tot = sum(s for _,s in biz_list)
        if tot == 0:
            biz_list = [[b, 100.0/len(biz_list)] for b,_ in biz_list]
        elif tot > 100.1:
            biz_list = [[b, s/tot*100] for b,s in biz_list]
    return biz_list


def extract_dminstt_codes(dminstt_list_str):
    """dminsttList 필드에서 수요기관 코드 추출"""
    codes = []
    if not dminstt_list_str or dminstt_list_str in ('nan', 'None', ''):
        return codes
    for chunk in str(dminstt_list_str).split('[')[1:]:
        chunk = chunk.split(']')[0]
        parts = chunk.split('^')
        if len(parts) >= 2:
            codes.append(str(parts[1]).strip())
    return codes


def dedup_by_dcsn(df):
    """dcsnCntrctNo 앞8자리(기관+공종) 기준 최신 차수만 남기기.
    
    같은 공사의 설계변경/물가변동 차수 중복을 제거.
    dcsnCntrctNo가 없거나 10자리 미만인 행은 영향 없음.
    """
    if 'dcsnCntrctNo' not in df.columns:
        return df
    
    dcsn = df['dcsnCntrctNo'].fillna('').astype(str).str.strip()
    # 10자리 이상인 건만 대상 (정상 확정번호)
    has_dcsn = dcsn.str.len() >= 10
    
    if has_dcsn.sum() == 0:
        return df
    
    # 앞8자리 = 기관+공종 (그룹키), 끝2자리 = 차수
    df = df.copy()
    df['_dcsn_base'] = dcsn.where(has_dcsn, '')
    df['_dcsn_base'] = df['_dcsn_base'].apply(lambda x: x[:-2] if len(x) >= 10 else '')
    df['_dcsn_ord'] = dcsn.where(has_dcsn, '').apply(lambda x: x[-2:] if len(x) >= 10 else '')
    
    # 동일 기본번호 내에서 최대 차수만 남기기
    before = len(df)
    keep_mask = pd.Series(True, index=df.index)
    
    grouped = df[df['_dcsn_base'] != ''].groupby('_dcsn_base')
    for base, grp in grouped:
        if len(grp) > 1:
            max_ord = grp['_dcsn_ord'].max()
            for idx in grp.index:
                if grp.loc[idx, '_dcsn_ord'] != max_ord:
                    keep_mask[idx] = False
    
    df = df[keep_mask].drop(columns=['_dcsn_base', '_dcsn_ord'])
    return df
# ============================================================
# 3. 필터 함수
# ============================================================
def is_non_busan_contract(row, lrg):
    """전화번호+키워드로 타지역 계약 판별 (부산시 소속기관은 항상 통과)"""
    if lrg == '부산광역시 및 소속기관':
        return False
    # 전화번호
    tel = str(row.get('cntrctInsttOfclTelNo', '')).strip()
    is_non_busan_tel = tel and not tel.startswith(('051', '070', '010', '****'))
    # 계약명 키워드
    cntrct_nm = str(row.get('cntrctNm', '') or row.get('cnstwkNm', '') or '').strip()
    has_kw = False
    for kw in NON_BUSAN_KEYWORDS:
        if kw in cntrct_nm:
            exceptions = BUSAN_EXCEPTIONS.get(kw, [])
            if any(exc in cntrct_nm for exc in exceptions):
                continue
            has_kw = True
            break
    return is_non_busan_tel or has_kw


def check_busan_restriction(rgn_json_str):
    """입찰공고 rgnLmtInfo JSON에서 부산 지역제한 여부 확인"""
    if not rgn_json_str or pd.isna(rgn_json_str):
        return False
    try:
        rgn = json.loads(rgn_json_str)
        for v in rgn.values():
            if v and '부산' in str(v): return True
        if str(rgn.get('rgnLmtBidLocplcJdgmBssNm')).strip() == '본사또는참여지사소재지':
            return True
        if rgn.get('cmmnSpldmdCorpRgnLmtYn') == 'Y':
            return True
    except:
        pass
    return False


def filter_cnstwk_by_site(df, bid_df):
    """공사 계약에서 부산 외 현장 배제 (bid_notices_raw 조인)"""
    df['ntceNo_str'] = df['ntceNo'].astype(str).str.replace('-', '', regex=False).str.strip()
    merged = pd.merge(df, bid_df[['bidNtceNo_str', 'cnstrtsiteRgnNm']],
                       how='left', left_on='ntceNo_str', right_on='bidNtceNo_str')
    mask_outside = merged['cnstrtsiteRgnNm'].notna() & (~merged['cnstrtsiteRgnNm'].str.contains('부산', na=False))
    n_dropped = mask_outside.sum()
    amt_dropped = merged.loc[mask_outside, 'totCntrctAmt'].astype(float).sum()
    filtered = merged[~mask_outside].copy()
    return filtered, n_dropped, amt_dropped


# 용역 현장 판별용: 실제 지역명으로 보이는 패턴 (광역시/도/시/군)
REGION_MARKERS = ['광역시', '특별시', '특별자치', '도 ', '시 ', '군 ', '구 ',
                  '서울', '인천', '대구', '대전', '광주', '울산', '세종',
                  '경기', '강원', '충북', '충남', '전북', '전남', '경북', '경남', '제주']

def filter_servc_by_site(df, inst_dict=None):
    """용역 계약에서 부산 외 현장 배제 (cnstrtsiteRgnNm 직접 사용)
    
    - inst_dict 전달 시: 부산시 소속기관은 현장=부산 확정(필터 스킵),
      정부/국가공공기관만 현장 필터 적용.
    - inst_dict=None: 전체 필터 적용 (하위호환)
    
    일반용역은 납품장소가 비정형('과업내역에 따름' 등)일 수 있으므로,
    실제 지역명이 포함된 경우에만 필터 적용.
    추가: 현장지역 필드가 비어있을 때 계약명에서 비부산 지역 키워드 보조 감지.
    """
    if 'cnstrtsiteRgnNm' not in df.columns:
        return df, 0, 0
    
    site = df['cnstrtsiteRgnNm'].fillna('')
    
    # 1차: 실제 지역명이 있고 + 부산이 아닌 건 배제
    has_real_region = site.apply(lambda s: any(m in s for m in REGION_MARKERS) if s else False)
    is_busan = site.str.contains('부산', na=False)
    mask_outside = has_real_region & ~is_busan
    
    # 2차: 현장지역 필드가 비어있을 때 계약명에서 비부산 지역 키워드 보조 감지
    NON_BUSAN_NAME_MARKERS = ['울산', '경남', '경북', '대구', '서울', '인천', '대전', '광주', '세종',
                               '경기', '강원', '충북', '충남', '전북', '전남', '제주',
                               '창원', '김해', '양산', '거제', '통영', '진주', '마산',
                               '포항', '구미', '경주', '안동', '천안', '청주', '전주', '광양', '순천',
                               '울주군', '장안', '서생', '온산']
    if 'cntrctNm' in df.columns:
        cnm = df['cntrctNm'].fillna('')
        site_empty = (site == '')
        name_has_nonbusan = cnm.apply(lambda n: any(m in str(n) for m in NON_BUSAN_NAME_MARKERS) if n else False)
        name_has_busan = cnm.str.contains('부산', na=False)
        mask_name_outside = site_empty & name_has_nonbusan & ~name_has_busan
        mask_outside = mask_outside | mask_name_outside
    
    # 기관 그룹별 차등 적용: 부산시 소속기관은 현장 필터 스킵
    if inst_dict is not None and 'dminsttCd' in df.columns:
        is_busan_local = df['dminsttCd'].apply(
            lambda cd: inst_dict.get(str(cd).strip(), {}).get('cate_lrg', '') == '부산광역시 및 소속기관'
        )
        # 부산시 소속기관은 mask_outside에서 제외 (현장=부산 확정)
        mask_outside = mask_outside & ~is_busan_local
    
    n_dropped = mask_outside.sum()
    amt_dropped = df.loc[mask_outside, 'totCntrctAmt'].astype(float).sum()
    filtered = df[~mask_outside].copy()
    return filtered, n_dropped, amt_dropped


def build_shopping_site_index(conn, busan_agency_cds):
    """혼재 기관(부산+타지역 공고 보유)의 공고명→현장지역 매핑 인덱스 생성.
    
    Returns:
        mixed_cds: set — 혼재 기관 코드
        other_only_cds: set — 타지역 전용 기관 코드
        ntc_index: dict — {dminsttCd: [(공고명, 현장지역, is_busan), ...]}
    """
    notices = pd.read_sql(
        "SELECT bidNtceNm, dminsttCd, cnstrtsiteRgnNm FROM bid_notices_raw"
        " WHERE dminsttCd IS NOT NULL AND dminsttCd != ''", conn)
    
    agency_sites = defaultdict(lambda: {'busan': 0, 'other': 0})
    for _, r in notices.iterrows():
        cd = str(r['dminsttCd']).strip()
        if cd not in busan_agency_cds:
            continue
        site = str(r['cnstrtsiteRgnNm'] or '').strip()
        if not site:
            continue
        if '부산' in site:
            agency_sites[cd]['busan'] += 1
        else:
            agency_sites[cd]['other'] += 1
    
    mixed_cds = {cd for cd, s in agency_sites.items()
                 if s['busan'] > 0 and s['other'] > 0}
    other_only_cds = {cd for cd, s in agency_sites.items()
                      if s['busan'] == 0 and s['other'] > 0}
    
    # 혼재 기관의 공고명-현장 인덱스
    ntc_index = defaultdict(list)
    for _, r in notices.iterrows():
        cd = str(r['dminsttCd']).strip()
        if cd not in mixed_cds:
            continue
        site = str(r['cnstrtsiteRgnNm'] or '').strip()
        if not site:
            continue
        ntc_index[cd].append((
            str(r['bidNtceNm'] or ''),
            site,
            '부산' in site,
        ))
    
    return mixed_cds, other_only_cds, dict(ntc_index)


def _clean_project_name(nm):
    """납품요구건명에서 자재/구매 관련 접미사 제거 → 공사 프로젝트명만 추출"""
    nm = nm or ''
    for kw in ['레미콘', '아스콘', '아스팔트콘크리트', '지급자재', '관급자재',
               '사급자재', '구매', '납품', '제작', '설치', '구입', '제조',
               '철근', '강판', '블록', '파형강관', '콘크리트블록',
               '스틸그레이팅', '금속제울타리', '차량방호책',
               '자연석경계석', '가로등기구', '단독경보형감지기']:
        nm = nm.replace(kw, '')
    for prefix in ['[관급자재]', '(재)', '(추)', '(재_2)', '(재_3)', '(재_4)']:
        nm = nm.replace(prefix, '')
    nm = re.sub(r'\s+', ' ', nm).strip().rstrip('(').rstrip('-').strip()
    return nm


def _match_project(dlvr_nm, agency_notices):
    """납품요구건명과 같은 기관의 공고명을 매칭하여 현장지역 반환.
    
    Returns: (site_region, is_busan) or (None, None)
    """
    cleaned = _clean_project_name(dlvr_nm)
    if len(cleaned) < 5:
        return None, None
    
    # 핵심 8자 매칭 (앞부분)
    core = cleaned[:8]
    for ntc_name, site, is_busan in agency_notices:
        if core in ntc_name:
            # 역검증: 공고명 앞 5자도 납품건명에 포함되는지
            ntc_core = ntc_name[:5]
            if ntc_core in cleaned or ntc_core in (dlvr_nm or ''):
                return site, is_busan
    
    return None, None


def filter_shopping_by_site(df, conn, busan_agency_cds):
    """쇼핑몰 공사자재에서 부산 외 현장 건 배제.
    
    3그룹 분류:
    - 부산 전용 기관(928개): 통과
    - 타지역 전용 기관(11개): cnstwkMtrlDrctPurchsObjYn='Y' 배제
    - 혼재 기관(64개): dlvrReqNm ↔ bidNtceNm 텍스트 매칭
    
    Returns: (filtered_df, n_dropped, amt_dropped)
    """
    if 'cnstwkMtrlDrctPurchsObjYn' not in df.columns:
        return df, 0, 0
    
    mixed_cds, other_only_cds, ntc_index = build_shopping_site_index(
        conn, busan_agency_cds)
    
    drop_mask = pd.Series(False, index=df.index)
    
    for idx, row in df.iterrows():
        is_cnstwk = str(row.get('cnstwkMtrlDrctPurchsObjYn', '')).strip() == 'Y'
        if not is_cnstwk:
            continue
        
        cd = str(row.get('dminsttCd', '')).strip()
        
        # 타지역 전용 기관 → 배제
        if cd in other_only_cds:
            drop_mask[idx] = True
            continue
        
        # 혼재 기관 → 텍스트 매칭
        if cd in mixed_cds:
            agency_ntcs = ntc_index.get(cd, [])
            if not agency_ntcs:
                continue
            dlvr_nm = str(row.get('dlvrReqNm', '') or '')
            site, is_busan = _match_project(dlvr_nm, agency_ntcs)
            if site is not None and not is_busan:
                drop_mask[idx] = True
    
    n_dropped = drop_mask.sum()
    amt_dropped = df.loc[drop_mask, 'prdctAmt'].astype(float).sum()
    filtered = df[~drop_mask].copy()
    return filtered, n_dropped, amt_dropped


# ============================================================
# 4. 핵심: 계약 데이터 → (기관코드, 금액, 지역수주액) 산출
# ============================================================
def process_contract_row(row, inst_dict, biznos, is_shopping=False,
                         use_location_filter=False, bid_dict=None, award_set=None):
    """단일 계약 행 처리 → (matched_cd, amt, local_amt) 또는 None (배제)"""
    if is_shopping:
        amt = float(row.get('prdctAmt', 0))
        if np.isnan(amt): amt = 0
        inst_cd = str(row.get('dminsttCd', '')).strip()
        biz_nos = [(str(row.get('cntrctCorpBizno', '')).replace('-','').strip(), 100.0)]
    else:
        amt = float(row.get('thtmCntrctAmt', 0))
        if np.isnan(amt) or amt == 0:
            amt = float(row.get('totCntrctAmt', 0))
        if np.isnan(amt): amt = 0
        inst_cd = str(row.get('dminsttCd', '')).strip()
        biz_nos = parse_corp_shares(row.get('corpList', ''))

    # 기관 매칭 (dminsttCd → dminsttList fallback)
    matched_cd = None
    if inst_cd in inst_dict:
        matched_cd = inst_cd
    elif not is_shopping:
        for dcd in extract_dminstt_codes(row.get('dminsttList', '')):
            if dcd in inst_dict:
                matched_cd = dcd
                break
    if matched_cd is None:
        return None

    agency = inst_dict[matched_cd]
    lrg = agency['cate_lrg']

    # 타지역 계약 필터
    if use_location_filter and not is_shopping and is_non_busan_contract(row, lrg):
        bypassed = False
        ntce_no = str(row.get('ntceNo', '')).replace('-', '').strip()
        if award_set and ntce_no in award_set:
            bypassed = True
        if not bypassed and bid_dict and ntce_no in bid_dict:
            if check_busan_restriction(bid_dict[ntce_no].get('rgnLmtInfo')):
                bypassed = True
        if not bypassed:
            return None

    # 지역업체 수주액 (마스터 DB + 사업자번호 앞3자리 보조 판별)
    loc_amt = 0
    for bno, share in biz_nos:
        if bno in biznos or (len(bno) >= 3 and bno[:3] in BUSAN_BIZNO_PREFIXES):
            loc_amt += amt * (share / 100.0)

    return (matched_cd, amt, loc_amt)


# ============================================================
# 5. 데이터 로딩 헬퍼
# ============================================================
def load_bid_dict(conn):
    """입찰공고 딕셔너리 로딩"""
    df = pd.read_sql("SELECT bidNtceNo, cnstrtsiteRgnNm, rgnLmtInfo FROM bid_notices_raw", conn)
    df['bidNtceNo_str'] = df['bidNtceNo'].astype(str).str.strip()
    df.drop_duplicates(subset=['bidNtceNo_str'], keep='last', inplace=True)
    return df.set_index('bidNtceNo_str').to_dict('index'), df


def load_award_sets(conn):
    """낙찰정보 브릿지 로딩"""
    sets = {}
    for tbl, key in [('busan_award_servc','용역'),('busan_award_cnstwk','공사'),('busan_award_thng','물품')]:
        try:
            sets[key] = set(pd.read_sql(f"SELECT bidNtceNo FROM {tbl}", conn)['bidNtceNo'].astype(str).str.strip())
        except:
            sets[key] = set()
    return sets
