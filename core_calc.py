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
    # -- 경북
    '포항', '경주', '김천', '안동', '구미', '영주', '영천', '상주', '문경', '예천',
    '경산', '군위', '의성', '청송', '영양', '영덕', '봉화', '울진', '청도', '고령', '성주', '칠곡',
    # -- 경남
    '창원', '진주', '통영', '사천', '김해', '밀양', '거제', '양산', '고성',
    '의령', '함안', '창녕', '합천', '산청', '함양', '하동', '남해',
    # -- 전남/전북
    '광양', '순천', '여수', '목포', '나주', '무안',
    '전주', '익산', '군산', '정읍', '남원',
    # -- 충남/충북
    '천안', '아산', '당진', '서산', '논산', '공주',
    '청주', '충주', '제천',
    # -- 강원
    '춘천', '원주', '강릉', '속초', '동해', '삼척', '태백',
    # -- 경기 (전체)
    '수원', '성남', '용인', '화성', '평택', '안산', '안양', '파주',
    '의정부', '구리', '남양주', '고양', '부천', '광명', '시흥', '군포',
    '의왕', '하남', '김포', '이천', '오산', '여주', '양평', '포천',
    '연천', '가평', '양주', '동두천', '안성', '과천',
    # -- 인천 (연수/강화는 오탐률 높아 제외)
    '계양', '미추홀', '부평', '남동', '영종', '옹진',
    # -- 서울 (부산과 겹치지 않는 구/동명)
    '관악', '동작', '강남', '송파', '강동', '마포', '영등포',
    '종로', '용산', '성북', '도봉', '노원', '은평', '서대문', '양천',
    '구로', '금천', '광진', '성동', '중랑', '서초', '동대문',
    # -- 서울 세부 지명
    '역삼', '삼성동', '잠실', '여의도', '광화문', '을지로',
    # -- 제주
    '제주', '서귀포',
    '새만금',
    # 특정 비부산 프로젝트 지명
    '웅상', '삼자현',
    # 우체국 등 국가기관 관할구역 내 비부산 지역
    '거창', '진해',
    # 한전 비부산 변전소/송전선 (부산울산지역본부 관할 내 경북/경남 시설)
    '상운', '풍기', '칠산', '선산', '명곡', '옥동', '신장수',
    # 전국 단위 사업
    '국도', '국가지원지방도',
    # 권역명
    '영남권', '호남권', '수도권', '충청권',
]

# 부산 관할 세무서 사업자번호 앞 3자리 (보조 판별용)
# 601~629: 부산지방국세청 관할 세무서 코드
BUSAN_BIZNO_PREFIXES = {str(i) for i in range(601, 630)}

# 대표자+업체명 매칭에서 제외할 비부산 지점 사업자번호 (오탐 방지)
NON_LOCAL_BRANCH_BIZNOS = {
    '3448700750',  # 로하스인터내셔널주식회사 (비부산)
    '2158735039',  # 주식회사 스마트이엔씨 (비부산)
    '3258101042',  # 주식회사로하스에코시스템 (비부산)
}

# 부산 지명과 겹치는 키워드 예외
BUSAN_EXCEPTIONS = {
    '대구': ['해운대구'],
    '동해': ['동해선', '동해남부'],  # 부산 동해선 공사
    '김해': ['김해공항'],           # 부산 김해공항
    '양산': ['양산단층'],           # 부산 관련 지질
    '고성': ['고성동'],             # 부산 고성동 (없지만 안전장치)
    '남해': ['남해안', '남해고속'],   # 부산 남해안 관련 공사
    '국도': ['부산'],               # 부산 구간 국도 사업 허용
    '진해': ['진해신항', '진해항', '부산진해'],  # 부산항 진해신항, LH 부산진해 사업
    '풍기': ['송풍기'],              # 장비명 '송풍기' 오탐 방지
    '산청': ['청사'],                # '부산청사'에서 '산청' 오탐 방지
    '서초': ['초등'],                # '연서초등학교', '성서초등학교' 오탐 방지
    '제주': ['제주도'],              # '제주도 수학여행' 등 부산학교 행사 보호
    '고양': ['고양이'],              # '길고양이 중성화' 오탐 방지
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
    
    # 추가: dcsnCntrctNo 완전 일치 중복 제거 (동일 확정번호의 다른 계약번호)
    dcsn2 = df['dcsnCntrctNo'].fillna('').astype(str).str.strip()
    has_dcsn2 = dcsn2.str.len() >= 10
    dup_mask = has_dcsn2 & df.duplicated(subset=['dcsnCntrctNo'], keep='first')
    if dup_mask.sum() > 0:
        df = df[~dup_mask]
    
    return df
# ============================================================
# 3. 필터 함수
# ============================================================
def is_non_busan_contract(row, lrg):
    """전화번호+키워드로 타지역 계약 판별.
    Returns: 'keyword' (키워드 매칭), 'tel' (전화번호만), False (부산)"""
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
    if has_kw:
        return 'keyword'
    if is_non_busan_tel:
        return 'tel'
    return False


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
                               '울주군', '장안', '서생', '온산',
                               '국도']
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


def filter_shopping_by_site(df, conn, busan_agency_cds, inst_dict=None):
    """쇼핑몰에서 부산 외 현장 건 배제 (2단계 필터).
    
    1차: 관급자재(Y) 현장추적 — 기존 공사현장 매칭 로직
    2차: 전체 — 납품건명(dlvrReqNm)에 타지역 키워드가 명확한 건 배제
         (단, 부산시 소속기관은 2차 필터 면제)
    
    Returns: (filtered_df, n_dropped, amt_dropped)
    """
    if 'cnstwkMtrlDrctPurchsObjYn' not in df.columns:
        return df, 0, 0
    
    mixed_cds, other_only_cds, ntc_index = build_shopping_site_index(
        conn, busan_agency_cds)
    
    drop_mask = pd.Series(False, index=df.index)
    
    # --- 1차: 관급자재(Y) 현장추적 필터 (기존 로직) ---
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
    
    # --- 2차: 납품건명 텍스트 키워드 필터 (관급·일반 모두 대상) ---
    # 부산시 소속기관은 2차 필터 면제
    for idx, row in df.iterrows():
        if drop_mask[idx]:
            continue  # 이미 1차에서 배제된 건은 스킵
        
        cd = str(row.get('dminsttCd', '')).strip()
        
        # 부산시 소속기관은 면제
        if inst_dict:
            agency_info = inst_dict.get(cd, {})
            if agency_info.get('cate_lrg') == '부산광역시 및 소속기관':
                continue
        
        dlvr_nm = str(row.get('dlvrReqNm', '') or '').strip()
        if not dlvr_nm:
            continue
        
        for kw in NON_BUSAN_KEYWORDS:
            if kw in dlvr_nm:
                exceptions = BUSAN_EXCEPTIONS.get(kw, [])
                if any(exc in dlvr_nm for exc in exceptions):
                    continue
                drop_mask[idx] = True
                break
    
    # --- 3차: 국도 구간명 패턴 필터 ---
    # "지명-지명(차수) 관급(자재)" 형식 (예: 군북-가야(7차) 관급(레미콘))
    road_section_pattern = re.compile(r'^[가-힣]+-[가-힣]+[\s가-힣]*\([\d차이월\s]*\)\s*관급')
    for idx, row in df.iterrows():
        if drop_mask[idx]:
            continue
        dlvr_nm = str(row.get('dlvrReqNm', '') or '').strip()
        if road_section_pattern.match(dlvr_nm):
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
        biz_nos = parse_corp_shares(row.get('corpList', ''))

    # 기관 매칭 (dminsttCd -> dminsttList -> cntrctInsttCd 최후 수단)
    matched_cd = None
    
    # 1. dminsttCd 확인
    cd_cand = str(row.get('dminsttCd', '') or '').strip()
    if cd_cand in inst_dict:
        matched_cd = cd_cand
        
    # 2. dminsttList 확인 (일반 공고의 수요기관은 주로 여기에 있음)
    if matched_cd is None and not is_shopping:
        for dcd in extract_dminstt_codes(row.get('dminsttList', '')):
            if dcd in inst_dict:
                matched_cd = dcd
                break
                
    # 3. cntrctInsttCd 확인 (기본값 없고 수기 계약처럼 dminstt 정보 누락 시 최후의 수단)
    if matched_cd is None:
        cd_cand2 = str(row.get('cntrctInsttCd', '') or '').strip()
        if cd_cand2 in inst_dict:
            matched_cd = cd_cand2
            
    if matched_cd is None:
        return None

    agency = inst_dict[matched_cd]
    lrg = agency['cate_lrg']

    # 타지역 계약 필터
    non_busan = is_non_busan_contract(row, lrg)
    if use_location_filter and not is_shopping and non_busan:
        bypassed = False
        ntce_no = str(row.get('ntceNo', '')).replace('-', '').strip()
        # 전화번호만으로 잡힌 건은 award bypass 허용 (키워드 매칭은 확실한 타지역이라 bypass 불가)
        if non_busan == 'tel' and award_set and ntce_no in award_set:
            bypassed = True
        # 부산 지역제한이 명시된 공고는 항상 bypass
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

def load_expanded_biznos(conn_cp, conn_pr=None):
    """지점 사업자번호 매칭 확장 (대표자+업체명)
    - conn_cp: busan_companies_master.db 연결
    - conn_pr: (선택) procurement_contracts.db 연결. 주어지면 계약 DB 스캔을 통해 지점 번호 추가 발견함.
    """
    biznos = set(pd.read_sql("SELECT bizno FROM company_master", conn_cp)['bizno']
                 .dropna().astype(str).str.replace('-', '', regex=False).str.strip())
    
    if not conn_pr:
        return biznos
        
    import re
    from collections import defaultdict
    _master_by_ceo = defaultdict(list)
    for _r in conn_cp.execute("SELECT bizno, corpNm, ceoNm FROM company_master WHERE ceoNm IS NOT NULL AND ceoNm != ''").fetchall():
        _bno = str(_r[0]).replace('-','').strip()
        _ceo = str(_r[2]).strip()
        _corp = str(_r[1] or '').strip()
        _norm = re.sub(r'주식회사|\(주\)|유한회사|\(유\)|사단법인|재단법인|\s', '', _corp)
        _master_by_ceo[_ceo].append((_bno, _corp, _norm))
        
    _branch_biznos = set()
    for _tbl in ['cnstwk_cntrct', 'servc_cntrct', 'thng_cntrct']:
        for (_corpList,) in conn_pr.execute(f"SELECT corpList FROM [{_tbl}]").fetchall():
            if not _corpList: continue
            for _chunk in str(_corpList).split('[')[1:]:
                _parts = _chunk.split(']')[0].split('^')
                if len(_parts) >= 10:
                    _bno = str(_parts[9]).replace('-','').strip()
                    if _bno and len(_bno) >= 10 and _bno not in biznos and _bno not in _branch_biznos:
                        _ceo = str(_parts[4]).strip() if len(_parts) > 4 else ''
                        _name = str(_parts[3]).strip() if len(_parts) > 3 else ''
                        if not _ceo or not _name: continue
                        _candidates = _master_by_ceo.get(_ceo, [])
                        if not _candidates: continue
                        _norm_c = re.sub(r'주식회사|\(주\)|유한회사|\(유\)|사단법인|재단법인|\s', '', _name)
                        if len(_norm_c) < 3: continue
                        if _bno in NON_LOCAL_BRANCH_BIZNOS: continue
                        for _m_bno, _m_name, _m_norm in _candidates:
                            if len(_m_norm) < 3: continue
                            if _norm_c == _m_norm or (len(_m_norm) >= 3 and _m_norm in _norm_c) or (len(_norm_c) >= 3 and _norm_c in _m_norm):
                                _branch_biznos.add(_bno)
                                break
    biznos.update(_branch_biznos)
    return biznos
