"""
API 캐시 생성기: 수주율 데이터를 미리 계산하여 JSON 파일로 저장
=============================================================
core_calc.py의 공통 로직을 사용하여 일관된 계산 보장.
필터링 수정 시 core_calc.py만 고치면 여기도 자동 반영됨.
"""
import sqlite3, pandas as pd, json, sys, time
from datetime import datetime
from collections import defaultdict

from core_calc import (
    parse_corp_shares, extract_dminstt_codes, dedup_by_dcsn,
    is_non_busan_contract, check_busan_restriction,
    filter_cnstwk_by_site, filter_servc_by_site, filter_shopping_by_site, process_contract_row,
    load_bid_dict, load_award_sets, BUSAN_BIZNO_PREFIXES,
)

sys.stdout.reconfigure(encoding='utf-8')

DB_PROCUREMENT = 'procurement_contracts.db'
DB_AGENCIES = 'busan_agencies_master.db'
DB_COMPANIES = 'busan_companies_master.db'
CACHE_FILE = 'api_cache.json'

MIN_AMT = {
    '공사': 10e8,
    '용역': 10e8,
    '물품': 10e8,
    '쇼핑몰': 10e8,
    None: 10e8,
}
TOP_N = {
    '부산광역시 및 소속기관': 20,
    '정부 및 국가공공기관': 15,
}

def pct(t,l): return round(l/t*100,1) if t>0 else 0

def build_cache():
    start = time.time()
    print("[캐시 생성] 시작...")
    
    # ========== 마스터 로딩 ==========
    conn_ag = sqlite3.connect(DB_AGENCIES)
    master = pd.read_sql("SELECT dminsttCd, cate_lrg, cate_mid, cate_sml, compare_unit FROM agency_master", conn_ag)
    conn_ag.close()
    master['dminsttCd'] = master['dminsttCd'].astype(str).str.strip()
    
    # inst_dict: process_contract_row에서 사용
    inst_dict = master.set_index('dminsttCd')[['cate_lrg','cate_mid','cate_sml']].to_dict('index')
    inst_unit = dict(zip(master['dminsttCd'], master['compare_unit']))
    inst_grp = dict(zip(master['dminsttCd'], master['cate_lrg']))
    inst_mid = dict(zip(master['dminsttCd'], master['cate_mid']))
    
    conn_cp = sqlite3.connect(DB_COMPANIES)
    from core_calc import load_expanded_biznos
    _conn_prot = sqlite3.connect(DB_PROCUREMENT)
    biznos = load_expanded_biznos(conn_cp, _conn_prot)
    _conn_prot.close()
    

    supplier_map = pd.read_sql("""
        SELECT rprsntDtlPrdnm, COUNT(*) as cnt FROM company_master
        WHERE rprsntDtlPrdnm IS NOT NULL AND rprsntDtlPrdnm != ''
        GROUP BY rprsntDtlPrdnm
    """, conn_cp).set_index('rprsntDtlPrdnm')['cnt'].to_dict()
    # 대표세부품명별 부산 업체 상세 리스트 (유출품목 업체 검색용)
    supplier_names_df = pd.read_sql("""
        SELECT rprsntDtlPrdnm, corpNm, ceoNm, adrs, dtlAdrs,
               rprsntDtlPrdnm as prdnm, rprsntIndstrytyNm,
               hdoffceDivNm, opbizDt
        FROM company_master
        WHERE rprsntDtlPrdnm IS NOT NULL AND rprsntDtlPrdnm != ''
        AND corpNm IS NOT NULL AND corpNm != ''
    """, conn_cp)
    supplier_names_map = {}
    for prd, grp in supplier_names_df.groupby('rprsntDtlPrdnm'):
        seen = set()
        details = []
        for _, r in grp.iterrows():
            nm = str(r['corpNm']).strip()
            if nm in seen:
                continue
            seen.add(nm)
            addr = str(r.get('adrs', '') or '').strip()
            dtl = str(r.get('dtlAdrs', '') or '').strip()
            full_addr = f"{addr} {dtl}".strip() if addr else dtl
            details.append({
                "업체명": nm,
                "대표자": str(r.get('ceoNm', '') or '').strip(),
                "주소": full_addr,
                "대표품명": str(r.get('prdnm', '') or '').strip(),
                "대표업종": str(r.get('rprsntIndstrytyNm', '') or '').strip(),
                "본사구분": str(r.get('hdoffceDivNm', '') or '').strip(),
                "개업일": str(r.get('opbizDt', '') or '').strip(),
            })
        supplier_names_map[prd] = details
    conn_cp.close()
    
    # 쇼핑몰 품목분류→세부품명 매핑 (상위분류로 검색 시 세부분류 업체까지 합산)
    conn_shop = sqlite3.connect(DB_PROCUREMENT)
    cat_children = {}
    for r in conn_shop.execute("""
        SELECT DISTINCT prdctClsfcNoNm, dtilPrdctClsfcNoNm
        FROM shopping_cntrct
        WHERE prdctClsfcNoNm IS NOT NULL AND dtilPrdctClsfcNoNm IS NOT NULL
        AND prdctClsfcNoNm != dtilPrdctClsfcNoNm
    """).fetchall():
        cat_children.setdefault(r[0], set()).add(r[1])
    conn_shop.close()
    
    def get_supplier_count(item_nm):
        """품목명으로 부산 공급가능 업체 수 조회 (정확매칭 → 세부분류 합산)"""
        cnt = supplier_map.get(item_nm, 0)
        if cnt > 0:
            return cnt
        children = cat_children.get(item_nm, set())
        if children:
            return sum(supplier_map.get(child, 0) for child in children)
        return 0
    
    def get_supplier_names(item_nm):
        """품목명으로 부산 공급업체 상세 리스트 조회 (dict 리스트)"""
        details = supplier_names_map.get(item_nm, [])
        if details:
            return details
        children = cat_children.get(item_nm, set())
        if children:
            all_details = []
            seen = set()
            for child in children:
                for d in supplier_names_map.get(child, []):
                    if d["업체명"] not in seen:
                        seen.add(d["업체명"])
                        all_details.append(d)
            return all_details
        return []
    
    conn = sqlite3.connect(DB_PROCUREMENT)
    
    # ========== 필터 데이터 로딩 ==========
    print("  입찰공고/낙찰정보 로딩...")
    bid_dict, bid_df = load_bid_dict(conn)
    award_sets = load_award_sets(conn)
    award_all = set().union(*award_sets.values())  # 보호제도 분석용 전체 합집합
    print(f"    입찰공고: {len(bid_dict):,}건, 낙찰정보: 공사 {len(award_sets['공사']):,} / 용역 {len(award_sets['용역']):,} / 물품 {len(award_sets['물품']):,}")
    
    # ========== compare_unit 헬퍼 ==========
    def get_unit(cd):
        unit = inst_unit.get(cd)
        if unit and inst_mid.get(cd,'') == '부산광역시 교육청':
            return '부산교육청'
        return unit
    
    # ========== 집계 ==========
    all_data = {}    # {분야: {그룹: {total, local}}}
    unit_data = {}   # {분야: {compare_unit: {total, local}}}
    
    # --- 공사 (현장 필터 포함) ---
    print("  [공사] 계산 중...")
    df = pd.read_sql("""SELECT untyCntrctNo, dcsnCntrctNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt,
        corpList, ntceNo, dminsttList, cnstwkNm, cntrctInsttOfclTelNo
        FROM cnstwk_cntrct""", conn)
    df.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
    n_before = len(df); df = dedup_by_dcsn(df)
    print(f"    차수 중복제거: {n_before - len(df)}건")
    
    # 공사현장 필터링
    df_filtered, n_drop, amt_drop = filter_cnstwk_by_site(df, bid_df)
    print(f"    현장 타지역 {n_drop}건 배제 ({amt_drop/1e8:.0f}억)")
    
    grp_r = {}
    ag_r = defaultdict(lambda:{'total':0,'local':0})
    for _, row in df_filtered.iterrows():
        result = process_contract_row(row, inst_dict, biznos,
                                       use_location_filter=True,
                                       bid_dict=bid_dict,
                                       award_set=award_sets['공사'])
        if not result: continue
        cd, amt, loc = result
        lrg = inst_grp.get(cd)
        unit = get_unit(cd)
        if not lrg or not unit: continue
        if lrg not in grp_r: grp_r[lrg] = {'total':0,'local':0}
        grp_r[lrg]['total'] += amt; grp_r[lrg]['local'] += loc
        ag_r[unit]['total'] += amt; ag_r[unit]['local'] += loc
    
    all_data['공사'] = {k:{'total':round(v['total']),'local':round(v['local'])} for k,v in grp_r.items()}
    unit_data['공사'] = {k:{'total':round(v['total']),'local':round(v['local'])} for k,v in ag_r.items()}
    print(f"    완료 ({len(df_filtered):,}건)")
    
    # --- 용역/물품 (전화번호+키워드 필터 포함) ---
    for tbl, name, award_key in [('servc_cntrct','용역','용역'),('thng_cntrct','물품','물품')]:
        print(f"  [{name}] 계산 중...")
        # 용역은 cnstrtsiteRgnNm 포함하여 로드
        extra_col = ', cnstrtsiteRgnNm' if tbl == 'servc_cntrct' else ''
        df = pd.read_sql(f"""SELECT untyCntrctNo, dcsnCntrctNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt,
            corpList, ntceNo, dminsttList, cntrctNm, cntrctInsttOfclTelNo{extra_col}
            FROM [{tbl}]""", conn)
        df.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
        n_before = len(df); df = dedup_by_dcsn(df)
        if n_before > len(df): print(f"    차수 중복제거: {n_before - len(df)}건")
        
        # 용역: 현장 타지역 사전 배제
        n_site_drop = 0
        if tbl == 'servc_cntrct':
            df, n_site_drop, amt_site_drop = filter_servc_by_site(df, inst_dict)
            if n_site_drop > 0:
                print(f"    현장 타지역 {n_site_drop}건 배제 ({amt_site_drop/1e8:.0f}억)")
        
        grp_r = {}
        ag_r = defaultdict(lambda:{'total':0,'local':0})
        n_filtered = 0
        for _, row in df.iterrows():
            result = process_contract_row(row, inst_dict, biznos,
                                           use_location_filter=True,
                                           bid_dict=bid_dict,
                                           award_set=award_sets[award_key])
            if not result:
                n_filtered += 1
                continue
            cd, amt, loc = result
            lrg = inst_grp.get(cd)
            unit = get_unit(cd)
            if not lrg or not unit: continue
            if lrg not in grp_r: grp_r[lrg] = {'total':0,'local':0}
            grp_r[lrg]['total'] += amt; grp_r[lrg]['local'] += loc
            ag_r[unit]['total'] += amt; ag_r[unit]['local'] += loc

        
        all_data[name] = {k:{'total':round(v['total']),'local':round(v['local'])} for k,v in grp_r.items()}
        unit_data[name] = {k:{'total':round(v['total']),'local':round(v['local'])} for k,v in ag_r.items()}
        print(f"    완료 ({len(df):,}건, 현장배제 {n_site_drop} + 필터배제 {n_filtered}건)")
    

    
    # ── 유출계약 비고란 생성 함수 ──
    def gen_비고(row, sector, grp, biznos, bid_dict=None):
        """유출계약의 비고(장기계속/공동이행/지역제한/단독) 생성"""
        tot_amt = float(row.get('totCntrctAmt', 0) or 0)
        thtm_amt = float(row.get('thtmCntrctAmt', 0) or 0)
        is_long = tot_amt > thtm_amt * 1.5 and tot_amt > 0 and thtm_amt > 0
        
        biz_list = parse_corp_shares(row.get('corpList', ''))
        n_comp = len(biz_list)
        is_joint = n_comp >= 2
        
        # 부산업체 지분 계산
        busan_share = 0
        for bno, share in biz_list:
            if bno in biznos or (len(bno) >= 3 and bno[:3] in BUSAN_BIZNO_PREFIXES):
                busan_share += share
        busan_share = round(busan_share)
        
        is_busan_grp = '부산' in str(grp or '')
        
        if sector == '용역':
            check_amt = tot_amt
            if is_busan_grp and check_amt < 330_000_000:
                remark = "비정상(지역제한비적용)"
            elif not is_busan_grp and check_amt < 220_000_000:
                remark = "비정상(지역제한비적용)"
            else:
                remark = "단독유출" if not is_joint else ""
            return remark

        if is_joint:
            # 공동이행 지분 검증
            if is_busan_grp and busan_share < 40:
                remark = f"공동이행(비정상) 부산{busan_share}%"
            elif not is_busan_grp and busan_share < 30:  # 국가기관도 30% 미만이면 의심
                remark = f"공동이행(비정상) 부산{busan_share}%"
            else:
                if is_long:
                    remark = f"장기계속 · 공동이행 부산{busan_share}%"
                else:
                    remark = f"공동이행(정상) 부산{busan_share}%"
        else:
            # 단독계약 (유출건이므로 부산업체 지분 0%)
            if sector == '공사':
                check_amt = tot_amt
                if is_busan_grp:
                    if check_amt <= 10_000_000_000:
                        remark = "지역제한 미적용"
                    else:
                        # 100억 초과는 지역제한 대상은 아니나, 
                        # 단독 유출이면 의무공동(40%) 위반임
                        remark = "의무공동 미적용(단독)"
                else:
                    if check_amt <= 8_800_000_000:
                        remark = "지역제한 미적용"
                    else:
                        remark = "장기계속" if is_long else "단독유출"
            else:
                remark = "장기계속" if is_long else "단독유출"
        return remark

    # 수의계약 기관별 상세 (공사/용역/물품만 — 쇼핑몰 제외)
    agency_suui_details = defaultdict(lambda: {
        "총발주액": 0, "총수주액": 0, "총수주율": 0, "그룹": "",
        "분야별": {},
        "유출계약": {"공사": [], "용역": [], "물품": []}
    })
    # 쇼핑몰 기관별 상세 (별도 탭용)
    agency_shop_details = defaultdict(lambda: {
        "총발주액": 0, "총수주액": 0, "총수주율": 0, "그룹": "",
        "유출계약": []
    })

    # --- 쇼핑몰 (공사자재 현장 필터) + 유출품목 집계 ---
    print("  [쇼핑몰] 계산 중...")
    df = pd.read_sql("""SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, dminsttCd,
        prdctAmt, cntrctCorpBizno, prdctClsfcNoNm,
        cnstwkMtrlDrctPurchsObjYn, dlvrReqNm FROM shopping_cntrct""", conn)
    df['dlvrReqChgOrd'] = pd.to_numeric(df['dlvrReqChgOrd'], errors='coerce').fillna(0)
    df.sort_values('dlvrReqChgOrd', ascending=False, inplace=True)
    df.drop_duplicates(subset=['dlvrReqNo','prdctSno'], keep='first', inplace=True)
    
    df, n_site_drop, amt_site_drop = filter_shopping_by_site(
        df, conn, set(inst_dict.keys()), inst_dict=inst_dict)
    print(f"    쇼핑몰 현장배제: {n_site_drop}건 ({amt_site_drop/1e8:.1f}억)")
    
    grp_r = {}
    ag_r = defaultdict(lambda:{'total':0,'local':0})
    # 유출품목 집계용
    item_total = defaultdict(float)
    item_leak = defaultdict(float)
    item_count = defaultdict(int)
    item_top_agency = defaultdict(lambda: defaultdict(float))  # 품목→{기관:금액}
    # 관급자재 vs 일반물품 구분 집계 (그룹별)
    shop_type_stats = defaultdict(lambda: {
        '관급자재': {'total': 0, 'local': 0},
        '일반물품': {'total': 0, 'local': 0},
    })
    _DISTRICTS = {"중구", "서구", "동구", "영도구", "부산진구", "동래구", "남구", "북구", "해운대구", "사하구", "금정구", "강서구", "연제구", "수영구", "사상구", "기장군"}
    
    for _, row in df.iterrows():
        result = process_contract_row(row, inst_dict, biznos, is_shopping=True)
        if not result: continue
        cd, amt, loc = result
        lrg = inst_grp.get(cd)
        unit = get_unit(cd)
        if not lrg or not unit: continue
        unit = str(unit).strip()
        if not unit or unit == 'nan': continue
        if lrg not in grp_r: grp_r[lrg] = {'total':0,'local':0}
        grp_r[lrg]['total'] += amt; grp_r[lrg]['local'] += loc
        ag_r[unit]['total'] += amt; ag_r[unit]['local'] += loc
        
        # 관급자재 vs 일반물품 구분
        is_material = str(row.get('cnstwkMtrlDrctPurchsObjYn', '') or '').strip().upper() == 'Y'
        stype = '관급자재' if is_material else '일반물품'
        shop_type_stats['전체'][stype]['total'] += amt
        shop_type_stats['전체'][stype]['local'] += loc
        shop_type_stats[lrg][stype]['total'] += amt
        shop_type_stats[lrg][stype]['local'] += loc
        # 구군 유형별 집계
        if unit in _DISTRICTS or any(unit.endswith(d) for d in _DISTRICTS):
            shop_type_stats['구군'][stype]['total'] += amt
            shop_type_stats['구군'][stype]['local'] += loc
            # 개별 구군 유형별 집계
            district_key = f'구군_{unit}'
            shop_type_stats[district_key][stype]['total'] += amt
            shop_type_stats[district_key][stype]['local'] += loc
        # 부산시 비구군 기관 유형별 집계
        if lrg == '부산광역시 및 소속기관' and unit not in _DISTRICTS:
            agency_key = f'부산기관_{unit}'
            shop_type_stats[agency_key][stype]['total'] += amt
            shop_type_stats[agency_key][stype]['local'] += loc
        
        # 유출품목 집계
        item_nm = str(row.get('prdctClsfcNoNm', '') or '').strip()
        if item_nm:
            item_total[item_nm] += amt
            leak_amt = amt - loc
            if leak_amt > 0:
                item_leak[item_nm] += leak_amt
                item_count[item_nm] += 1
                item_top_agency[item_nm][unit] += leak_amt
    
    all_data['쇼핑몰'] = {k:{'total':round(v['total']),'local':round(v['local'])} for k,v in grp_r.items()}
    unit_data['쇼핑몰'] = {k:{'total':round(v['total']),'local':round(v['local'])} for k,v in ag_r.items()}
    
    # 쇼핑몰 유출품목 Top 10
    leak_items = sorted(item_leak.items(), key=lambda x: x[1], reverse=True)[:10]
    leakage_shopping = []
    for item_nm, lk in leak_items:
        tot = item_total[item_nm]
        top_ag = max(item_top_agency[item_nm].items(), key=lambda x:x[1])[0] if item_top_agency[item_nm] else ''
        # 부산 공급가능 업체 수 (정확매칭 → 세부분류 합산)
        supplier_cnt = get_supplier_count(item_nm)
        leakage_shopping.append({
            "품목명": item_nm,
            "유출액": round(lk),
            "총액": round(tot),
            "유출율": round(lk/tot*100, 1) if tot > 0 else 0,
            "유출건수": item_count[item_nm],
            "주요수요기관": top_ag,
            "부산공급업체": supplier_cnt,
            "부산업체명": get_supplier_names(item_nm),
        })
    print(f"    완료 ({len(df):,}건, 유출품목 {len(leakage_shopping)}개)")
    
    # --- 공사/용역/물품 유출계약 Top 10 (core_calc 필터 적용 후) ---
    print("  [유출계약] 집계 중...")
    
    # bizno→지역 매핑 구축 (낙찰자 주소에서 추출)
    bizno_region = {}
    def _extract_city(addr):
        """주소에서 시/도 추출 (예: 부산광역시→부산, 서울특별시→서울)"""
        if not addr: return ''
        city = addr.split()[0]
        if '광역시' in city: return city.replace('광역시','')
        if '특별시' in city: return city.replace('특별시','')
        if '특별자치' in city: return city.split('특별자치')[0]
        if len(city) > 2 and city.endswith(('도','시')): return city[:-1]
        return city
    
    # 1) 낙찰정보 테이블
    for award_tbl in ['busan_award_cnstwk', 'busan_award_servc', 'busan_award_thng']:
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT bidwinnrBizno, bidwinnrAdrs FROM {award_tbl} WHERE bidwinnrAdrs IS NOT NULL AND bidwinnrAdrs != ''")
            for bno_row in cur.fetchall():
                bno = str(bno_row[0]).replace('-','').strip()
                addr = str(bno_row[1]).strip()
                if bno and addr:
                    city = _extract_city(addr)
                    if city and bno not in bizno_region:
                        bizno_region[bno] = city
        except: pass
    n_award = len(bizno_region)
    
    # 2) 부산업체 마스터DB (rgnNm 또는 adrs에서 추출)
    try:
        conn_cp2 = sqlite3.connect(DB_COMPANIES)
        for r in conn_cp2.execute("SELECT bizno, rgnNm, adrs FROM company_master WHERE bizno IS NOT NULL").fetchall():
            bno = str(r[0]).replace('-','').strip()
            if bno and bno not in bizno_region:
                rgn = str(r[1] or '').strip()
                city = _extract_city(rgn) if rgn else _extract_city(str(r[2] or ''))
                if city:
                    bizno_region[bno] = city
        conn_cp2.close()
    except: pass
    
    # 3) 계약 corpList에서 주소 추출 보충
    for tbl in ['cnstwk_cntrct', 'servc_cntrct', 'thng_cntrct']:
        try:
            for row_c in conn.execute(f"SELECT corpList FROM [{tbl}] WHERE corpList IS NOT NULL AND corpList != ''").fetchall():
                for chunk in str(row_c[0]).split('[')[1:]:
                    parts = chunk.split(']')[0].split('^')
                    if len(parts) >= 12:
                        bno = str(parts[9]).replace('-','').strip()
                        if bno and bno not in bizno_region:
                            addr = str(parts[11]).strip() if len(parts) > 11 else ''
                            city = _extract_city(addr)
                            if city:
                                bizno_region[bno] = city
        except: pass
    print(f"    bizno→지역 매핑: {len(bizno_region):,}건 (낙찰 {n_award:,} + 마스터/계약 {len(bizno_region)-n_award:,})")
    
    # 4) 사업자번호 앞 3자리(세무서 코드)로 지역 추정 (fallback)
    def _bizno_to_region(bno):
        """사업자번호 앞 3자리 세무서 코드 → 지역 추정"""
        if not bno or len(bno) < 3: return ''
        try:
            prefix = int(bno[:3])
        except: return ''
        # 세무서 코드 → 시도 매핑
        if 101 <= prefix <= 199: return '서울'
        if 200 <= prefix <= 299:
            if 201 <= prefix <= 220: return '인천'
            return '경기'
        if 300 <= prefix <= 399:
            if 301 <= prefix <= 310: return '대전'
            if 311 <= prefix <= 315: return '세종'
            if 316 <= prefix <= 340: return '충남'
            if 341 <= prefix <= 360: return '충북'
            return '강원'
        if 400 <= prefix <= 499:
            if 401 <= prefix <= 420: return '광주'
            if 421 <= prefix <= 450: return '전남'
            return '전북'
        if 500 <= prefix <= 599:
            if 501 <= prefix <= 520: return '대구'
            return '경북'
        if 600 <= prefix <= 699:
            if 601 <= prefix <= 610: return '부산'
            if 611 <= prefix <= 615: return '울산'
            if 616 <= prefix <= 650: return '경남'
            return '제주'
        return ''
    
    # corpList에서 모든 bizno 수집 (10필드짜리도 포함) 후 fallback 적용
    n_before = len(bizno_region)
    for tbl in ['cnstwk_cntrct', 'servc_cntrct', 'thng_cntrct']:
        try:
            for row_c in conn.execute(f"SELECT corpList FROM [{tbl}] WHERE corpList IS NOT NULL AND corpList != ''").fetchall():
                for chunk in str(row_c[0]).split('[')[1:]:
                    parts = chunk.split(']')[0].split('^')
                    if len(parts) >= 10:
                        bno = str(parts[9]).replace('-','').strip()
                        if bno and bno not in bizno_region:
                            rgn = _bizno_to_region(bno)
                            if rgn:
                                bizno_region[bno] = rgn
        except: pass
    # shopping_cntrct도 보강
    try:
        for row_s in conn.execute("SELECT cntrctCorpBizno FROM shopping_cntrct WHERE cntrctCorpBizno IS NOT NULL").fetchall():
            bno = str(row_s[0]).replace('-','').strip()
            if bno and bno not in bizno_region:
                rgn = _bizno_to_region(bno)
                if rgn:
                    bizno_region[bno] = rgn
    except: pass
    n_fallback = len(bizno_region) - n_before
    print(f"    사업자번호 추정 보강: +{n_fallback:,}건 → 총 {len(bizno_region):,}건")
    
    leak_contracts = []
    
    # 공사: 이미 필터된 df_filtered 재사용
    for _, row in df_filtered.iterrows():
        res = process_contract_row(row, inst_dict, biznos,
                                    use_location_filter=True,
                                    bid_dict=bid_dict, award_set=award_sets['공사'])
        if not res: continue
        cd, amt, loc = res
        if amt == 0: continue
        nloc = amt - loc
        if nloc < amt * 0.5: continue
        unit = get_unit(cd)
        grp = inst_grp.get(cd, "")
        # 업체명 추출
        corp_nm = ''
        for chunk in str(row.get('corpList','') or '').split('[')[1:]:
            parts = chunk.split(']')[0].split('^')
            if len(parts) >= 10:
                bno = str(parts[9]).replace('-','').strip()
                if bno not in biznos and not (len(bno) >= 3 and bno[:3] in BUSAN_BIZNO_PREFIXES) and len(parts) >= 4:
                    corp_nm = parts[3].strip(); break
        leak_contracts.append({
            "분야": "공사", "수요기관": unit or '', "계약명": str(row.get('cnstwkNm',''))[:60],
            "계약액": round(amt), "유출액": round(nloc),
            "유출율": round(nloc/amt*100, 1), "수주업체": corp_nm[:40],
            "그룹": grp, "비고": gen_비고(row, '공사', grp, biznos, bid_dict),
        })
    
    # 용역/물품 유출계약
    conn2 = sqlite3.connect(DB_PROCUREMENT)
    for tbl, sector, awk in [('servc_cntrct','용역','용역'),('thng_cntrct','물품','물품')]:
        cols_t = [r[1] for r in conn2.execute(f"PRAGMA table_info({tbl})").fetchall()]
        nm_col = 'cntrctNm' if 'cntrctNm' in cols_t else "''"
        extra_col = ', cnstrtsiteRgnNm' if tbl == 'servc_cntrct' else ''
        dcsn_col = ', dcsnCntrctNo' if 'dcsnCntrctNo' in cols_t else ''
        df_l = pd.read_sql(f"""SELECT untyCntrctNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt,
            corpList, ntceNo, dminsttList, {nm_col} as cntrctNm, cntrctInsttOfclTelNo, dminsttCd{extra_col}{dcsn_col}
            FROM [{tbl}]""", conn2)
        df_l.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
        df_l = dedup_by_dcsn(df_l)
        if tbl == 'servc_cntrct':
            df_l, _, _ = filter_servc_by_site(df_l, inst_dict)
        for _, row in df_l.iterrows():
            res = process_contract_row(row, inst_dict, biznos,
                                        use_location_filter=True,
                                        bid_dict=bid_dict, award_set=award_sets[awk])
            if not res: continue
            cd, amt, loc = res
            if amt == 0: continue
            nloc = amt - loc
            if nloc < amt * 0.5: continue
            unit = get_unit(cd)
            grp = inst_grp.get(cd, "")
            corp_nm = ''
            for chunk in str(row.get('corpList','') or '').split('[')[1:]:
                parts = chunk.split(']')[0].split('^')
                if len(parts) >= 10:
                    bno = str(parts[9]).replace('-','').strip()
                    if bno not in biznos and not (len(bno) >= 3 and bno[:3] in BUSAN_BIZNO_PREFIXES) and len(parts) >= 4:
                        corp_nm = parts[3].strip(); break
            leak_contracts.append({
                "분야": sector, "수요기관": unit or '', "계약명": str(row.get('cntrctNm',''))[:60],
                "계약액": round(amt), "유출액": round(nloc),
                "유출율": round(nloc/amt*100, 1), "수주업체": corp_nm[:40],
                "그룹": grp, "비고": gen_비고(row, sector, grp, biznos, bid_dict),
            })

    # 쇼핑몰 유출계약
    df_shop = pd.read_sql("SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, dminsttCd, prdctAmt, cntrctCorpBizno, corpNm, dlvrReqNm, cnstwkMtrlDrctPurchsObjYn FROM shopping_cntrct", conn2)
    df_shop['dlvrReqChgOrd'] = pd.to_numeric(df_shop['dlvrReqChgOrd'], errors='coerce').fillna(0)
    df_shop.sort_values('dlvrReqChgOrd', ascending=False, inplace=True)
    df_shop.drop_duplicates(subset=['dlvrReqNo','prdctSno'], keep='first', inplace=True)
    df_shop, _, _ = filter_shopping_by_site(df_shop, conn2, set(inst_dict.keys()), inst_dict=inst_dict)
    
    # 그룹핑 (dlvrReqNo 단위 합산)
    df_shop['prdctAmt'] = pd.to_numeric(df_shop['prdctAmt'], errors='coerce').fillna(0)
    grouped_shop = df_shop.groupby([
        'dlvrReqNo', 'dminsttCd', 'cntrctCorpBizno', 'corpNm', 'dlvrReqNm', 'cnstwkMtrlDrctPurchsObjYn'
    ], as_index=False, dropna=False).agg({
        'prdctAmt': 'sum'
    })
    
    for _, row in grouped_shop.iterrows():
        result = process_contract_row(row, inst_dict, biznos, is_shopping=True)
        if not result: continue
        cd, amt, loc = result
        if amt == 0: continue
        nloc = amt - loc
        unit = get_unit(cd)
        grp = inst_grp.get(cd, "")
        bno = str(row.get('cntrctCorpBizno','')).replace('-','').strip()
        corp_nm = ''
        if bno and bno not in biznos and not (len(bno) >= 3 and bno[:3] in BUSAN_BIZNO_PREFIXES):
            corp_nm = str(row.get('corpNm','')).strip()
        if nloc >= amt * 0.5:
            leak_contracts.append({
                "분야": "쇼핑몰", "수요기관": unit or '', "계약명": str(row.get('dlvrReqNm',''))[:60],
                "계약액": round(amt), "유출액": round(nloc),
                "유출율": round(nloc/amt*100, 1), "수주업체": corp_nm[:40],
                "그룹": grp, "비고": "직접구매",
                "관급자재여부": str(row.get('cnstwkMtrlDrctPurchsObjYn', '') or '').strip().upper(),
            })
        
        # --- agency_shop_details (쇼핑몰 별도 탭용) ---
        agency_shop_details[unit]["총발주액"] += amt
        agency_shop_details[unit]["총수주액"] += loc
        if grp: agency_shop_details[unit]["그룹"] = grp
        # 유출이 있는 계약만 저장 (관급자재 + 일반물품 모두 포함)
        if nloc > 0:
            agency_shop_details[unit]["유출계약"].append({
                "분야": "쇼핑몰", "수요기관": unit or '', "계약명": str(row.get('dlvrReqNm',''))[:60],
                "계약액": round(amt), "유출액": round(nloc),
                "유출율": round(nloc/amt*100, 1) if amt>0 else 0, "수주업체": corp_nm[:40], "그룹": grp,
                "비고": "직접구매",
                "관급자재여부": str(row.get('cnstwkMtrlDrctPurchsObjYn', '') or '').strip().upper(),
            })
        # -------------------------------------
        
    conn2.close()
    
    leak_contracts.sort(key=lambda x: x['유출액'], reverse=True)
    leakage_contracts = leak_contracts[:50]
    print(f"    유출계약(공사/용역/물품) 후보 {len(leak_contracts):,}건 중 Top 50 선정")
    
    # ========== 보호제도 미적용 분석 (계약 기반) ==========
    # 계약DB에서 출발 → 일반/제한경쟁 → 지분율로 미적용 판단
    print("  [보호제도] 계약 기반 분석 중...")

    SPECIALTY_TYPES = ['전기공사', '정보통신공사', '소방시설공사', '기계설비공사',
                       '전기', '통신', '소방', '기계설비', '기계공사', '정보통신',
                       '조경', '실내건축', '철근·콘크리트', '상하수도', '포장',
                       '철강구조물', '금속구조물창호', '도장', '습식방수', '석공사',
                       '비계', '지반조성', '철도궤도']
    PROT_THRESHOLDS = {
        '부산광역시 및 소속기관': {'종합공사': 100e8, '전문공사': 10e8, '용역': 3.3e8},
        '정부 및 국가공공기관':  {'종합공사': 88e8,  '전문공사': 10e8, '용역': 2.2e8},
    }
    gov_stats = defaultdict(lambda: {'기준이하': 0, '지역제한': 0, '의무공동': 0, '미적용': 0, '미적용액': 0})
    bsn_stats = defaultdict(lambda: {'기준이하': 0, '지역제한': 0, '미적용': 0, '미적용액': 0})
    bsn_jnt = defaultdict(lambda: {'모수': 0, '의무공동': 0})
    prot_by_agency = defaultdict(lambda: {'total': 0, 'applied': 0, 'unapplied': 0, 'unapplied_amt': 0, 'grp': ''})
    prot_violations = []

    # 공고 추정가격 lookup (계약→공고 역매칭용)
    price_map = {}
    for _r in conn.execute("SELECT REPLACE(bidNtceNo,'-',''), presmptPrce FROM bid_notices_price WHERE presmptPrce IS NOT NULL AND presmptPrce != ''").fetchall():
        if _r[0] and _r[1]:
            try: price_map[_r[0]] = float(_r[1])
            except: pass

    # 공고 현장지역 lookup (공사 현장 필터용)
    site_map = {}
    for _r in conn.execute("SELECT REPLACE(bidNtceNo,'-',''), cnstrtsiteRgnNm FROM bid_notices_price WHERE cnstrtsiteRgnNm IS NOT NULL").fetchall():
        if _r[0]: site_map[_r[0]] = str(_r[1] or '')

    prot_contract_queries = {
        'cnstwk_cntrct': ('공사', """SELECT ntceNo, corpList, totCntrctAmt, thtmCntrctAmt,
            dminsttCd, dminsttList, cntrctCnclsMthdNm, dcsnCntrctNo,
            cnstwkNm as cntrctNm, cntrctInsttOfclTelNo, '' as mainCnsttyNm,
            cnstwkTypeLrg, cnstwkTypeDtl
            FROM [cnstwk_cntrct]
            WHERE cntrctCnclsMthdNm IN ('일반경쟁', '제한경쟁')"""),
        'servc_cntrct': ('용역', """SELECT ntceNo, corpList, totCntrctAmt, thtmCntrctAmt,
            dminsttCd, dminsttList, cntrctCnclsMthdNm, dcsnCntrctNo,
            cntrctNm, cntrctInsttOfclTelNo, '' as mainCnsttyNm
            FROM [servc_cntrct]
            WHERE cntrctCnclsMthdNm IN ('일반경쟁', '제한경쟁')"""),
    }

    for tbl, (sector, query) in prot_contract_queries.items():
        rows = pd.read_sql(query, conn)
        # 장기계속 후속차수 제외 (최초계약만)
        rows = dedup_by_dcsn(rows)
        dcsn = rows['dcsnCntrctNo'].fillna('').astype(str).str.strip()
        rows = rows[~((dcsn.str.len() >= 10) & (~dcsn.str.endswith('00')))]

        for _, row in rows.iterrows():
            ntce_clean = str(row.get('ntceNo', '')).replace('-','').strip()
            method = str(row.get('cntrctCnclsMthdNm', ''))
            amt = float(row.get('thtmCntrctAmt', 0) or 0)
            if amt == 0: amt = float(row.get('totCntrctAmt', 0) or 0)
            if amt <= 0: continue
            name = str(row.get('cntrctNm', '') or '')

            # 기관 매칭
            cd = str(row.get('dminsttCd', '')).strip()
            matched_cd = cd if cd in inst_dict else None
            if not matched_cd:
                for dcd in extract_dminstt_codes(row.get('dminsttList', '')):
                    if dcd in inst_dict:
                        matched_cd = dcd; break
            if not matched_cd: continue

            grp = inst_grp.get(matched_cd)
            unit = get_unit(matched_cd)
            if not grp or grp not in PROT_THRESHOLDS or not unit: continue

            # 공사: 현장 타지역 배제 + 전화번호/키워드 필터 (수주율 계산과 동일)
            if sector == '공사':
                site = site_map.get(ntce_clean, '')
                if site and '부산' not in site: continue
                # 전화번호/계약명 키워드로 타지역 계약 추가 배제
                lrg = inst_dict.get(matched_cd, {}).get('cate_lrg', '')
                non_busan = is_non_busan_contract(row, lrg)
                if non_busan:
                    bypassed = False
                    if non_busan == 'tel' and ntce_clean and ntce_clean in award_all:
                        bypassed = True
                    if not bypassed and bid_dict and ntce_clean in bid_dict:
                        if check_busan_restriction(bid_dict[ntce_clean].get('rgnLmtInfo')):
                            bypassed = True
                    if not bypassed:
                        continue
                # 공사 세분류: cnstwkTypeLrg → mainCnsttyNm → 계약명 순으로 확인
                type_lrg = str(row.get('cnstwkTypeLrg', '') or '').strip()
                type_dtl = str(row.get('cnstwkTypeDtl', '') or '').strip()
                main_type = str(row.get('mainCnsttyNm', '') or '').strip()
                if type_lrg and any(k in type_lrg for k in SPECIALTY_TYPES): sub = '전문공사'
                elif type_dtl and any(k in type_dtl for k in SPECIALTY_TYPES): sub = '전문공사'
                elif main_type and any(k in main_type for k in SPECIALTY_TYPES): sub = '전문공사'
                elif any(k in name for k in SPECIALTY_TYPES): sub = '전문공사'
                else: sub = '종합공사'
            else:
                # 용역: 타지역 키워드 필터
                lrg = inst_dict.get(matched_cd, {}).get('cate_lrg', '')
                if is_non_busan_contract(row, lrg): continue
                sub = '용역'

            # 추정가격 (공고매칭 → 총계약액 → 당해계약액)
            # 장기계속계약은 당해(thtmCntrctAmt)가 작으므로 총계약액(totCntrctAmt) 우선
            tot_amt = float(row.get('totCntrctAmt', 0) or 0)
            est_price = price_map.get(ntce_clean, None) or tot_amt or amt
            threshold = PROT_THRESHOLDS[grp].get(sub)
            if not threshold: continue

            # 낙찰업체 지분 확인
            blist = parse_corp_shares(row.get('corpList', ''))
            if not blist: continue
            local_share = sum(s for b, s in blist if b in biznos or (len(b)>=3 and b[:3] in BUSAN_BIZNO_PREFIXES))

            # 수주업체 추출 (대표 1개사)
            corp_names = [parts[3].strip() for chunk in str(row.get('corpList', '')).split('[')[1:] if len((parts := chunk.split(']')[0].split('^'))) >= 4]
            corp_nm = corp_names[0] if corp_names else ""

            # ===== 판정 로직 =====
            if est_price < 1_000_000: continue  # 100만원 미만 이상 데이터 제외
            if grp == '정부 및 국가공공기관':
                if est_price > threshold: continue  # 기준 초과 → 대상 아님
                gov_stats[sub]['기준이하'] += 1
                prot_by_agency[unit]['total'] += 1
                prot_by_agency[unit]['grp'] = grp
                if method == '제한경쟁':
                    gov_stats[sub]['지역제한'] += 1
                    prot_by_agency[unit]['applied'] += 1
                elif local_share >= 30:
                    gov_stats[sub]['의무공동'] += 1  # 30%+ 지역 → 보호 작동
                    prot_by_agency[unit]['applied'] += 1
                else:
                    gov_stats[sub]['미적용'] += 1
                    gov_stats[sub]['미적용액'] += est_price
                    prot_by_agency[unit]['unapplied'] += 1
                    prot_by_agency[unit]['unapplied_amt'] += est_price
                    prot_violations.append({"분야": sub, "계약방식": method,
                        "공고명": name[:55], "추정가격": round(est_price),
                        "기관그룹": grp, "수요기관": str(row.get('dminsttNm', '') or inst_dict.get(matched_cd,{}).get('dminsttNm',''))[:25],
                        "비교단위": unit, "수주업체": corp_nm, "비고": "비정상(지역제한 미적용)"})

            elif grp == '부산광역시 및 소속기관':
                if sector == '공사' and est_price > threshold:
                    # 100억 이상 공사: 의무공동도급 (40%+ 지역지분)
                    bsn_stats[sub]['기준이하'] += 1
                    prot_by_agency[unit]['total'] += 1
                    prot_by_agency[unit]['grp'] = grp
                    if local_share >= 40:
                        bsn_stats[sub]['지역제한'] += 1
                        prot_by_agency[unit]['applied'] += 1
                    else:
                        bsn_stats[sub]['미적용'] += 1
                        bsn_stats[sub]['미적용액'] += est_price
                        prot_by_agency[unit]['unapplied'] += 1
                        prot_by_agency[unit]['unapplied_amt'] += est_price
                        prot_violations.append({"분야": sub, "계약방식": method,
                            "공고명": name[:55], "추정가격": round(est_price),
                            "기관그룹": grp, "수요기관": str(row.get('dminsttNm', '') or inst_dict.get(matched_cd,{}).get('dminsttNm',''))[:25],
                            "비교단위": unit, "수주업체": corp_nm, "비고": "비정상(의무공동 위반)"})
                elif est_price <= threshold:
                    # 기준이하: 지역제한경쟁 대상
                    bsn_stats[sub]['기준이하'] += 1
                    prot_by_agency[unit]['total'] += 1
                    prot_by_agency[unit]['grp'] = grp
                    if method == '제한경쟁':
                        bsn_stats[sub]['지역제한'] += 1
                        prot_by_agency[unit]['applied'] += 1
                    elif local_share >= 100:
                        bsn_stats[sub]['지역제한'] += 1  # 100% 부산 수주 → OK
                        prot_by_agency[unit]['applied'] += 1
                    else:
                        bsn_stats[sub]['미적용'] += 1
                        bsn_stats[sub]['미적용액'] += est_price
                        prot_by_agency[unit]['unapplied'] += 1
                        prot_by_agency[unit]['unapplied_amt'] += est_price
                        prot_violations.append({"분야": sub, "계약방식": method,
                            "공고명": name[:55], "추정가격": round(est_price),
                            "기관그룹": grp, "수요기관": str(row.get('dminsttNm', '') or inst_dict.get(matched_cd,{}).get('dminsttNm',''))[:25],
                            "비교단위": unit, "수주업체": corp_nm, "비고": "비정상(지역제한 미적용)"})
                # 의무공동도급 집계
                if method != '제한경쟁':
                    sec_key = '공사' if sub in ('종합공사', '전문공사') else '용역'
                    bsn_jnt[sec_key]['모수'] += 1
                    jnt_yn = str(row.get('rgnDutyJntcontrctYn', '') or '').strip().upper()
                    if jnt_yn == 'Y' or (sector == '공사' and local_share >= 30 and local_share < 100):
                        bsn_jnt[sec_key]['의무공동'] += 1

    # --- B) 수의계약 (장기계속 후속차수 제외: 최초계약만 포함) ---
    # dcsnCntrctNo 끝2자리 '00' = 최초계약, 그 외 = 후속차수(이미 업체 결정된 건)
    suui_queries = {
        'cnstwk_cntrct': ('공사', """SELECT untyCntrctNo, dminsttCd, corpList,
            totCntrctAmt, thtmCntrctAmt, dminsttList, cnstwkNm as cntrctNm,
            cntrctInsttOfclTelNo, ntceNo FROM [cnstwk_cntrct]
            WHERE cntrctCnclsMthdNm='수의계약'
            AND (dcsnCntrctNo LIKE '%00' OR dcsnCntrctNo IS NULL OR dcsnCntrctNo = '')"""),
        'servc_cntrct': ('용역', """SELECT untyCntrctNo, dminsttCd, corpList,
            totCntrctAmt, thtmCntrctAmt, dminsttList, cntrctNm,
            cntrctInsttOfclTelNo, ntceNo FROM [servc_cntrct]
            WHERE cntrctCnclsMthdNm='수의계약'
            AND (dcsnCntrctNo LIKE '%00' OR dcsnCntrctNo IS NULL OR dcsnCntrctNo = '')"""),
        'thng_cntrct': ('물품', """SELECT untyCntrctNo, dminsttCd, corpList,
            totCntrctAmt, thtmCntrctAmt, dminsttList, cntrctNm,
            cntrctInsttOfclTelNo, ntceNo FROM [thng_cntrct]
            WHERE cntrctCnclsMthdNm='수의계약'
            AND (dcsnCntrctNo LIKE '%00' OR dcsnCntrctNo IS NULL OR dcsnCntrctNo = '')"""),
    }
    suui_stats = defaultdict(lambda: {'total': 0, 'busan': 0, 'non_busan': 0, 'non_busan_amt': 0})
    suui_leakages = []
    for tbl, (sector, sql) in suui_queries.items():
        suui_df = pd.read_sql(sql, conn)
        suui_df.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
        for _, row in suui_df.iterrows():
            inst_cd = str(row.get('dminsttCd', '')).strip()
            if not inst_cd or inst_cd not in inst_dict:
                for dcd in extract_dminstt_codes(row.get('dminsttList', '')):
                    if dcd in inst_dict: inst_cd = dcd; break
            if inst_cd not in inst_dict: continue
            grp = inst_grp.get(inst_cd)
            if not grp or grp not in PROT_THRESHOLDS: continue
            unit = get_unit(inst_cd)
            if not unit: continue
            lrg = inst_dict[inst_cd]['cate_lrg']
            if is_non_busan_contract(row, lrg):
                ntce_no = str(row.get('ntceNo', '')).replace('-', '').strip()
                bypassed = ntce_no and ntce_no in award_sets.get(sector, set())
                if not bypassed and bid_dict and ntce_no and ntce_no in bid_dict:
                    bypassed = check_busan_restriction(bid_dict[ntce_no].get('rgnLmtInfo'))
                if not bypassed: continue
            biz_list = parse_corp_shares(row.get('corpList', ''))
            if not biz_list: continue
            
            # --- agency_suui_details logic ---
            res = process_contract_row(row, inst_dict, biznos, use_location_filter=True, bid_dict=bid_dict, award_set=award_sets.get(sector, set()))
            if res:
                _, p_amt, p_loc = res
                if p_amt > 0:
                    if sector not in agency_suui_details[unit]["분야별"]:
                        agency_suui_details[unit]["분야별"][sector] = {"발주액": 0, "수주액": 0}
                    agency_suui_details[unit]["총발주액"] += p_amt
                    agency_suui_details[unit]["총수주액"] += p_loc
                    if grp: agency_suui_details[unit]["그룹"] = grp
                    agency_suui_details[unit]["분야별"][sector]["발주액"] += p_amt
                    agency_suui_details[unit]["분야별"][sector]["수주액"] += p_loc
                    pnloc = p_amt - p_loc
                    if pnloc >= p_amt * 0.5:
                        corp_nmm = ''
                        for chunk in str(row.get('corpList','') or '').split('[')[1:]:
                            parts = chunk.split(']')[0].split('^')
                            if len(parts) >= 10:
                                bnn = str(parts[9]).replace('-','').strip()
                                if bnn not in biznos and not (len(bnn) >= 3 and bnn[:3] in BUSAN_BIZNO_PREFIXES) and len(parts) >= 4:
                                    corp_nmm = parts[3].strip(); break
                        agency_suui_details[unit]["유출계약"][sector].append({
                            "분야": sector, "수요기관": unit, "계약명": str(row.get('cntrctNm', '') or '')[:60],
                            "계약액": round(p_amt), "유출액": round(pnloc),
                            "유출율": round(pnloc/p_amt*100, 1) if p_amt>0 else 0, "수주업체": corp_nmm[:40], "그룹": grp,
                            "비고": gen_비고(row, sector, grp, biznos, bid_dict)
                        })
            # ---------------------------------
            
            key = f"{grp}_{sector}"
            suui_stats[key]['total'] += 1
            amt = float(row.get('thtmCntrctAmt', 0) or 0)
            if amt == 0: amt = float(row.get('totCntrctAmt', 0) or 0)
            if sum(1 for bno, _ in biz_list if bno in biznos or (len(bno) >= 3 and bno[:3] in BUSAN_BIZNO_PREFIXES)) > 0:
                suui_stats[key]['busan'] += 1
                suui_stats[key]['busan_amt'] = suui_stats[key].get('busan_amt', 0) + amt
            else:
                suui_stats[key]['non_busan'] += 1
                suui_stats[key]['non_busan_amt'] += amt
                suui_leakages.append({
                    "분야": sector, "계약명": str(row.get('cntrctNm', '') or '')[:60],
                    "금액": round(amt), "그룹": grp, "수요기관": unit,
                })

    prot_violations.sort(key=lambda x: x['추정가격'], reverse=True)
    prot_violations = [v for v in prot_violations if v['추정가격'] >= 1_000_000]  # 100만원 미만 이상 데이터 제외

    protection_summary = {
        "정부 및 국가공공기관": {sub: dict(gov_stats[sub]) for sub in gov_stats},
        "부산시 및 소관기관_지역제한": {sub: dict(bsn_stats[sub]) for sub in bsn_stats},
        "부산시 및 소관기관_의무공동": {sec: dict(bsn_jnt[sec]) for sec in bsn_jnt},
        "수의계약": {key: dict(suui_stats[key]) for key in suui_stats},
    }
    prot_agency_ranking = sorted(
        [{"기관": u, "기관그룹": d['grp'], "기준이하": d['total'], "적용": d['applied'],
          "미적용": d['unapplied'], "미적용금액": round(d['unapplied_amt']),
          "미적용률": round(d['unapplied']/d['total']*100, 1) if d['total'] else 0}
         for u, d in prot_by_agency.items() if d['total'] > 0],
        key=lambda x: x['미적용'], reverse=True)

    print("    ■ 국가:")
    for sub in ['종합공사', '전문공사', '용역']:
        s = gov_stats.get(sub, {})
        if s.get('기준이하', 0) > 0:
            print(f"      {sub}: 기준이하 {s['기준이하']:,} → 지역제한 {s['지역제한']:,} / 의무공동 {s['의무공동']:,} / 미적용 {s['미적용']:,}")
    print("    ■ 부산시 [지역제한]:")
    for sub in ['종합공사', '전문공사', '용역']:
        s = bsn_stats.get(sub, {})
        if s.get('기준이하', 0) > 0:
            rr = s['지역제한']/s['기준이하']*100
            print(f"      {sub}: 기준이하 {s['기준이하']:,} → 지역제한 {s['지역제한']:,} ({rr:.1f}%) / 미적용 {s['미적용']:,}")
    print("    ■ 부산시 [의무공동수급 - 금액무관]:")
    for sec in ['공사', '용역']:
        j = bsn_jnt.get(sec, {})
        if j.get('모수', 0) > 0:
            rt = j['의무공동']/j['모수']*100
            print(f"      {sec}: 적용대상(총발주-지역제한-수의) {j['모수']:,}건 → 의무공동 {j['의무공동']:,} ({rt:.1f}%)")
    print("    ■ 수의계약:")
    for key in sorted(suui_stats.keys()):
        s = suui_stats[key]
        print(f"      {key}: {s['total']:,}건 → 부산 {s['busan']:,} / 비부산 {s['non_busan']:,}")
    top3_str = ', '.join(f"{a['기관']}({a['미적용']}건)" for a in prot_agency_ranking[:3])
    print(f"    미적용 Top3: {top3_str}")

    conn.close()
    
    # ========== JSON 캐시 구조 ==========
    sectors = ['공사','용역','물품','쇼핑몰']
    groups = ['부산광역시 및 소속기관', '정부 및 국가공공기관']
    
    unit_to_grp = {}
    for cd, grp in inst_grp.items():
        unit = get_unit(cd)
        if unit: unit_to_grp[unit] = grp
    
    gt = sum(d['total'] for s in sectors for d in all_data[s].values())
    gl = sum(d['local'] for s in sectors for d in all_data[s].values())
    
    sec_sum = {}
    for s in sectors:
        st = sum(d['total'] for d in all_data[s].values())
        sl = sum(d['local'] for d in all_data[s].values())
        sec_sum[s] = {"발주액":st, "수주액":sl, "수주율":pct(st,sl)}
    
    grp_sum = {}
    for g in groups:
        t = sum(all_data[s].get(g,{}).get('total',0) for s in sectors)
        l = sum(all_data[s].get(g,{}).get('local',0) for s in sectors)
        grp_sum[g] = {"발주액":t, "수주액":l, "수주율":pct(t,l)}
    
    grp_sec = {}
    for g in groups:
        grp_sec[g] = {}
        for s in sectors:
            d = all_data[s].get(g,{'total':0,'local':0})
            grp_sec[g][s] = {"발주액":d['total'], "수주액":d['local'], "수주율":pct(d['total'],d['local'])}
    
    def rankings(sector_filter=None):
        min_amt = MIN_AMT.get(sector_filter, MIN_AMT[None])
        result = {}
        for g in groups:
            agg = defaultdict(lambda:{'total':0,'local':0})
            for s in ([sector_filter] if sector_filter else sectors):
                for unit, d in unit_data[s].items():
                    if unit_to_grp.get(unit) == g:
                        agg[unit]['total'] += d['total']
                        agg[unit]['local'] += d['local']
            filtered = [(u,d) for u,d in agg.items() if d['total'] >= min_amt]
            scored = sorted([
                {"비교단위":u, "발주액":round(d['total']), "수주액":round(d['local']), "수주율":pct(d['total'],d['local'])}
                for u,d in filtered
            ], key=lambda x: x['수주율'])
            top_n = TOP_N.get(g, 15)
            result[g] = {
                "최소기준액": f"{min_amt/1e8:.0f}억원",
                "해당기관수": len(scored),
                "표출수": top_n,
                "상위": list(reversed(scored[-top_n:])),
                "하위": [x for x in scored[:top_n] if x not in scored[-top_n:]]  # 상위와 중복 제거
            }
        return result
    
    BUSAN_DISTRICTS = {"중구", "서구", "동구", "영도구", "부산진구", "동래구", "남구", "북구", "해운대구", "사하구", "금정구", "강서구", "연제구", "수영구", "사상구", "기장군"}
    shop_districts = {"발주액": 0, "수주액": 0, "수주율": 0}
    if '쇼핑몰' in unit_data:
        for u, d in unit_data['쇼핑몰'].items():
            name = str(u).strip()
            if name in BUSAN_DISTRICTS or any(name.endswith(bdu) for bdu in BUSAN_DISTRICTS):
                shop_districts["발주액"] += d["total"]
                shop_districts["수주액"] += d["local"]
        shop_districts["수주율"] = pct(shop_districts["발주액"], shop_districts["수주액"])

    # 관급자재/일반물품 구분 캐시 직렬화
    def _type_summary(d):
        return {
            k: {"발주액": round(v["total"]), "수주액": round(v["local"]),
                "수주율": pct(v["total"], v["local"])}
            for k, v in d.items()
        }
    shop_type_cache = {grp: _type_summary(td) for grp, td in shop_type_stats.items()}

    cache = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "데이터_기간": "2026-01-01 ~ 현재",
        "1_전체": {"발주액":gt, "수주액":gl, "수주율":pct(gt,gl)},
        "2_분야별": sec_sum,
        "3_그룹별": grp_sum,
        "4_그룹별_분야별": grp_sec,
        "5_기관랭킹_전체": rankings(),
        "5_기관랭킹_분야별": {s: rankings(s) for s in sectors},
        "6_유출품목_쇼핑몰": leakage_shopping,
        "7_유출계약_주요": leakage_contracts,
        "8_보호제도_현황": protection_summary,
        "8_보호제도_미적용": prot_violations,
        "8_보호제도_기관별": prot_agency_ranking,
        "9_수의계약": {
            key: {**dict(suui_stats[key]),
                  "busan_amt": round(suui_stats[key].get('busan_amt', 0)),
                  "수주율_건수": round(suui_stats[key]['busan']/suui_stats[key]['total']*100, 1) if suui_stats[key]['total'] > 0 else 0}
            for key in suui_stats},
        "9_수의계약_유출": [],
        "9_수의계약_유출_기관별": [],
        "15_쇼핑몰_구군_상세": shop_districts,
        "16_쇼핑몰_유형별": shop_type_cache,
    }

    # 기관별 상세 검색용 데이터 (12_기관별_상세)
    agency_details = defaultdict(lambda: {
        "총발주액": 0, "총수주액": 0, "총수주율": 0, "그룹": "",
        "분야별": {},
        "유출계약": []
    })
    
    for s in sectors:
        for unit, d in unit_data[s].items():
            agency_details[unit]["총발주액"] += d["total"]
            agency_details[unit]["총수주액"] += d["local"]
            grp = unit_to_grp.get(unit, "")
            if grp: agency_details[unit]["그룹"] = grp
            # 분야별 데이터 추가
            agency_details[unit]["분야별"][s] = {
                "발주액": round(d["total"]),
                "수주액": round(d["local"]),
                "수주율": pct(d["total"], d["local"]),
            }
            
    for unit, details in agency_details.items():
        details["총수주율"] = pct(details["총발주액"], details["총수주액"])
        details["총발주액"] = round(details["총발주액"])
        details["총수주액"] = round(details["총수주액"])
        
    for lc in leak_contracts:
        u = lc["수요기관"]
        if u and u in agency_details:
            agency_details[u]["유출계약"].append(lc)

    cache["12_기관별_상세"] = dict(agency_details)
    
    for unit, details in agency_suui_details.items():
        details["총수주율"] = pct(details["총발주액"], details["총수주액"])
        details["총발주액"] = round(details["총발주액"])
        details["총수주액"] = round(details["총수주액"])
        for sct, sct_d in details["분야별"].items():
            sct_d["수주율"] = pct(sct_d["발주액"], sct_d["수주액"])
            sct_d["발주액"] = round(sct_d["발주액"])
            sct_d["수주액"] = round(sct_d["수주액"])
        for sct, leak_list in details["유출계약"].items():
            leak_list.sort(key=lambda x: x["유출액"], reverse=True)
            
    cache["13_수의계약_기관별_상세"] = dict(agency_suui_details)

    # ── 수의계약 유출 데이터 통일 (agency_suui_details 기반) ──
    # 기존 suui_leakages(이진 판정, 쇼핑몰 미포함) 대신
    # agency_suui_details(지분율 비례, 쇼핑몰 포함)로 통일하여
    # 수의계약 탭 내 좌측 차트 ↔ 하단 기관검색 수치 일치
    # (쇼핑몰도 경쟁입찰 없이 직접 구매하므로 수의계약 성격)
    _all_suui_leaks = [
        lk for d in agency_suui_details.values()
        for lks in d["유출계약"].values() for lk in lks
    ]
    cache["9_수의계약_유출"] = sorted(_all_suui_leaks, key=lambda x: x["유출액"], reverse=True)[:20]
    cache["9_수의계약_유출_기관별"] = sorted([
        {"기관": u, "유출액": round(d["총발주액"] - d["총수주액"]),
         "건수": sum(len(lk) for lk in d["유출계약"].values()),
         "그룹": d["그룹"]}
        for u, d in agency_suui_details.items()
        if d["총발주액"] - d["총수주액"] > 0
    ], key=lambda x: x["유출액"], reverse=True)[:15]

    # ── 쇼핑몰 별도 탭용 캐시 (agency_shop_details 기반) ──
    for unit, details in agency_shop_details.items():
        details["총수주율"] = pct(details["총발주액"], details["총수주액"])
        details["총발주액"] = round(details["총발주액"])
        details["총수주액"] = round(details["총수주액"])
        details["유출계약"].sort(key=lambda x: x["계약액"], reverse=True)
        details["유출계약"] = details["유출계약"][:100]  # 계약액 상위 100건
    cache["14_쇼핑몰_기관별_상세"] = dict(agency_shop_details)
    _all_shop_leaks = [
        lk for d in agency_shop_details.values() for lk in d["유출계약"]
    ]
    cache["14_쇼핑몰_유출"] = sorted(_all_shop_leaks, key=lambda x: x["유출액"], reverse=True)[:20]
    cache["14_쇼핑몰_유출_기관별"] = sorted([
        {"기관": u, "유출액": round(d["총발주액"] - d["총수주액"]),
         "발주액": d["총발주액"], "수주액": d["총수주액"], "수주율": d["총수주율"],
         "건수": len(d["유출계약"]), "그룹": d["그룹"]}
        for u, d in agency_shop_details.items()
        if d["총발주액"] - d["총수주액"] > 0
    ], key=lambda x: x["유출액"], reverse=True)[:50]
    
    # 지역업체 현황표 (busan_companies_master.db에서 집계)
    print("  [지역업체 현황표] 집계 중...")
    conn_cp2 = sqlite3.connect(DB_COMPANIES)
    c2 = conn_cp2.cursor()
    total_co = c2.execute("SELECT COUNT(*) FROM company_master").fetchone()[0]
    local_company_stats = {"전체": total_co}
    for div in ['물품', '용역', '공사']:
        cnt = c2.execute("SELECT COUNT(*) FROM company_master WHERE corpBsnsDivNm LIKE ?", (f'%{div}%',)).fetchone()[0]
        local_company_stats[div] = cnt
    local_company_stats["제조"] = c2.execute("SELECT COUNT(*) FROM company_master WHERE mnfctDivNm='제조'").fetchone()[0]
    local_company_stats["공급"] = c2.execute("SELECT COUNT(*) FROM company_master WHERE mnfctDivNm='공급'").fetchone()[0]
    
    # UNSPSC 대분류 코드 → 한국어명
    UNSPSC = {
        '10':'식물/동물/미생물','11':'광물/섬유원료','12':'화학제품','13':'수지/로진제품',
        '14':'종이/제지','15':'연료/윤활제','20':'광업장비','21':'농업/임업장비',
        '22':'건설운반장비','23':'산업제조장비','24':'운반/보관장비','25':'차량/운송장비',
        '26':'전력/발전장비','27':'공구/일반기계','30':'건축자재','31':'제조부품',
        '32':'전자부품','39':'조명/전선','40':'배관/냉난방','41':'실험/측정장비',
        '42':'의료장비','43':'IT/통신장비','44':'사무기기/용품','45':'영상/음향장비',
        '46':'안전/보안장비','47':'세척장비','48':'급식/주방장비','49':'스포츠/레저장비',
        '50':'식품/음료','51':'의약품','52':'가전/생활용품','53':'의류/섬유',
        '54':'신발/가방','55':'인쇄/출판','56':'가구/인테리어','60':'문구/사무용품',
        '70':'조경/원예','72':'건설/건축서비스','73':'산업/생산서비스',
        '76':'환경/청소서비스','77':'환경관리서비스','78':'운송/물류서비스',
        '80':'경영/행정서비스','81':'IT/엔지니어링서비스','82':'광고/마케팅서비스',
        '83':'공공서비스','84':'금융/보험서비스','85':'보건/복지서비스',
        '86':'교육/훈련서비스','90':'여행/숙박서비스','91':'경비/소방서비스',
        '92':'국방/치안','93':'정치/시민단체','99':'기타',
    }
    
    # 대분류별 품목 그룹핑
    cat_items = defaultdict(list)
    c2.execute("""SELECT SUBSTR(rprsntDtlPrdnmNo,1,2) seg, rprsntDtlPrdnm, COUNT(*) cnt
        FROM company_master
        WHERE rprsntDtlPrdnmNo IS NOT NULL AND rprsntDtlPrdnmNo != ''
        GROUP BY seg, rprsntDtlPrdnm ORDER BY seg, cnt DESC""")
    for seg, prdnm, cnt in c2.fetchall():
        cat_items[seg].append({"품목명": prdnm, "업체수": cnt})
    
    categories = []
    for seg in sorted(cat_items.keys()):
        items = cat_items[seg]
        categories.append({
            "코드": seg,
            "분류명": UNSPSC.get(seg, f'기타({seg})'),
            "업체수": sum(i["업체수"] for i in items),
            "품목수": len(items),
            "품목": items,
        })
    categories.sort(key=lambda x: x["업체수"], reverse=True)
    local_company_stats["물품_대분류"] = categories
    
    # 공사/용역 업종 (면허업종)
    CNSTWK_KW = ['공사', '건설', '건축', '토목', '포장', '조경', '시공', '소방', '설비',
                  '전기', '통신', '철근', '콘크리트', '금속', '도장', '방수', '상하수도']
    c2.execute("""SELECT indstrytyNm, COUNT(DISTINCT bizno) cnt FROM company_industry
        GROUP BY indstrytyNm ORDER BY cnt DESC""")
    cnstwk_list, servc_list = [], []
    for nm, cnt in c2.fetchall():
        entry = {"업종명": nm, "업체수": cnt}
        if any(k in nm for k in CNSTWK_KW):
            cnstwk_list.append(entry)
        else:
            servc_list.append(entry)
    local_company_stats["공사_업종"] = cnstwk_list
    local_company_stats["용역_업종"] = servc_list
    
    conn_cp2.close()
    cache["10_지역업체현황"] = local_company_stats
    print(f"    물품 대분류 {len(categories)}개 (품목 {sum(c['품목수'] for c in categories)}종), 공사 업종 {len(cnstwk_list)}종, 용역 업종 {len(servc_list)}종")
    
    # ========== 지역경제 기여도 (한국은행 2020 지역산업연관표 부산 산업별 계수) ==========
    print("  [경제효과] 산출 중...")
    # 부산 산업별 계수 (한국은행 2020년 지역산업연관표, 2025년 발행)
    # 조달 분류 → 한국은행 산업 분류 매핑
    SECTOR_COEFFS = {
        '공사': {'매핑산업': '건설업',           'va': 0.472, 'emp': 10.8},
        '용역': {'매핑산업': '사업지원서비스',     'va': 0.542, 'emp': 16.5},
        '물품': {'매핑산업': '제조업 평균',       'va': 0.385, 'emp': 6.4},
        '쇼핑몰': {'매핑산업': '도소매/유통업',    'va': 0.495, 'emp': 13.2},
    }
    # 부산 전산업 평균 (참고용)
    BUSAN_AVG_VA = 0.467    # 지역내 부가가치유발계수 (전산업)
    BUSAN_AVG_EMP = 6.6     # 지역내 취업유발계수 (전산업, 명/10억)
    
    economic_impact = {"계수": {
        "출처": "한국은행 2020년 지역산업연관표 (2025년 발행)",
        "주석": "본 지표는 한국은행 2020년 지역산업연관표(2025년 발행)의 부산 지역 계수를 활용한 추정치",
        "산업별": {s: {"매핑산업": c['매핑산업'], "부가가치유발계수": c['va'],
                      "취업유발계수_명_10억원": c['emp']}
                   for s, c in SECTOR_COEFFS.items()},
        "부산_전산업평균": {"부가가치유발계수": BUSAN_AVG_VA, "취업유발계수": BUSAN_AVG_EMP},
    }}
    
    # 분야별 지역업체 수주액 기반 경제효과 산출 (산업별 차등 계수)
    sector_impact = {}
    total_local_amt = 0
    total_va = 0
    total_emp = 0.0
    for s in sectors:
        local_amt = sum(d['local'] for d in all_data[s].values())
        total_local_amt += local_amt
        coeff = SECTOR_COEFFS[s]
        va = local_amt * coeff['va']                    # 지역생산부가가치
        emp = local_amt / 1e9 * coeff['emp']            # 지역고용기여도 (명)
        total_va += va
        total_emp += emp
        sector_impact[s] = {
            "매핑산업": coeff['매핑산업'],
            "부가가치유발계수": coeff['va'],
            "취업유발계수": coeff['emp'],
            "지역업체수주액": round(local_amt),
            "지역생산부가가치": round(va),
            "지역고용기여도_명": round(emp, 1),
        }
    
    economic_impact["전체"] = {
        "지역업체수주액": round(total_local_amt),
        "지역생산부가가치": round(total_va),
        "지역고용기여도_명": round(total_emp, 1),
    }
    economic_impact["분야별"] = sector_impact
    
    # 그룹별 경제효과 (분야별 가중합)
    grp_impact = {}
    for g in groups:
        g_va = 0; g_emp = 0.0; g_local = 0
        for s in sectors:
            s_local = all_data[s].get(g,{}).get('local',0)
            g_local += s_local
            coeff = SECTOR_COEFFS[s]
            g_va += s_local * coeff['va']
            g_emp += s_local / 1e9 * coeff['emp']
        grp_impact[g] = {
            "지역업체수주액": round(g_local),
            "지역생산부가가치": round(g_va),
            "지역고용기여도_명": round(g_emp, 1),
        }
    economic_impact["그룹별"] = grp_impact
    
    cache["11_경제효과"] = economic_impact
    print(f"    지역업체 수주액: {total_local_amt/1e8:,.0f}억")
    print(f"    지역생산부가가치: {total_va/1e8:,.0f}억 (산업별 차등 계수)")
    print(f"    지역고용기여도: {total_emp:,.0f}명")
    for s in sectors:
        si = sector_impact[s]
        print(f"      {s}({SECTOR_COEFFS[s]['매핑산업']}): {si['지역업체수주액']/1e8:,.0f}억 → 부가가치 {si['지역생산부가가치']/1e8:,.0f}억 / 고용 {si['지역고용기여도_명']:,.0f}명")
    
    # ========== 주간 데이터 집계 (월~일 기준) ==========
    print("  [주간 데이터] 집계 중...")
    from datetime import date, timedelta
    conn = sqlite3.connect(DB_PROCUREMENT)  # 재연결 (line 539에서 close됨)
    
    today = date.today()
    this_monday = today - timedelta(days=today.weekday())
    last_monday = this_monday - timedelta(days=7)
    this_sunday = this_monday + timedelta(days=6)
    last_sunday = last_monday + timedelta(days=6)
    
    def calc_weekly(start_dt, end_dt):
        start_s = start_dt.strftime('%Y-%m-%d')
        end_s = end_dt.strftime('%Y-%m-%d')
        wk = defaultdict(lambda: {'total': 0, 'local': 0})
        contracts = []  # 개별 계약 추적 (수주율 변동 원인 분석용)
        for tbl, nm, award_key in [('cnstwk_cntrct','공사','공사'),
                                    ('servc_cntrct','용역','용역'),
                                    ('thng_cntrct','물품','물품')]:
            extra_col = ', cnstrtsiteRgnNm' if tbl == 'servc_cntrct' else ''
            try:
                wdf = pd.read_sql(f"""SELECT untyCntrctNo, dcsnCntrctNo, cntrctInsttCd,
                    totCntrctAmt, thtmCntrctAmt, corpList, ntceNo, dminsttList,
                    {'cnstwkNm' if tbl=='cnstwk_cntrct' else 'cntrctNm'} as cntrctNm,
                    cntrctInsttOfclTelNo{extra_col}
                    FROM [{tbl}]
                    WHERE cntrctCnclsDate >= '{start_s}' AND cntrctCnclsDate <= '{end_s}'""", conn)
                wdf.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
                wdf = dedup_by_dcsn(wdf)
                for _, row in wdf.iterrows():
                    result = process_contract_row(row, inst_dict, biznos,
                                                   use_location_filter=True,
                                                   bid_dict=bid_dict,
                                                   award_set=award_sets.get(award_key, set()))
                    if not result: continue
                    cd, amt, loc = result
                    lrg = inst_grp.get(cd)
                    if not lrg: continue
                    wk[lrg]['total'] += amt; wk[lrg]['local'] += loc
                    wk['전체']['total'] += amt; wk['전체']['local'] += loc
                    # 분야별 + 그룹×분야별 추적
                    wk[nm]['total'] += amt; wk[nm]['local'] += loc
                    wk[f"{lrg}_{nm}"]['total'] += amt; wk[f"{lrg}_{nm}"]['local'] += loc
                    if amt >= 1e8:  # 1억 이상만 추적
                        _corp = ''; _rgn = ''
                        for _ck in str(row.get('corpList','') or '').split('[')[1:]:
                            _ps = _ck.split(']')[0].split('^')
                            if len(_ps) >= 10:
                                _bno = str(_ps[9]).replace('-','').strip()
                                _corp = _ps[3].strip() if len(_ps) >= 4 else ''
                                _rgn = bizno_region.get(_bno, '')
                                break
                        contracts.append({"분야": nm, "기관": get_unit(cd) or '', "계약명": str(row.get('cntrctNm','') or '')[:50], "계약액": round(amt), "수주액": round(loc), "유출액": round(amt - loc), "수주업체": _corp[:30], "지역": _rgn})
            except:
                pass
        try:
            sdf = pd.read_sql(f"""SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, dminsttCd,
                prdctAmt, cntrctCorpBizno, corpNm, prdctClsfcNoNm,
                cnstwkMtrlDrctPurchsObjYn, dlvrReqNm
                FROM shopping_cntrct
                WHERE dlvrReqRcptDate >= '{start_s}' AND dlvrReqRcptDate <= '{end_s}'""", conn)
            sdf['dlvrReqChgOrd'] = pd.to_numeric(sdf['dlvrReqChgOrd'], errors='coerce').fillna(0)
            sdf.sort_values('dlvrReqChgOrd', ascending=False, inplace=True)
            sdf.drop_duplicates(subset=['dlvrReqNo','prdctSno'], keep='first', inplace=True)
            for _, row in sdf.iterrows():
                result = process_contract_row(row, inst_dict, biznos, is_shopping=True)
                if not result: continue
                cd, amt, loc = result
                lrg = inst_grp.get(cd)
                if not lrg: continue
                wk[lrg]['total'] += amt; wk[lrg]['local'] += loc
                wk['전체']['total'] += amt; wk['전체']['local'] += loc
                # 분야별 + 그룹×분야별 추적
                wk['쇼핑몰']['total'] += amt; wk['쇼핑몰']['local'] += loc
                wk[f"{lrg}_쇼핑몰"]['total'] += amt; wk[f"{lrg}_쇼핑몰"]['local'] += loc
                if amt >= 1e8:
                    _bno_s = str(row.get('cntrctCorpBizno','')).replace('-','').strip()
                    _corp_s = str(row.get('corpNm','') or '').strip()
                    _rgn_s = bizno_region.get(_bno_s, '')
                    contracts.append({"분야": "쇼핑몰", "기관": get_unit(cd) or '', "계약명": str(row.get('dlvrReqNm','') or '')[:50], "계약액": round(amt), "수주액": round(loc), "유출액": round(amt - loc), "수주업체": _corp_s[:30], "지역": _rgn_s})
        except:
            pass
        return dict(wk), contracts
    
    this_week_data, this_week_contracts = calc_weekly(this_monday, this_sunday)
    last_week_data, last_week_contracts = calc_weekly(last_monday, last_sunday)
    
    # ── 7주간 주별 수주율 히스토리 ──
    print(f"  [주간 히스토리] 7주간 수주율 계산 중...")
    weekly_history = []
    # 주별 데이터를 먼저 수집 (오래된 순)
    weekly_raw = []
    for wk_offset in range(6, -1, -1):  # 6주전 → 이번주
        wk_mon = this_monday - timedelta(days=7 * wk_offset)
        wk_sun = wk_mon + timedelta(days=6)
        if wk_offset == 0:
            wk_data = this_week_data
        elif wk_offset == 1:
            wk_data = last_week_data
        else:
            wk_data, _ = calc_weekly(wk_mon, wk_sun)
        weekly_raw.append((wk_mon, wk_sun, wk_data))
    
    # 누계 수주율 역산: 현재 누계에서 최근주부터 빼면서 각 주차 시점의 누계를 구함
    cum_totals = {}
    for dim_key in ['전체', '공사', '용역', '물품', '쇼핑몰']:
        if dim_key == '전체':
            cum_totals[dim_key] = {'total': cache["1_전체"]["발주액"], 'local': cache["1_전체"]["수주액"]}
        else:
            sd = cache.get("2_분야별", {}).get(dim_key, {})
            cum_totals[dim_key] = {'total': sd.get("발주액", 0), 'local': sd.get("수주액", 0)}
    
    # 각 주차 끝 시점의 누계를 역산 (이번주 → 6주전, 역순으로)
    cum_snapshots = [None] * 7
    for i in range(6, -1, -1):  # 이번주(6)부터 거꾸로
        wk_mon, wk_sun, wk_data = weekly_raw[i]
        snap = {}
        for dim_key in ['전체', '공사', '용역', '물품', '쇼핑몰']:
            ct = cum_totals[dim_key]
            snap[f"{dim_key}_cum_rate"] = round(ct['local'] / ct['total'] * 100, 1) if ct['total'] > 0 else 0
        cum_snapshots[i] = snap
        # 이 주의 계약을 빼서 이전 주 누계로
        if i > 0:
            for dim_key in ['전체', '공사', '용역', '물품', '쇼핑몰']:
                wd = wk_data.get(dim_key, {'total': 0, 'local': 0})
                cum_totals[dim_key]['total'] -= wd['total']
                cum_totals[dim_key]['local'] -= wd['local']
    
    for i, (wk_mon, wk_sun, wk_data) in enumerate(weekly_raw):
        wk_entry = {"기간": f"{wk_mon.strftime('%m/%d')}~{wk_sun.strftime('%m/%d')}"}
        for dim_key in ['전체', '공사', '용역', '물품', '쇼핑몰']:
            wd = wk_data.get(dim_key, {'total': 0, 'local': 0})
            wk_entry[f"{dim_key}_rate"] = round(wd['local'] / wd['total'] * 100, 1) if wd['total'] > 0 else 0
            wk_entry[f"{dim_key}_total"] = round(wd['total'])
        # 누계 수주율 추가
        if cum_snapshots[i]:
            wk_entry.update(cum_snapshots[i])
        weekly_history.append(wk_entry)
        print(f"    {wk_entry['기간']}: 주간 {wk_entry['전체_rate']}% / 누계 {wk_entry.get('전체_cum_rate',0)}%")
    
    weekly_cache = {
        "이번주_기간": f"{this_monday.strftime('%m/%d')}~{this_sunday.strftime('%m/%d')}",
        "지난주_기간": f"{last_monday.strftime('%m/%d')}~{last_sunday.strftime('%m/%d')}",
        "주간이력": weekly_history,
    }
    for grp_key in set(list(this_week_data.keys()) + list(last_week_data.keys())):
        tw = this_week_data.get(grp_key, {'total': 0, 'local': 0})
        lw = last_week_data.get(grp_key, {'total': 0, 'local': 0})
        tw_rate = round(tw['local'] / tw['total'] * 100, 1) if tw['total'] > 0 else 0
        lw_rate = round(lw['local'] / lw['total'] * 100, 1) if lw['total'] > 0 else 0
        weekly_cache[grp_key] = {
            "이번주_계약액": round(tw['total']),
            "이번주_수주액": round(tw['local']),
            "이번주_수주율": tw_rate,
            "지난주_계약액": round(lw['total']),
            "지난주_수주액": round(lw['local']),
            "지난주_수주율": lw_rate,
            "수주율_증감": round(tw_rate - lw_rate, 1),
        }
    
    # 수주율 변동 원인 분석 (이번주/지난주 Top 10)
    leak_top = sorted(this_week_contracts, key=lambda x: x['유출액'], reverse=True)[:10]
    local_top = sorted(this_week_contracts, key=lambda x: x['수주액'], reverse=True)[:10]
    leak_top_lw = sorted(last_week_contracts, key=lambda x: x['유출액'], reverse=True)[:10]
    local_top_lw = sorted(last_week_contracts, key=lambda x: x['수주액'], reverse=True)[:10]
    weekly_cache["이번주_주요유출"] = leak_top
    weekly_cache["이번주_주요수주"] = local_top
    weekly_cache["지난주_주요유출"] = leak_top_lw
    weekly_cache["지난주_주요수주"] = local_top_lw
    cache["13_주간데이터"] = weekly_cache
    tw_all = weekly_cache.get("전체", {})
    print(f"    이번주({weekly_cache['이번주_기간']}): 계약액 {tw_all.get('이번주_계약액',0)/1e8:,.0f}억, 수주율 {tw_all.get('이번주_수주율',0)}%")
    print(f"    지난주({weekly_cache['지난주_기간']}): 계약액 {tw_all.get('지난주_계약액',0)/1e8:,.0f}억, 수주율 {tw_all.get('지난주_수주율',0)}%")
    print(f"    수주율 증감: {tw_all.get('수주율_증감',0):+.1f}%p")
    # ── 일별 누계 수주율의 7일 평균 vs 현재 수주율 비교 ──
    # 전체 + 분야별 + 그룹별 running totals 초기화
    # 그룹×분야 조합 추가
    grp_sec_dims = [f"{g}_{s}" for g in groups for s in sectors]
    dims = ['전체', '공사', '용역', '물품', '쇼핑몰', '부산광역시 및 소속기관', '정부 및 국가공공기관'] + grp_sec_dims
    running = {d: {'total': 0, 'local': 0} for d in dims}
    
    # 현재 누계 값 세팅
    running['전체']['total'] = cache["1_전체"]["발주액"]
    running['전체']['local'] = cache["1_전체"]["수주액"]
    for sector_name in ['공사', '용역', '물품', '쇼핑몰']:
        sd = cache.get("2_분야별", {}).get(sector_name, {})
        running[sector_name]['total'] = sd.get('발주액', 0)
        running[sector_name]['local'] = sd.get('수주액', 0)
    for grp_name in ['부산광역시 및 소속기관', '정부 및 국가공공기관']:
        gd = cache.get("3_그룹별", {}).get(grp_name, {})
        running[grp_name]['total'] = gd.get('발주액', 0)
        running[grp_name]['local'] = gd.get('수주액', 0)
        for sector_name in sectors:
            gs_key = f"{grp_name}_{sector_name}"
            gsd = cache.get("4_그룹별_분야별", {}).get(grp_name, {}).get(sector_name, {})
            running[gs_key]['total'] = gsd.get('발주액', 0)
            running[gs_key]['local'] = gsd.get('수주액', 0)
    
    daily_dim_rates = {d: [] for d in dims}  # 7일간의 일별 누계 수주율
    
    for d_offset in range(7):
        target_date = today - timedelta(days=d_offset)
        ds = target_date.strftime('%Y-%m-%d')
        day_by_dim = {d: {'total': 0, 'local': 0} for d in dims}
        
        for tbl, sector_name, award_key in [('cnstwk_cntrct','공사','공사'),('servc_cntrct','용역','용역'),('thng_cntrct','물품','물품')]:
            try:
                ddf = pd.read_sql(f"""SELECT untyCntrctNo, dcsnCntrctNo, cntrctInsttCd,
                    totCntrctAmt, thtmCntrctAmt, corpList, ntceNo, dminsttList,
                    {'cnstwkNm' if tbl=='cnstwk_cntrct' else 'cntrctNm'} as cntrctNm,
                    cntrctInsttOfclTelNo
                    FROM [{tbl}]
                    WHERE cntrctCnclsDate = '{ds}'""", conn)
                ddf.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
                ddf = dedup_by_dcsn(ddf)
                for _, row in ddf.iterrows():
                    result = process_contract_row(row, inst_dict, biznos,
                                                   use_location_filter=True,
                                                   bid_dict=bid_dict,
                                                   award_set=award_sets.get(award_key, set()))
                    if not result: continue
                    cd, amt, loc = result
                    lrg = inst_grp.get(cd)
                    if not lrg: continue
                    day_by_dim['전체']['total'] += amt; day_by_dim['전체']['local'] += loc
                    day_by_dim[sector_name]['total'] += amt; day_by_dim[sector_name]['local'] += loc
                    if lrg in day_by_dim:
                        day_by_dim[lrg]['total'] += amt; day_by_dim[lrg]['local'] += loc
                    gs_key = f"{lrg}_{sector_name}"
                    if gs_key in day_by_dim:
                        day_by_dim[gs_key]['total'] += amt; day_by_dim[gs_key]['local'] += loc
            except: pass
        # 쇼핑몰
        try:
            sdf2 = pd.read_sql(f"""SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, dminsttCd,
                prdctAmt, cntrctCorpBizno, prdctClsfcNoNm,
                cnstwkMtrlDrctPurchsObjYn, dlvrReqNm
                FROM shopping_cntrct WHERE dlvrReqRcptDate = '{ds}'""", conn)
            sdf2['dlvrReqChgOrd'] = pd.to_numeric(sdf2['dlvrReqChgOrd'], errors='coerce').fillna(0)
            sdf2.sort_values('dlvrReqChgOrd', ascending=False, inplace=True)
            sdf2.drop_duplicates(subset=['dlvrReqNo','prdctSno'], keep='first', inplace=True)
            for _, row in sdf2.iterrows():
                result = process_contract_row(row, inst_dict, biznos, is_shopping=True)
                if not result: continue
                cd, amt, loc = result
                lrg = inst_grp.get(cd)
                if not lrg: continue
                day_by_dim['전체']['total'] += amt; day_by_dim['전체']['local'] += loc
                day_by_dim['쇼핑몰']['total'] += amt; day_by_dim['쇼핑몰']['local'] += loc
                if lrg in day_by_dim:
                    day_by_dim[lrg]['total'] += amt; day_by_dim[lrg]['local'] += loc
                gs_key = f"{lrg}_쇼핑몰"
                if gs_key in day_by_dim:
                    day_by_dim[gs_key]['total'] += amt; day_by_dim[gs_key]['local'] += loc
        except: pass
        
        if d_offset > 0:  # 오늘 제외
            for d in dims:
                r = running[d]
                rate = round(r['local'] / r['total'] * 100, 1) if r['total'] > 0 else 0
                daily_dim_rates[d].append(rate)
        
        # 해당 일의 실적을 누계에서 차감 (역순)
        for d in dims:
            running[d]['total'] -= day_by_dim[d]['total']
            running[d]['local'] -= day_by_dim[d]['local']
    
    cum_compare = {}
    for d in dims:
        rates = daily_dim_rates[d]
        if d == '전체':
            cur = cache["1_전체"]["수주율"]
        elif d in ['공사','용역','물품','쇼핑몰']:
            cur = cache.get("2_분야별", {}).get(d, {}).get('수주율', 0)
        elif '_' in d:
            # 그룹×분야 (e.g. "부산광역시 및 소속기관_공사")
            parts = d.rsplit('_', 1)
            cur = cache.get("4_그룹별_분야별", {}).get(parts[0], {}).get(parts[1], {}).get('수주율', 0)
        else:
            cur = cache.get("3_그룹별", {}).get(d, {}).get('수주율', 0)
        avg7 = round(sum(rates) / len(rates), 1) if rates else cur
        cum_compare[d] = {
            "현재_수주율": cur,
            "7일평균_수주율": avg7,
            "증감": round(cur - avg7, 1),
        }
    
    weekly_cache["누계비교"] = cum_compare
    print(f"    [누계비교] 전체: {cum_compare['전체']['현재_수주율']}% vs 7일평균 {cum_compare['전체']['7일평균_수주율']}% = {cum_compare['전체']['증감']:+.1f}%p")
    for d in ['공사','용역','물품','쇼핑몰','부산광역시 및 소속기관','정부 및 국가공공기관']:
        c = cum_compare[d]
        print(f"      {d}: {c['현재_수주율']}% vs {c['7일평균_수주율']}% = {c['증감']:+.1f}%p")
    
    import tempfile, os, math
    # NaN/Inf 치환: Pandas 계산 결과에 NaN이 섞이면 FastAPI가 500 에러 발생
    def sanitize_nan(obj):
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return 0
        elif isinstance(obj, dict):
            return {k: sanitize_nan(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [sanitize_nan(v) for v in obj]
        return obj
    cache = sanitize_nan(cache)
    
    # Atomic write: 임시파일에 쓴 후 rename → API 서버가 불완전 캐시를 읽는 것 방지
    tmp_fd, tmp_path = tempfile.mkstemp(suffix='.json', dir=os.path.dirname(CACHE_FILE) or '.')
    try:
        with os.fdopen(tmp_fd, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, CACHE_FILE)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise
    
    elapsed = time.time() - start
    print(f"\n[캐시 생성 완료] {CACHE_FILE} ({elapsed:.1f}초)")
    print(f"  전체 수주율: {cache['1_전체']['수주율']}%")
    for g in groups:
        r = cache['5_기관랭킹_전체'][g]
        print(f"\n  [{g}] ({r['최소기준액']} 이상 {r['해당기관수']}개)")
        for a in r['상위'][:3]:
            print(f"    ▲ {a['비교단위']:25s} {a['발주액']/1e8:.1f}억 수주율 {a['수주율']}%")
        for a in r['하위'][:3]:
            print(f"    ▼ {a['비교단위']:25s} {a['발주액']/1e8:.1f}억 수주율 {a['수주율']}%")

if __name__ == '__main__':
    build_cache()
