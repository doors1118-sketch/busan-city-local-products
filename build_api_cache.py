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
    load_bid_dict, load_award_sets,
)

sys.stdout.reconfigure(encoding='utf-8')

DB_PROCUREMENT = 'procurement_contracts.db'
DB_AGENCIES = 'busan_agencies_master.db'
DB_COMPANIES = 'busan_companies_master.db'
CACHE_FILE = 'api_cache.json'

MIN_AMT = {
    '공사': 30e8,
    '용역': 30e8,
    '물품': 10e8,
    '쇼핑몰': 10e8,
    None: 50e8,
}
TOP_N = 10

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
    biznos = set(pd.read_sql("SELECT bizno FROM company_master", conn_cp)['bizno']
                .dropna().astype(str).str.replace('-','',regex=False).str.strip())
    # 대표세부품명별 부산 업체 수 (유출품목 매칭용)
    supplier_map = pd.read_sql("""
        SELECT rprsntDtlPrdnm, COUNT(*) as cnt FROM company_master
        WHERE rprsntDtlPrdnm IS NOT NULL AND rprsntDtlPrdnm != ''
        GROUP BY rprsntDtlPrdnm
    """, conn_cp).set_index('rprsntDtlPrdnm')['cnt'].to_dict()
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
        # 상위분류 → 세부분류 매핑으로 합산
        children = cat_children.get(item_nm, set())
        if children:
            return sum(supplier_map.get(child, 0) for child in children)
        return 0
    
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
    

    
    # --- 쇼핑몰 (공사자재 현장 필터) + 유출품목 집계 ---
    print("  [쇼핑몰] 계산 중...")
    df = pd.read_sql("""SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, dminsttCd,
        prdctAmt, cntrctCorpBizno, prdctClsfcNoNm,
        cnstwkMtrlDrctPurchsObjYn, dlvrReqNm FROM shopping_cntrct""", conn)
    df['dlvrReqChgOrd'] = pd.to_numeric(df['dlvrReqChgOrd'], errors='coerce').fillna(0)
    df.sort_values('dlvrReqChgOrd', ascending=False, inplace=True)
    df.drop_duplicates(subset=['dlvrReqNo','prdctSno'], keep='first', inplace=True)
    
    # 공사자재 현장 필터 적용
    df, n_site_drop, amt_site_drop = filter_shopping_by_site(
        df, conn, set(inst_dict.keys()))
    print(f"    공사자재 현장배제: {n_site_drop}건 ({amt_site_drop/1e8:.1f}억)")
    
    grp_r = {}
    ag_r = defaultdict(lambda:{'total':0,'local':0})
    # 유출품목 집계용
    item_total = defaultdict(float)
    item_leak = defaultdict(float)
    item_count = defaultdict(int)
    item_top_agency = defaultdict(lambda: defaultdict(float))  # 품목→{기관:금액}
    
    for _, row in df.iterrows():
        result = process_contract_row(row, inst_dict, biznos, is_shopping=True)
        if not result: continue
        cd, amt, loc = result
        lrg = inst_grp.get(cd)
        unit = get_unit(cd)
        if not lrg or not unit: continue
        if lrg not in grp_r: grp_r[lrg] = {'total':0,'local':0}
        grp_r[lrg]['total'] += amt; grp_r[lrg]['local'] += loc
        ag_r[unit]['total'] += amt; ag_r[unit]['local'] += loc
        
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
        })
    print(f"    완료 ({len(df):,}건, 유출품목 {len(leakage_shopping)}개)")
    
    # --- 공사/용역/물품 유출계약 Top 10 (core_calc 필터 적용 후) ---
    print("  [유출계약] 집계 중...")
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
                if bno not in biznos and len(parts) >= 2:
                    corp_nm = parts[1]; break
        leak_contracts.append({
            "분야": "공사", "수요기관": unit or '', "계약명": str(row.get('cnstwkNm',''))[:60],
            "계약액": round(amt), "유출액": round(nloc),
            "유출율": round(nloc/amt*100, 1), "수주업체": corp_nm[:25],
            "그룹": grp,
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
                    if bno not in biznos and len(parts) >= 2:
                        corp_nm = parts[1]; break
            leak_contracts.append({
                "분야": sector, "수요기관": unit or '', "계약명": str(row.get('cntrctNm',''))[:60],
                "계약액": round(amt), "유출액": round(nloc),
                "유출율": round(nloc/amt*100, 1), "수주업체": corp_nm[:25],
                "그룹": grp,
            })
    conn2.close()
    
    leak_contracts.sort(key=lambda x: x['유출액'], reverse=True)
    leakage_contracts = leak_contracts[:50]
    print(f"    유출계약(공사/용역/물품) 후보 {len(leak_contracts):,}건 중 Top 50 선정")
    
    # ========== 보호제도 미적용 분석 (국가/부산시 분리) ==========
    print("  [보호제도] 분석 중...")
    SPECIALTY_TYPES = ['전기공사', '정보통신공사', '소방시설공사', '기계설비공사',
                       '전기', '통신', '소방', '기계설비', '기계공사', '정보통신']
    PROT_THRESHOLDS = {
        '부산광역시 및 소속기관': {'종합공사': 100e8, '전문공사': 10e8, '용역': 3.3e8},
        '정부 및 국가공공기관':  {'종합공사': 88e8,  '전문공사': 10e8, '용역': 2.2e8},
    }
    gov_stats = defaultdict(lambda: {'기준이하': 0, '지역제한': 0, '의무공동': 0, '미적용': 0, '미적용액': 0})
    bsn_stats = defaultdict(lambda: {'기준이하': 0, '지역제한': 0, '미적용': 0, '미적용액': 0})
    bsn_jnt = defaultdict(lambda: {'모수': 0, '의무공동': 0})
    prot_by_agency = defaultdict(lambda: {'total': 0, 'applied': 0, 'unapplied': 0, 'unapplied_amt': 0, 'grp': ''})
    prot_violations = []

    price_rows = pd.read_sql("""SELECT bidNtceNo, bidNtceNm, presmptPrce, sector, dminsttCd,
        dminsttNm, cntrctCnclsMthdNm, cnstrtsiteRgnNm, mainCnsttyNm,
        prtcptLmtRgnNm, rgnDutyJntcontrctYn
        FROM bid_notices_price
        WHERE presmptPrce IS NOT NULL AND presmptPrce != '' AND presmptPrce != '0'
        AND sector IN ('공사', '용역')
        AND cntrctCnclsMthdNm IN ('일반경쟁', '제한경쟁')""", conn)

    for _, pr in price_rows.iterrows():
        price = float(pr['presmptPrce'])
        if price <= 0: continue
        grp = inst_grp.get(pr['dminsttCd'])
        if not grp or grp not in PROT_THRESHOLDS: continue
        sector = pr['sector']
        unit = get_unit(pr['dminsttCd'])
        if not unit: continue
        method = str(pr['cntrctCnclsMthdNm'] or '')
        if sector == '공사':
            site = str(pr.get('cnstrtsiteRgnNm') or '').strip()
            if not site or '부산' not in site: continue
            main_type = str(pr.get('mainCnsttyNm') or '').strip()
            if main_type and any(k in main_type for k in SPECIALTY_TYPES): sub = '전문공사'
            elif any(k in str(pr['bidNtceNm'] or '') for k in SPECIALTY_TYPES): sub = '전문공사'
            else: sub = '종합공사'
        elif sector == '용역':
            # 용역: 키워드 필터로 타지역 배제 (국가기관만 - 부산시는 통과)
            lrg = inst_dict.get(pr['dminsttCd'], {}).get('cate_lrg', '')
            fake_row = {'cntrctNm': str(pr['bidNtceNm'] or ''), 'cntrctInsttOfclTelNo': ''}
            if is_non_busan_contract(fake_row, lrg): continue
            sub = '용역'
        else: continue

        ntce_clean = str(pr['bidNtceNo']).replace('-','').strip()
        rgn_code = str(pr.get('prtcptLmtRgnNm') or '').strip()
        is_busan_rgn = rgn_code.startswith('26') if rgn_code else False
        is_rgn = (method == '제한경쟁') and (ntce_clean in award_all or is_busan_rgn)
        jnt_yn = str(pr.get('rgnDutyJntcontrctYn') or '').strip().upper()
        threshold = PROT_THRESHOLDS[grp].get(sub)

        if grp == '정부 및 국가공공기관':
            if not threshold or price > threshold: continue
            gov_stats[sub]['기준이하'] += 1
            prot_by_agency[unit]['total'] += 1
            prot_by_agency[unit]['grp'] = grp
            if is_rgn:
                gov_stats[sub]['지역제한'] += 1
                prot_by_agency[unit]['applied'] += 1
            elif jnt_yn == 'Y':
                gov_stats[sub]['의무공동'] += 1
                prot_by_agency[unit]['applied'] += 1
            else:
                gov_stats[sub]['미적용'] += 1
                gov_stats[sub]['미적용액'] += price
                prot_by_agency[unit]['unapplied'] += 1
                prot_by_agency[unit]['unapplied_amt'] += price
                prot_violations.append({"분야": sub, "계약방식": method,
                    "공고명": str(pr['bidNtceNm'] or '')[:55], "추정가격": round(price),
                    "기관그룹": grp, "수요기관": str(pr['dminsttNm'] or '')[:25], "비교단위": unit})

        elif grp == '부산광역시 및 소속기관':
            # 1단계: 기준이하 지역제한
            if threshold and price <= threshold:
                bsn_stats[sub]['기준이하'] += 1
                prot_by_agency[unit]['total'] += 1
                prot_by_agency[unit]['grp'] = grp
                if is_rgn:
                    bsn_stats[sub]['지역제한'] += 1
                    prot_by_agency[unit]['applied'] += 1
                else:
                    bsn_stats[sub]['미적용'] += 1
                    bsn_stats[sub]['미적용액'] += price
                    prot_by_agency[unit]['unapplied'] += 1
                    prot_by_agency[unit]['unapplied_amt'] += price
                    prot_violations.append({"분야": sub, "계약방식": method,
                        "공고명": str(pr['bidNtceNm'] or '')[:55], "추정가격": round(price),
                        "기관그룹": grp, "수요기관": str(pr['dminsttNm'] or '')[:25], "비교단위": unit})
            # 2단계: 전체 건(금액 무관) 중 지역제한·수의 제외 → 의무공동
            if not is_rgn:
                sec_key = '공사' if sub in ('종합공사', '전문공사') else '용역'
                bsn_jnt[sec_key]['모수'] += 1
                if jnt_yn == 'Y': bsn_jnt[sec_key]['의무공동'] += 1

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
            key = f"{grp}_{sector}"
            suui_stats[key]['total'] += 1
            prot_by_agency[unit]['total'] += 1
            prot_by_agency[unit]['grp'] = grp
            amt = float(row.get('thtmCntrctAmt', 0) or 0)
            if amt == 0: amt = float(row.get('totCntrctAmt', 0) or 0)
            if sum(1 for bno, _ in biz_list if bno in biznos) > 0:
                suui_stats[key]['busan'] += 1
                suui_stats[key]['busan_amt'] = suui_stats[key].get('busan_amt', 0) + amt
                prot_by_agency[unit]['applied'] += 1
            else:
                suui_stats[key]['non_busan'] += 1
                suui_stats[key]['non_busan_amt'] += amt
                prot_by_agency[unit]['unapplied'] += 1
                prot_by_agency[unit]['unapplied_amt'] += amt

    prot_violations.sort(key=lambda x: x['추정가격'], reverse=True)

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
            result[g] = {
                "최소기준액": f"{min_amt/1e8:.0f}억원",
                "해당기관수": len(scored),
                "상위": list(reversed(scored[-TOP_N:])),
                "하위": scored[:TOP_N]
            }
        return result
    
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
        "8_보호제도_미적용": prot_violations[:10],
        "8_보호제도_기관별": prot_agency_ranking[:20],
        "9_수의계약": {
            key: {**dict(suui_stats[key]),
                  "busan_amt": round(suui_stats[key].get('busan_amt', 0)),
                  "수주율_건수": round(suui_stats[key]['busan']/suui_stats[key]['total']*100, 1) if suui_stats[key]['total'] > 0 else 0}
            for key in suui_stats},
    }

    # 기관별 상세 검색용 데이터 (12_기관별_상세)
    agency_details = defaultdict(lambda: {
        "총발주액": 0, "총수주액": 0, "총수주율": 0, "그룹": "",
        "유출계약": []
    })
    
    for s in sectors:
        for unit, d in unit_data[s].items():
            agency_details[unit]["총발주액"] += d["total"]
            agency_details[unit]["총수주액"] += d["local"]
            grp = unit_to_grp.get(unit, "")
            if grp: agency_details[unit]["그룹"] = grp
            
    for unit, details in agency_details.items():
        details["총수주율"] = pct(details["총발주액"], details["총수주액"])
        details["총발주액"] = round(details["총발주액"])
        details["총수주액"] = round(details["총수주액"])
        
    for lc in leak_contracts:
        u = lc["수요기관"]
        if u and u in agency_details:
            agency_details[u]["유출계약"].append(lc)

    cache["12_기관별_상세"] = dict(agency_details)
    
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
    
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    
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
