"""
수주율 산출 리포트: 분야별/그룹별/소분류별 상세 트리
================================================================
core_calc.py의 공통 로직을 사용하여 일관된 계산 보장.
"""
import pandas as pd
import sqlite3
import sys
import time

from core_calc import (
    parse_corp_shares, extract_dminstt_codes, dedup_by_dcsn,
    is_non_busan_contract, check_busan_restriction,
    filter_cnstwk_by_site, filter_servc_by_site, filter_shopping_by_site, process_contract_row,
    load_bid_dict, load_award_sets,
)

sys.stdout.reconfigure(encoding='utf-8')

DB_AGENCIES = 'busan_agencies_master.db'
DB_COMPANIES = 'busan_companies_master.db'
DB_PROCUREMENT = 'procurement_contracts.db'

def get_stats_structure():
    return {'total_amt': 0.0, 'local_amt': 0.0, 'sub': {}}

def print_tree(stats_dict, sector_name):
    tot = sum(d['total_amt'] for d in stats_dict.values())
    loc = sum(d['local_amt'] for d in stats_dict.values())
    rate = (loc / tot * 100) if tot > 0 else 0
    
    print(f"\n 📊 < {sector_name} 통합 실적 상세 > (발주액: {tot:,.0f}원 | 지역수주율: {rate:.2f}%)")
    print("-" * 70)
    
    for lrg, data_lrg in stats_dict.items():
        if data_lrg['total_amt'] == 0: continue
        l_rate = (data_lrg['local_amt'] / data_lrg['total_amt'] * 100) if data_lrg['total_amt'] > 0 else 0
        print(f" 🔽 [{lrg}] 그룹 (발주액: {data_lrg['total_amt']:,.0f}원 | 수주율: {l_rate:.2f}%)")
        
        sorted_sub = sorted(data_lrg['sub'].items(), key=lambda x: x[1]['total_amt'], reverse=True)
        for i, (mid, data_mid) in enumerate(sorted_sub):
            if data_mid['total_amt'] == 0: continue
            m_rate = (data_mid['local_amt'] / data_mid['total_amt'] * 100) if data_mid['total_amt'] > 0 else 0
            connector_mid = "└─" if i == len(sorted_sub) - 1 else "├─"
            print(f"     {connector_mid} {mid}: 발주액 {data_mid['total_amt']:,.0f}원 (수주율 {m_rate:.2f}%)")
            
            sorted_sml = sorted(data_mid['sub'].items(), key=lambda x: x[1]['total_amt'], reverse=True)
            for j, (sml, data_sml) in enumerate(sorted_sml):
                if data_sml['total_amt'] == 0: continue
                if len(sorted_sml) == 1 and sml == '미분류': continue
                s_rate = (data_sml['local_amt'] / data_sml['total_amt'] * 100) if data_sml['total_amt'] > 0 else 0
                spacer = "    " if i == len(sorted_sub) - 1 else "│   "
                connector_sml = "└─" if j == len(sorted_sml) - 1 else "├─"
                print(f"     {spacer} {connector_sml} {sml}: 발주액 {data_sml['total_amt']:,.0f}원 (수주율 {s_rate:.2f}%)")
        print()
    return tot, loc


def process_dataframe(df, busan_inst_dict, busan_comp_biznos,
                      is_shopping=False, use_location_filter=False,
                      bid_dict=None, award_ntce_set=None):
    """core_calc의 process_contract_row를 사용하여 그룹별 통계 산출"""
    stats = {}
    
    for _, row in df.iterrows():
        result = process_contract_row(
            row, busan_inst_dict, busan_comp_biznos,
            is_shopping=is_shopping,
            use_location_filter=use_location_filter,
            bid_dict=bid_dict,
            award_set=award_ntce_set,
        )
        if result is None:
            continue
        
        matched_cd, amt, loc_amt = result
        agency = busan_inst_dict[matched_cd]
        lrg, mid, sml = agency['cate_lrg'], agency['cate_mid'], agency['cate_sml']
        
        if lrg not in stats: stats[lrg] = get_stats_structure()
        if mid not in stats[lrg]['sub']: stats[lrg]['sub'][mid] = get_stats_structure()
        if sml not in stats[lrg]['sub'][mid]['sub']: stats[lrg]['sub'][mid]['sub'][sml] = get_stats_structure()
        
        stats[lrg]['total_amt'] += amt
        stats[lrg]['sub'][mid]['total_amt'] += amt
        stats[lrg]['sub'][mid]['sub'][sml]['total_amt'] += amt
        
        stats[lrg]['local_amt'] += loc_amt
        stats[lrg]['sub'][mid]['local_amt'] += loc_amt
        stats[lrg]['sub'][mid]['sub'][sml]['local_amt'] += loc_amt
        
    return stats


def main():
    start = time.time()
    print("1. 마스터 DB (기관/업체) 로딩 중...")
    
    conn_ag = sqlite3.connect(DB_AGENCIES)
    df_ag = pd.read_sql("SELECT dminsttCd, cate_lrg, cate_mid, cate_sml FROM agency_master", conn_ag)
    conn_ag.close()
    df_ag['dminsttCd'] = df_ag['dminsttCd'].astype(str).str.strip()
    df_ag['cate_lrg'] = df_ag['cate_lrg'].fillna('미분류').replace('', '미분류')
    df_ag['cate_mid'] = df_ag['cate_mid'].fillna('미분류').replace('', '미분류')
    df_ag['cate_sml'] = df_ag['cate_sml'].fillna('미분류').replace('', '미분류')
    busan_inst_dict = df_ag.set_index('dminsttCd').to_dict('index')
    
    conn_cp = sqlite3.connect(DB_COMPANIES)
    conn_pr = sqlite3.connect(DB_PROCUREMENT)
    from core_calc import load_expanded_biznos
    busan_comp_biznos = load_expanded_biznos(conn_cp, conn_pr)
    conn_pr.close()
    conn_cp.close()
    
    print("\n2. 통합 계약 DB 조회 및 연산 중...")
    conn_pr = sqlite3.connect(DB_PROCUREMENT)
    
    # 필터 데이터 로딩 (core_calc 사용)
    print("  입찰공고/낙찰정보 로딩 중...")
    bid_dict, bid_df = load_bid_dict(conn_pr)
    award_sets = load_award_sets(conn_pr)
    print(f"    용역: {len(award_sets['용역']):,} / 공사: {len(award_sets['공사']):,} / 물품: {len(award_sets['물품']):,} 공고번호")
    
    # --- [A. 공사] ---
    df_const = pd.read_sql("""SELECT untyCntrctNo, dcsnCntrctNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt,
        corpList, ntceNo, dminsttList, cnstwkNm, cntrctInsttOfclTelNo
        FROM cnstwk_cntrct""", conn_pr)
    df_const.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
    n_b = len(df_const); df_const = dedup_by_dcsn(df_const)
    print(f"  차수 중복제거: {n_b - len(df_const)}건")
    
    df_const_filtered, n_drop, amt_drop = filter_cnstwk_by_site(df_const, bid_df)
    print(f"  🚨 [허수 배제] 현장위치 타지역 공사 {n_drop}건 배제 (규모: {amt_drop:,.0f}원)")
    
    stats_const = process_dataframe(df_const_filtered, busan_inst_dict, busan_comp_biznos,
                                     use_location_filter=True, bid_dict=bid_dict,
                                     award_ntce_set=award_sets['공사'])
    
    # --- [B. 용역] ---
    df_servc = pd.read_sql("""SELECT untyCntrctNo, dcsnCntrctNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt,
        corpList, dminsttList, cntrctNm, cntrctInsttOfclTelNo, ntceNo, cnstrtsiteRgnNm, dminsttCd
        FROM servc_cntrct""", conn_pr)
    df_servc.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
    df_servc = dedup_by_dcsn(df_servc)
    df_servc, n_site, amt_site = filter_servc_by_site(df_servc, busan_inst_dict)
    if n_site > 0: print(f"  용역 현장 타지역 {n_site}건 배제 ({amt_site/1e8:.0f}억)")
    stats_servc = process_dataframe(df_servc, busan_inst_dict, busan_comp_biznos,
                                     use_location_filter=True, bid_dict=bid_dict,
                                     award_ntce_set=award_sets['용역'])
    
    # --- [C. 물품] ---
    df_thng = pd.read_sql("""SELECT untyCntrctNo, dcsnCntrctNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt,
        corpList, dminsttList, cntrctNm, cntrctInsttOfclTelNo, ntceNo
        FROM thng_cntrct""", conn_pr)
    df_thng.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
    df_thng = dedup_by_dcsn(df_thng)
    stats_thng = process_dataframe(df_thng, busan_inst_dict, busan_comp_biznos,
                                    use_location_filter=True, bid_dict=bid_dict,
                                    award_ntce_set=award_sets['물품'])
    
    # --- [D. 쇼핑몰] ---
    df_shop = pd.read_sql("""SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, dminsttCd, prdctAmt,
        cntrctCorpBizno, cnstwkMtrlDrctPurchsObjYn, dlvrReqNm FROM shopping_cntrct""", conn_pr)
    df_shop['dlvrReqChgOrd'] = pd.to_numeric(df_shop['dlvrReqChgOrd'], errors='coerce').fillna(0)
    df_shop.sort_values('dlvrReqChgOrd', ascending=False, inplace=True)
    df_shop.drop_duplicates(subset=['dlvrReqNo', 'prdctSno'], keep='first', inplace=True)
    
    # 공사자재 현장 필터
    df_shop, n_shop_drop, amt_shop_drop = filter_shopping_by_site(
        df_shop, conn_pr, set(busan_inst_dict.keys()))
    print(f"  🚨 [쇼핑몰 현장배제] 공사자재 타지역 {n_shop_drop}건 배제 ({amt_shop_drop:,.0f}원)")
    
    stats_shop = process_dataframe(df_shop, busan_inst_dict, busan_comp_biznos, is_shopping=True)
    
    conn_pr.close()
    
    # --- 물품+쇼핑몰 통합 ---
    stats_goods_total = {}
    for dic in [stats_thng, stats_shop]:
        for lrg, lrg_data in dic.items():
            if lrg not in stats_goods_total: stats_goods_total[lrg] = get_stats_structure()
            stats_goods_total[lrg]['total_amt'] += lrg_data['total_amt']
            stats_goods_total[lrg]['local_amt'] += lrg_data['local_amt']
            for mid, mid_data in lrg_data['sub'].items():
                if mid not in stats_goods_total[lrg]['sub']: stats_goods_total[lrg]['sub'][mid] = get_stats_structure()
                stats_goods_total[lrg]['sub'][mid]['total_amt'] += mid_data['total_amt']
                stats_goods_total[lrg]['sub'][mid]['local_amt'] += mid_data['local_amt']
                for sml, sml_data in mid_data['sub'].items():
                    if sml not in stats_goods_total[lrg]['sub'][mid]['sub']: stats_goods_total[lrg]['sub'][mid]['sub'][sml] = get_stats_structure()
                    stats_goods_total[lrg]['sub'][mid]['sub'][sml]['total_amt'] += sml_data['total_amt']
                    stats_goods_total[lrg]['sub'][mid]['sub'][sml]['local_amt'] += sml_data['local_amt']

    # 출력
    tot_c = sum(d['total_amt'] for d in stats_const.values())
    loc_c = sum(d['local_amt'] for d in stats_const.values())
    tot_s = sum(d['total_amt'] for d in stats_servc.values())
    loc_s = sum(d['local_amt'] for d in stats_servc.values())
    tot_t = sum(d['total_amt'] for d in stats_thng.values())
    loc_t = sum(d['local_amt'] for d in stats_thng.values())
    tot_p = sum(d['total_amt'] for d in stats_shop.values())
    loc_p = sum(d['local_amt'] for d in stats_shop.values())
    
    grand_tot = tot_c + tot_s + tot_t + tot_p
    grand_loc = loc_c + loc_s + loc_t + loc_p
    grand_rate = (grand_loc / grand_tot * 100) if grand_tot > 0 else 0
    
    print(f"\n{'='*70}")
    print(f" 🌟 [전체 부산시 통합 발주/구매 합산 리포트]")
    print(f"{'='*70}")
    print(f" ▶ 총괄: {grand_tot:,.0f}원 | 지역수주: {grand_loc:,.0f}원 ({grand_rate:.2f}%)")
    print(f"\n 📊 부문별:")
    print(f"  - [공사]: {tot_c:,.0f}원, 수주율 {(loc_c/tot_c*100) if tot_c>0 else 0:.2f}%")
    print(f"  - [용역]: {tot_s:,.0f}원, 수주율 {(loc_s/tot_s*100) if tot_s>0 else 0:.2f}%")
    print(f"  - [물품]: {tot_t:,.0f}원, 수주율 {(loc_t/tot_t*100) if tot_t>0 else 0:.2f}%")
    print(f"  - [쇼핑몰]: {tot_p:,.0f}원, 수주율 {(loc_p/tot_p*100) if tot_p>0 else 0:.2f}%")
    print(f"{'='*70}\n")
    
    print_tree(stats_const, "1. [공사 분야]")
    print_tree(stats_goods_total, "2. [물품+쇼핑몰 통합 분야]")
    print_tree(stats_servc, "3. [용역 분야]")
    
    print(f"\n소요 시간: {time.time()-start:.1f}초")

if __name__ == '__main__':
    main()
