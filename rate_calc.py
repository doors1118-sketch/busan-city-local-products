import pandas as pd
import numpy as np
import warnings
import sys
import pprint
import sqlite3

sys.stdout.reconfigure(encoding='utf-8')
warnings.filterwarnings('ignore')

DB_AGENCIES = 'busan_agencies_master.db'

try:
    print("1. Loading Master Files...")

    # ── 부산 수요기관 마스터 DB에서 카테고리 정보와 함께 로드 (절대 기준) ──
    conn_ag = sqlite3.connect(DB_AGENCIES)
    df_ag = pd.read_sql("SELECT dminsttCd, dminsttNm, cate_lrg, cate_mid, cate_sml FROM agency_master", conn_ag)
    # 빈 값은 '미분류'로 통일
    df_ag['cate_lrg'] = df_ag['cate_lrg'].fillna('미분류').replace('', '미분류')
    df_ag['cate_mid'] = df_ag['cate_mid'].fillna('미분류').replace('', '미분류')
    df_ag['cate_sml'] = df_ag['cate_sml'].fillna('미분류').replace('', '미분류')
    
    busan_inst_codes = set(df_ag['dminsttCd'].dropna().astype(str).str.strip())
    conn_ag.close()

    # Load 부산 지역업체 마스터 (DB 조회로 교체)
    conn_comp = sqlite3.connect('busan_companies_master.db')
    df_comp = pd.read_sql("SELECT bizno FROM company_master", conn_comp)
    busan_comp_biznos = set(df_comp['bizno'].dropna().astype(str).str.replace('-', '').str.strip())
    conn_comp.close()
    
    print(f"  - 부산 수요기관 갯수 (마스터 DB 기준): {len(busan_inst_codes)}")
    print(f"  - 부산 지역업체 갯수: {len(busan_comp_biznos)}")

    #################################################
    # 2. API Data Calculation
    #################################################
    print("\n2. Processing API Data (계층별 그룹화 연산 중)...")
    df_api = pd.read_excel('API_공사계약조회_전체(20260101_20260131).xlsx')
    
    # 조인을 위해 타입 통일
    df_api['cntrctInsttCd_str'] = df_api['cntrctInsttCd'].astype(str).str.strip()
    df_ag['dminsttCd_str'] = df_ag['dminsttCd'].astype(str).str.strip()
    
    # 1차 필터링 & 카테고리 매핑 (LEFT JOIN)
    api_busan = pd.merge(df_api, df_ag[['dminsttCd_str', 'cate_lrg', 'cate_mid', 'cate_sml']], 
                         how='inner', left_on='cntrctInsttCd_str', right_on='dminsttCd_str')
    
    # -----------------[현장 위치 정밀 검증 필터 스위치]-----------------
    # 입찰공고 테이블의 현장 주소와 조인하여 타지역 공사를 발라냄
    conn_db = sqlite3.connect('procurement_contracts.db')
    df_bid = pd.read_sql("SELECT bidNtceNo, cnstrtsiteRgnNm FROM bid_notices_raw", conn_db)
    conn_db.close()
    
    api_busan['ntceNo_str'] = api_busan['ntceNo'].astype(str).str.strip()
    df_bid['bidNtceNo_str'] = df_bid['bidNtceNo'].astype(str).str.strip()
    
    api_busan = pd.merge(api_busan, df_bid, how='left', left_on='ntceNo_str', right_on='bidNtceNo_str')
    
    # 타지역 공사 필터링 마스킹
    api_busan['site_loc'] = api_busan['cnstrtsiteRgnNm'].fillna('').astype(str)
    
    mask_mapped = api_busan['site_loc'] != ''
    mask_outside = mask_mapped & (~api_busan['site_loc'].str.contains('부산'))
    
    outside_busan_df = api_busan[mask_outside]
    if len(outside_busan_df) > 0:
        outside_orders_amt = outside_busan_df[['totCntrctAmt', 'thtmCntrctAmt']].max(axis=1).fillna(0).sum()
        print(f"  🚨 [허수 배제] 발주처는 부산이나 현장위치가 타지역인 허수 공사 {len(outside_busan_df)}건 배제 완료 (규모: {outside_orders_amt:,.0f}원)")
    
    # 진짜 부산 공사만 남김
    api_busan = api_busan[~mask_outside]
    # -------------------------------------------------------------------
    
    # 그룹 통계 저장용 딕셔너리
    # stats_dict = { "cate_lrg": { "total_amt": 0, "local_amt": 0, "sub": { "cate_mid": { "total_amt": 0, "local_amt": 0 } } } }
    stats_dict = {}
    
    grand_total_amt = 0
    grand_local_amt = 0
    
    for i, row in api_busan.iterrows():
        # Get Amount
        amt = float(row.get('thtmCntrctAmt', 0))
        if np.isnan(amt) or amt == 0:
            amt = float(row.get('totCntrctAmt', 0))
        if np.isnan(amt): amt = 0
            
        grand_total_amt += amt
        
        # 카테고리 획득
        lrg = str(row.get('cate_lrg', '미분류'))
        mid = str(row.get('cate_mid', '미분류'))
        sml = str(row.get('cate_sml', '미분류'))
        
        if lrg not in stats_dict:
            stats_dict[lrg] = {'total_amt': 0, 'local_amt': 0, 'sub': {}}
        if mid not in stats_dict[lrg]['sub']:
            stats_dict[lrg]['sub'][mid] = {'total_amt': 0, 'local_amt': 0, 'sub': {}}
        if sml not in stats_dict[lrg]['sub'][mid]['sub']:
            stats_dict[lrg]['sub'][mid]['sub'][sml] = {'total_amt': 0, 'local_amt': 0}
            
        stats_dict[lrg]['total_amt'] += amt
        stats_dict[lrg]['sub'][mid]['total_amt'] += amt
        stats_dict[lrg]['sub'][mid]['sub'][sml]['total_amt'] += amt
        
        # Calculate local share
        local_amt_this_row = 0
        corp_list_str = str(row.get('corpList', ''))
        
        biz_nos = []
        if corp_list_str and corp_list_str != 'nan':
            corps = corp_list_str.split('[')[1:]
            for c in corps:
                c = c.split(']')[0]
                parts = c.split('^')
                if len(parts) >= 10:
                    biz_no = str(parts[9]).replace('-', '').strip()
                    share_str = str(parts[6]).strip()
                    try: share = float(share_str) if share_str else 0.0
                    except: share = 0.0
                    biz_nos.append([biz_no, share])
                    
            if biz_nos:
                tot_share = sum(s[1] for s in biz_nos)
                if tot_share == 0:
                    n_corps = len(biz_nos)
                    if n_corps > 0:
                        for idx in range(n_corps):
                            biz_nos[idx][1] = 100.0 / n_corps
                        tot_share = 100.0
                if tot_share > 100.1:
                    for idx in range(len(biz_nos)):
                        biz_nos[idx][1] = (biz_nos[idx][1] / tot_share) * 100.0
                        
            for b_no, share in biz_nos:
                if b_no in busan_comp_biznos:
                    local_amt_this_row += (amt * (share / 100.0))
        else:
            biz_no = str(row.get('rprsntCorpBizrno', '')).replace('-', '').strip()
            if biz_no in busan_comp_biznos:
                local_amt_this_row += amt
                
        grand_local_amt += local_amt_this_row
        stats_dict[lrg]['local_amt'] += local_amt_this_row
        stats_dict[lrg]['sub'][mid]['local_amt'] += local_amt_this_row
        stats_dict[lrg]['sub'][mid]['sub'][sml]['local_amt'] += local_amt_this_row

    print(f"\\n========================================================")
    print(f" [최종 산출 결과 리포트 (API 계층별 병합)]")
    print(f"========================================================")
    grand_rate = (grand_local_amt / grand_total_amt * 100) if grand_total_amt > 0 else 0
    print(f" 🌟 [대조군 전체 합산] 발주총액: {grand_total_amt:,.0f}원 | 지역수주액: {grand_local_amt:,.0f}원 (지역수주율: {grand_rate:.2f}%)")
    print()
    
    # 딕셔너리를 순회하며 트리 구조로 출력
    for lrg, data_lrg in stats_dict.items():
        l_rate = (data_lrg['local_amt'] / data_lrg['total_amt'] * 100) if data_lrg['total_amt'] > 0 else 0
        print(f" 🔽 [{lrg}] 그룹 (발주액: {data_lrg['total_amt']:,.0f}원 | 수주액: {data_lrg['local_amt']:,.0f}원 | 수주율: {l_rate:.2f}%)")
        
        # 하위 카테고리 정렬(총액 기준 내림차순)
        sorted_sub = sorted(data_lrg['sub'].items(), key=lambda x: x[1]['total_amt'], reverse=True)
        for i, (mid, data_mid) in enumerate(sorted_sub):
            m_rate = (data_mid['local_amt'] / data_mid['total_amt'] * 100) if data_mid['total_amt'] > 0 else 0
            connector_mid = "└─" if i == len(sorted_sub) - 1 else "├─"
            print(f"     {connector_mid} {mid}: 발주액 {data_mid['total_amt']:,.0f}원 (수주율 {m_rate:.2f}%)")
            
            # 3단계 소분류 정렬
            sorted_sml = sorted(data_mid['sub'].items(), key=lambda x: x[1]['total_amt'], reverse=True)
            for j, (sml, data_sml) in enumerate(sorted_sml):
                if max([v['total_amt'] for k, v in data_mid['sub'].items()]) == 0: continue
                # 소분류가 미분류 1개뿐이고 의미가 없는 경우 생략
                if len(sorted_sml) == 1 and sml == '미분류': continue
                
                s_rate = (data_sml['local_amt'] / data_sml['total_amt'] * 100) if data_sml['total_amt'] > 0 else 0
                spacer = "    " if i == len(sorted_sub) - 1 else "│   "
                connector_sml = "└─" if j == len(sorted_sml) - 1 else "├─"
                print(f"     {spacer} {connector_sml} {sml}: 발주액 {data_sml['total_amt']:,.0f}원 (수주율 {s_rate:.2f}%)")
        print()

    #################################################
    # 3. Manual Data Calculation
    #################################################
    print("\n3. Processing Manual Data...")
    df_man = pd.read_excel('260101 공사 공동수급 계약 내역.xlsx', header=0)
    
    # Heuristic:
    # 10th col (Index 9 or 10) usually has Agency Region "부산광역시"
    # amount is usually Unnamed: 46 (participant amount)
    # biz_no is somewhere between Unnamed: 30 to 39
    
    col_region = df_man.columns[10]
    col_amt = df_man.columns[46]
    
    # Filter by Busan region
    man_busan = df_man[df_man[col_region].astype(str).str.contains('부산', na=False)]
    
    total_man_orders = 0
    total_man_local_amt = 0
    
    for i, row in man_busan.iterrows():
        try:
            amt_str = str(row[col_amt]).replace(',', '').strip()
            part_amt = float(amt_str) if amt_str.replace('.', '', 1).isdigit() else 0.0
        except:
            part_amt = 0.0
            
        total_man_orders += part_amt
        
        # Search for valid business number in this row's columns (index 30 to 40)
        is_local = False
        for c_idx in range(30, min(42, len(df_man.columns))):
            val = str(row.iloc[c_idx]).replace('-', '').strip()
            if val in busan_comp_biznos:
                is_local = True
                break
                
        if is_local:
            total_man_local_amt += part_amt
            
    print(f"  [Manual Result]")
    print(f"  - 발주 금액 총액: {total_man_orders:,.0f} 원")
    print(f"  - 지역업체 수주액: {total_man_local_amt:,.0f} 원")
    rate_man = (total_man_local_amt / total_man_orders * 100) if total_man_orders > 0 else 0
    print(f"  - 지역업체 수주율: {rate_man:.2f}%")

except Exception as e:
    import traceback
    traceback.print_exc()

