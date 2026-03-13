import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

DB_PROC = 'procurement_contracts.db'
DB_AG = 'busan_agencies_master.db'
DB_COMP = 'busan_companies_master.db'

# 1. 기관 코드 -> 대분류(cate_lrg) 사전 생성
conn_ag = sqlite3.connect(DB_AG)
df_ag = pd.read_sql("SELECT dminsttCd, cate_lrg FROM agency_master", conn_ag)
conn_ag.close()
df_ag['dminsttCd'] = df_ag['dminsttCd'].astype(str).str.strip()
df_ag['cate_lrg'] = df_ag['cate_lrg'].fillna('미분류')
agency_map = df_ag.set_index('dminsttCd')['cate_lrg'].to_dict()

# 2. 지역업체 목록
conn_cp = sqlite3.connect(DB_COMP)
df_cp = pd.read_sql("SELECT bizno FROM company_master", conn_cp)
conn_cp.close()
busan_comp_biznos = set(df_cp['bizno'].dropna().astype(str).str.replace('-', '', regex=False).str.strip())

# ★ 용역/물품 타지역 계약 배제용 키워드
NON_BUSAN_KEYWORDS = [
    '서울', '인천', '대구', '대전', '광주광역', '울산',
    '세종', '제주',
    '경기', '경기도', '강원', '강원도', '강원특별',
    '충북', '충청북도', '충남', '충청남도',
    '전북', '전라북도', '전북특별', '전남', '전라남도',
    '경북', '경상북도', '경남', '경상남도',
    '울릉', '독도',
]
BUSAN_EXCEPTIONS = {'대구': ['해운대구']}

def is_non_busan_contract(row, lrg):
    if lrg == '부산광역시 및 소속기관':
        return False
    tel = str(row.get('cntrctInsttOfclTelNo', '')).strip()
    is_non_busan_tel = tel and not tel.startswith(('051', '070', '010', '****'))
    cntrct_nm = str(row.get('cntrctNm', '')).strip()
    has_kw = False
    for kw in NON_BUSAN_KEYWORDS:
        if kw in cntrct_nm:
            exceptions = BUSAN_EXCEPTIONS.get(kw, [])
            if any(exc in cntrct_nm for exc in exceptions):
                continue
            has_kw = True
            break
    return is_non_busan_tel or has_kw

def extract_lrg(dminstt_list_str, instt_cd):
    # 우선 dminsttList에서 첫 번째로 유효한 코드를 찾아 대분류를 판별
    if dminstt_list_str and str(dminstt_list_str) not in ('nan', 'None', ''):
        for chunk in str(dminstt_list_str).split('[')[1:]:
            parts = chunk.split(']')[0].split('^')
            if len(parts) >= 2:
                code = str(parts[1]).strip()
                if code in agency_map and agency_map[code] != '미분류':
                    return agency_map[code]
    
    # 실패하면 cntrctInsttCd(계약기관) 코드로 판별
    cd = str(instt_cd).strip()
    return agency_map.get(cd, '미분류')

conn_pr = sqlite3.connect(DB_PROC)

def calc_sector(table_name, date_col, is_shop=False, use_location_filter=False):
    if is_shop:
        df = pd.read_sql(f"SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, dminsttCd, prdctAmt, cntrctCorpBizno FROM {table_name} WHERE {date_col} >= '2026-01-01'", conn_pr)
        df.sort_values('dlvrReqChgOrd', ascending=False, inplace=True)
        df.drop_duplicates(subset=['dlvrReqNo', 'prdctSno'], keep='first', inplace=True)
    else:
        extra_cols = ', cntrctNm, cntrctInsttOfclTelNo' if use_location_filter else ''
        ntce_col = ', ntceNo' if table_name == 'cnstwk_cntrct' else ''
        df = pd.read_sql(f"SELECT untyCntrctNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt, corpList, dminsttList, cntrctRefNo{ntce_col}{extra_cols} FROM {table_name} WHERE {date_col} >= '2026-01-01'", conn_pr)
        df.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
        
        # 공사인 경우 타지역 필터링
        if table_name == 'cnstwk_cntrct':
            df_bid = pd.read_sql("SELECT bidNtceNo, cnstrtsiteRgnNm FROM bid_notices_raw WHERE cnstrtsiteRgnNm IS NOT NULL", conn_pr)
            df['ntceNo_str'] = df['ntceNo'].astype(str).str.replace('-', '', regex=False).str.strip()
            df_bid['bidNtceNo_str'] = df_bid['bidNtceNo'].astype(str).str.strip()
            df = pd.merge(df, df_bid[['bidNtceNo_str', 'cnstrtsiteRgnNm']], how='left', left_on='ntceNo_str', right_on='bidNtceNo_str')
            df = df[~ (df['cnstrtsiteRgnNm'].notna() & (~df['cnstrtsiteRgnNm'].str.contains('부산', na=False)))]

    stats = {
        '부산광역시 및 소속기관': {'tot': 0.0, 'loc': 0.0},
        '정부 및 국가공공기관': {'tot': 0.0, 'loc': 0.0},
        '기타(민간/미분류)': {'tot': 0.0, 'loc': 0.0}
    }
    
    for i, row in df.iterrows():
        if is_shop:
            lrg = agency_map.get(str(row.get('dminsttCd')).strip(), '미분류')
            amt = float(row.get('prdctAmt', 0))
            b_nos = [(str(row.get('cntrctCorpBizno', '')).replace('-','').strip(), 100.0)]
        else:
            lrg = extract_lrg(row.get('dminsttList'), row.get('cntrctInsttCd'))
            # ★ 용역/물품 타지역 필터
            if use_location_filter and is_non_busan_contract(row, lrg):
                continue
            amt = float(row.get('thtmCntrctAmt', 0))
            if pd.isna(amt) or amt == 0: amt = float(row.get('totCntrctAmt', 0))
            
            b_nos = []
            corp_list = str(row.get('corpList', ''))
            if corp_list and corp_list != 'nan' and corp_list != 'None':
                for c in corp_list.split('[')[1:]:
                    parts = c.split(']')[0].split('^')
                    if len(parts) >= 10:
                        b_no = str(parts[9]).replace('-', '').strip()
                        try: share = float(str(parts[6]).strip())
                        except: share = 0.0
                        b_nos.append([b_no, share])
                        
            # 정규화 방어 로직 추가
            if b_nos:
                tot_share = sum(s[1] for s in b_nos)
                if tot_share == 0:
                    n_corps = len(b_nos)
                    if n_corps > 0:
                        for idx in range(n_corps):
                            b_nos[idx][1] = 100.0 / n_corps
                        tot_share = 100.0
                if tot_share > 100.1:
                    for idx in range(len(b_nos)):
                        b_nos[idx][1] = (b_nos[idx][1] / tot_share) * 100.0
                        
        if pd.isna(amt): amt = 0
        loc_amt = 0
        for b_no, share in b_nos:
            if b_no in busan_comp_biznos:
                loc_amt += amt * (share / 100.0)
                
        # Group handling
        if lrg == '부산광역시 및 소속기관': group = '부산광역시 및 소속기관'
        elif lrg == '정부 및 국가공공기관': group = '정부 및 국가공공기관'
        else: group = '기타(민간/미분류)'
        
        stats[group]['tot'] += amt
        stats[group]['loc'] += loc_amt
        
    return stats

res_c = calc_sector('cnstwk_cntrct', 'cntrctDate')
res_s = calc_sector('servc_cntrct', 'cntrctDate', use_location_filter=True)
res_t = calc_sector('thng_cntrct', 'cntrctDate', use_location_filter=True)
res_p = calc_sector('shopping_cntrct', 'dlvrReqRcptDate', True)

conn_pr.close()

groups = ['부산광역시 및 소속기관', '정부 및 국가공공기관']

print("\n" + "="*70)
print(" 📊 [부산광역시 전체 발주 집계] 대분류/분야별 상세 내역 (26년 1월)")
print("="*70)

for g in groups:
    c_tot, c_loc = res_c[g]['tot'], res_c[g]['loc']
    s_tot, s_loc = res_s[g]['tot'], res_s[g]['loc']
    m_tot = res_t[g]['tot'] + res_p[g]['tot']
    m_loc = res_t[g]['loc'] + res_p[g]['loc']
    
    g_tot = c_tot + s_tot + m_tot
    g_loc = c_loc + s_loc + m_loc
    
    print(f"\n▶ 🏢 [{g}] 합계")
    print(f"  - 전체 합계: 발주액 {g_tot:,.0f}원 / 부산수주 {g_loc:,.0f}원 (지역수주율: {(g_loc/g_tot*100) if g_tot>0 else 0:.1f}%)")
    print("-" * 50)
    print(f"  - 🏗️ 공사: 발주액 {c_tot:,.0f}원 / 수주율 {(c_loc/c_tot*100) if c_tot>0 else 0:.1f}%")
    print(f"  - 🤝 용역: 발주액 {s_tot:,.0f}원 / 수주율 {(s_loc/s_tot*100) if s_tot>0 else 0:.1f}%")
    print(f"  - 📦 물품: 발주액 {m_tot:,.0f}원 / 수주율 {(m_loc/m_tot*100) if m_tot>0 else 0:.1f}%")

print("\n" + "="*70)
print(" 🌟 [부산광역시 + 정부기관 총괄 합산]")
print("="*70)

all_c_tot = sum(res_c[g]['tot'] for g in groups)
all_c_loc = sum(res_c[g]['loc'] for g in groups)
all_s_tot = sum(res_s[g]['tot'] for g in groups)
all_s_loc = sum(res_s[g]['loc'] for g in groups)
all_m_tot = sum(res_t[g]['tot'] + res_p[g]['tot'] for g in groups)
all_m_loc = sum(res_t[g]['loc'] + res_p[g]['loc'] for g in groups)

grand_tot = all_c_tot + all_s_tot + all_m_tot
grand_loc = all_c_loc + all_s_loc + all_m_loc

print(f"  - 전체 실적: 발주액 {grand_tot:,.0f}원 / 수주액 {grand_loc:,.0f}원 (수주율: {(grand_loc/grand_tot*100) if grand_tot>0 else 0:.2f}%)")
print(f"  - 공사 수주율: {(all_c_loc/all_c_tot*100) if all_c_tot>0 else 0:.1f}%")
print(f"  - 용역 수주율: {(all_s_loc/all_s_tot*100) if all_s_tot>0 else 0:.1f}%")
print(f"  - 물품 수주율: {(all_m_loc/all_m_tot*100) if all_m_tot>0 else 0:.1f}%")
