import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

def find_agency_stats(keyword):
    DB_PROC = 'procurement_contracts.db'
    DB_AG = 'busan_agencies_master.db'
    DB_COMP = 'busan_companies_master.db'
    
    # 1. 기관 코드 획득
    conn_ag = sqlite3.connect(DB_AG)
    df_ag = pd.read_sql(f"SELECT dminsttCd, dminsttNm FROM agency_master WHERE dminsttNm LIKE '%{keyword}%'", conn_ag)
    conn_ag.close()
    
    if df_ag.empty:
        print(f"[{keyword}] 에 해당하는 기관을 찾을 수 없습니다.")
        return
        
    target_codes = set(df_ag['dminsttCd'].astype(str).str.strip())
    print(f"\n🔎 [{keyword}] 검색 결과 ({len(target_codes)}개 코드 발견)")
    print(", ".join(df_ag['dminsttNm'].unique().tolist()) + "\n")
    
    # 2. 지역업체 마스터 로드
    conn_cp = sqlite3.connect(DB_COMP)
    df_cp = pd.read_sql("SELECT bizno FROM company_master", conn_cp)
    conn_cp.close()
    busan_comp_biznos = set(df_cp['bizno'].dropna().astype(str).str.replace('-', '', regex=False).str.strip())
    
    conn_pr = sqlite3.connect(DB_PROC)
    
    def extract_dminstt_codes(dminstt_list_str):
        codes = []
        if not dminstt_list_str or str(dminstt_list_str) in ('nan', 'None', ''):
            return codes
        for chunk in str(dminstt_list_str).split('[')[1:]:
            chunk = chunk.split(']')[0]
            parts = chunk.split('^')
            if len(parts) >= 2:
                codes.append(str(parts[1]).strip())
        return codes

    # ★ 용역/물품 타지역 계약 배제용
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

    def is_non_busan(row):
        tel = str(row.get('cntrctInsttOfclTelNo', '')).strip()
        is_non_tel = tel and not tel.startswith(('051', '070', '010', '****'))
        nm = str(row.get('cntrctNm', '')).strip()
        has_kw = False
        for kw in NON_BUSAN_KEYWORDS:
            if kw in nm:
                excs = BUSAN_EXCEPTIONS.get(kw, [])
                if any(e in nm for e in excs): continue
                has_kw = True; break
        return is_non_tel or has_kw

    def calc_stats(table_name, date_col, is_shop=False, use_location_filter=False):
        if is_shop:
            df = pd.read_sql(f"SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, dminsttCd, prdctAmt, cntrctCorpBizno FROM {table_name} WHERE {date_col} >= '2026-01-01'", conn_pr)
            df.sort_values('dlvrReqChgOrd', ascending=False, inplace=True)
            df.drop_duplicates(subset=['dlvrReqNo', 'prdctSno'], keep='first', inplace=True)
            df['target_cd'] = df['dminsttCd'].astype(str).str.strip()
            df_target = df[df['target_cd'].isin(target_codes)]
        else:
            extra = ', cntrctNm, cntrctInsttOfclTelNo' if use_location_filter else ''
            df = pd.read_sql(f"SELECT untyCntrctNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt, corpList, dminsttList, cntrctRefNo{extra} FROM {table_name} WHERE {date_col} >= '2026-01-01'", conn_pr)
            df.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
            df['target_cd'] = df['cntrctInsttCd'].astype(str).str.strip()
            
            # ★ cntrctInsttCd로 매칭 + dminsttList에서 수요기관코드로도 매칭
            mask_direct = df['target_cd'].isin(target_codes)
            mask_dminstt = df['dminsttList'].apply(
                lambda x: any(cd in target_codes for cd in extract_dminstt_codes(x))
            )
            df_target = df[mask_direct | mask_dminstt]
        
        tot_amt = 0
        loc_amt = 0
        
        for i, row in df_target.iterrows():
            if is_shop:
                amt = float(row.get('prdctAmt', 0))
                b_nos = [(str(row.get('cntrctCorpBizno', '')).replace('-','').strip(), 100)]
            else:
                amt = float(row.get('thtmCntrctAmt', 0))
                if pd.isna(amt) or amt == 0: amt = float(row.get('totCntrctAmt', 0))
                # ★ 타지역 필터
                if use_location_filter and is_non_busan(row):
                    continue
                b_nos = []
                corp_list = str(row.get('corpList', ''))
                if corp_list and corp_list != 'nan' and corp_list != 'None':
                    for c in corp_list.split('[')[1:]:
                        parts = c.split(']')[0].split('^')
                        if len(parts) >= 10:
                            b_no = str(parts[9]).replace('-', '').strip()
                            try: share = float(str(parts[6]).strip())
                            except: share = 100.0
                            b_nos.append((b_no, share))
            
            if pd.isna(amt): amt = 0
            tot_amt += amt
            
            for b_no, share in b_nos:
                if b_no in busan_comp_biznos:
                    loc_amt += amt * (share / 100.0)
                    
        return len(df_target), tot_amt, loc_amt

    # 공사는 편의상 필터링 없이 원시계약으로 조회 테스트 (특정기관용)
    cnt_c, tot_c, loc_c = calc_stats('cnstwk_cntrct', 'cntrctDate', False)
    cnt_s, tot_s, loc_s = calc_stats('servc_cntrct', 'cntrctDate', False, use_location_filter=True)
    cnt_t, tot_t, loc_t = calc_stats('thng_cntrct', 'cntrctDate', False, use_location_filter=True)
    cnt_p, tot_p, loc_p = calc_stats('shopping_cntrct', 'dlvrReqRcptDate', True)
    
    conn_pr.close()
    
    print(f"🏢 [공사 실적] {cnt_c}건 | 발주: {tot_c:,.0f}원 / 수주: {loc_c:,.0f}원 ({(loc_c/tot_c*100) if tot_c>0 else 0:.1f}%)")
    print(f"🤝 [용역 실적] {cnt_s}건 | 발주: {tot_s:,.0f}원 / 수주: {loc_s:,.0f}원 ({(loc_s/tot_s*100) if tot_s>0 else 0:.1f}%)")
    print(f"📦 [일반물품] {cnt_t}건 | 발주: {tot_t:,.0f}원 / 수주: {loc_t:,.0f}원 ({(loc_t/tot_t*100) if tot_t>0 else 0:.1f}%)")
    print(f"🛒 [종합쇼핑몰] {cnt_p}건 | 발주: {tot_p:,.0f}원 / 수주: {loc_p:,.0f}원 ({(loc_p/tot_p*100) if tot_p>0 else 0:.1f}%)")
    
    tt = tot_c+tot_s+tot_t+tot_p
    tl = loc_c+loc_s+loc_t+loc_p
    print("-" * 50)
    print(f"🌟 [종합 합계] {cnt_c+cnt_s+cnt_t+cnt_p}건 | 발주: {tt:,.0f}원 / 수주: {tl:,.0f}원 ({(tl/tt*100) if tt>0 else 0:.2f}%)")

if __name__ == '__main__':
    find_agency_stats('부산광역시 남구')
    find_agency_stats('부산항만공사')
