"""
기관별 전체 계약 내역 엑셀 생성 모듈
dashboard.py에서 버튼 클릭 시 실시간으로 DB를 조회하여 엑셀을 만듭니다.
"""
import pandas as pd
import sqlite3
import io
import datetime
import os

from core_calc import (
    load_expanded_biznos, parse_corp_shares,
    filter_cnstwk_by_site, filter_servc_by_site, filter_shopping_by_site,
    is_non_busan_contract, check_busan_restriction, process_contract_row,
    load_bid_dict, load_award_sets
)

DB_AGENCIES = 'busan_agencies_master.db'
DB_COMPANIES = 'busan_companies_master.db'
DB_PROCUREMENT = 'procurement_contracts.db'

def generate_agency_excel(agency_name: str) -> io.BytesIO:
    """특정 기관명(비교단위)이 포함된 모든 기관의 전체 계약내역 엑셀 생성 (BytesIO)"""
    
    # 1. 기관 코드 조회 (비교단위 기준 검색)
    conn_ag = sqlite3.connect(DB_AGENCIES)
    df_ag = pd.read_sql("SELECT dminsttCd, compare_unit, cate_lrg, dminsttNm FROM agency_master", conn_ag)
    conn_ag.close()
    
    df_ag['dminsttCd'] = df_ag['dminsttCd'].astype(str).str.strip()
    busan_inst_dict = df_ag.set_index('dminsttCd').to_dict('index')
    
    # 검색된 기관 코드 필터링
    target_cds = {}
    for cd, info in busan_inst_dict.items():
        unit = str(info.get('compare_unit', '') or info.get('dminsttNm', ''))
        if agency_name in unit:
            target_cds[cd] = info
            
    if not target_cds:
        return None

    # 2. 지역업체 마스터 로딩 (동적 지점 스캔 제외: conn_pr=None)
    conn_cp = sqlite3.connect(DB_COMPANIES)
    conn_pr = sqlite3.connect(DB_PROCUREMENT)
    busan_comp_biznos = load_expanded_biznos(conn_cp)
    
    # 주소 딕셔너리 로딩
    df_adrs = pd.read_sql("SELECT bizno, rgnNm, adrs FROM company_master", conn_cp)
    df_adrs['bizno'] = df_adrs['bizno'].astype(str).str.replace('-', '', regex=False).str.strip()
    df_adrs['full_adrs'] = (df_adrs['rgnNm'].fillna('') + ' ' + df_adrs['adrs'].fillna('')).str.strip()
    comp_adrs_dict = df_adrs.set_index('bizno')['full_adrs'].to_dict()
    
    conn_cp.close()

    # 3. 브릿지 데이터 로딩
    bid_dict, bid_df = load_bid_dict(conn_pr)
    award_sets = load_award_sets(conn_pr)
    
    # 4. 조회 함수 정의
    exported_rows = []
    
    # 조회 편의성을 위한 처리기
    def process_and_append(df, sector_name, is_shopping=False, award_set=None):
        for _, row in df.iterrows():
            result = process_contract_row(
                row, busan_inst_dict, busan_comp_biznos,
                is_shopping=is_shopping,
                use_location_filter=True,
                bid_dict=bid_dict,
                award_set=award_set
            )
            if not result:
                continue
            
            matched_cd, amt, loc_amt = result
            if matched_cd not in target_cds:
                continue
                
            non_loc_amt = amt - loc_amt
            agency_info = busan_inst_dict[matched_cd]
            
            # 수주업체명 및 주소 추출
            corp_nm = ""
            corp_adrs = ""
            if is_shopping:
                corp_nm = str(row.get('corpNm', '')).strip()
                bizno = str(row.get('cntrctCorpBizno', '')).strip().replace('-', '')
                if not corp_nm or corp_nm == 'nan' or corp_nm == 'None':
                    corp_nm = bizno
                corp_adrs = comp_adrs_dict.get(bizno, '')
            else:
                corp_names = []
                adrs_list = []
                cl = str(row.get('corpList', ''))
                if cl:
                    for chunk in cl.split('[')[1:]:
                        parts = chunk.split(']')[0].split('^')
                        if len(parts) >= 4:
                            bizno = parts[2].strip().replace('-', '')
                            c_name = parts[3].strip()
                            corp_names.append(c_name)
                            c_adrs = comp_adrs_dict.get(bizno, '')
                            if c_adrs:
                                adrs_list.append(f"{c_name}({c_adrs})")
                corp_nm = ", ".join(corp_names)
                corp_adrs = " / ".join(adrs_list)

            date = ""
            if is_shopping:
                date = str(row.get('dlvrReqRcptDate', ''))
            else:
                date = str(row.get('cntrctCnclsDate', ''))
                
            method = ""
            if is_shopping:
                method = "쇼핑몰직접구매"
            else:
                method = str(row.get('cntrctCnclsMthdNm', ''))
                
            cntrct_name = str(row.get('cntrctNm', '') or row.get('cnstwkNm', '') or row.get('dlvrReqNm', '') or '')
            
            exported_rows.append({
                "계약일자": date,
                "기관그룹": agency_info.get('cate_lrg', ''),
                "수요기관": agency_info.get('compare_unit', '') or agency_info.get('dminsttNm', ''),
                "분야": sector_name,
                "계약명": cntrct_name[:100],
                "계약방식": method,
                "수주업체": corp_nm[:50],
                "수주업체 주소": corp_adrs,
                "발주액(계약액)": amt,
                "지역업체 수주액": loc_amt,
                "타지역업체 수주액(유출액)": non_loc_amt,
                "지역수주 여부": "관내수주" if loc_amt >= amt * 0.5 else "관외유출"
            })

    # --- Pre-filter Helper (Superfast using itertuples) ---
    def pre_filter(df):
        if df.empty: return df
        target_list = [str(c).strip() for c in target_cds.keys() if str(c).strip()]
        target_set = set(target_list)
        
        has_d = 'dminsttCd' in df.columns
        has_c = 'cntrctInsttCd' in df.columns
        has_l = 'dminsttList' in df.columns
        
        # Determine column indices for itertuples
        d_idx = df.columns.get_loc('dminsttCd') + 1 if has_d else -1
        c_idx = df.columns.get_loc('cntrctInsttCd') + 1 if has_c else -1
        l_idx = df.columns.get_loc('dminsttList') + 1 if has_l else -1
        
        mask = []
        for row in df.itertuples(index=False):
            if has_d and str(row[d_idx - 1]).strip() in target_set:
                mask.append(True); continue
            if has_c and str(row[c_idx - 1]).strip() in target_set:
                mask.append(True); continue
            if has_l:
                dl = str(row[l_idx - 1])
                if dl and dl not in ('nan', 'None'):
                    if any(c in dl for c in target_list):
                        mask.append(True); continue
            mask.append(False)
            
        return df[mask].copy()

    # 공사
    import core_calc
    import time
    t0 = time.time()
    with open('log.txt', 'a') as f:
        f.write(f"공사 시작\n")
    df_const = pd.read_sql("SELECT untyCntrctNo, dcsnCntrctNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt, corpList, ntceNo, dminsttList, cnstwkNm, cntrctInsttOfclTelNo, cntrctCnclsMthdNm, cntrctCnclsDate FROM cnstwk_cntrct", conn_pr)
    df_const = pre_filter(df_const)
    df_const = core_calc.dedup_by_dcsn(df_const)
    df_const, _, _ = filter_cnstwk_by_site(df_const, bid_df)
    process_and_append(df_const, "공사", award_set=award_sets['공사'])
    with open('log.txt', 'a') as f: f.write(f"공사 완료: {time.time() - t0:.2f}초\n")
    
    # 용역
    t0 = time.time()
    df_servc = pd.read_sql("SELECT untyCntrctNo, dcsnCntrctNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt, corpList, dminsttList, cntrctNm, cntrctInsttOfclTelNo, ntceNo, cnstrtsiteRgnNm, dminsttCd, cntrctCnclsMthdNm, cntrctCnclsDate FROM servc_cntrct", conn_pr)
    df_servc = pre_filter(df_servc)
    df_servc = core_calc.dedup_by_dcsn(df_servc)
    df_servc, _, _ = filter_servc_by_site(df_servc, busan_inst_dict)
    process_and_append(df_servc, "용역", award_set=award_sets['용역'])
    with open('log.txt', 'a') as f: f.write(f"용역 완료: {time.time() - t0:.2f}초\n")
    
    # 물품
    t0 = time.time()
    df_thng = pd.read_sql("SELECT untyCntrctNo, dcsnCntrctNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt, corpList, dminsttList, cntrctNm, cntrctInsttOfclTelNo, ntceNo, cntrctCnclsMthdNm, cntrctCnclsDate FROM thng_cntrct", conn_pr)
    df_thng = pre_filter(df_thng)
    df_thng = core_calc.dedup_by_dcsn(df_thng)
    process_and_append(df_thng, "물품", award_set=award_sets['물품'])
    with open('log.txt', 'a') as f: f.write(f"물품 완료: {time.time() - t0:.2f}초\n")
    
    # 쇼핑몰
    t0 = time.time()
    df_shop = pd.read_sql("SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, dminsttCd, prdctAmt, cntrctCorpBizno, corpNm, cnstwkMtrlDrctPurchsObjYn, dlvrReqNm, dlvrReqRcptDate FROM shopping_cntrct", conn_pr)
    df_shop = pre_filter(df_shop)
    df_shop['dlvrReqChgOrd'] = pd.to_numeric(df_shop['dlvrReqChgOrd'], errors='coerce').fillna(0)
    df_shop.sort_values('dlvrReqChgOrd', ascending=False, inplace=True)
    df_shop.drop_duplicates(subset=['dlvrReqNo', 'prdctSno'], keep='first', inplace=True)
    df_shop, _, _ = filter_shopping_by_site(df_shop, conn_pr, set(busan_inst_dict.keys()), inst_dict=busan_inst_dict)
    process_and_append(df_shop, "쇼핑몰", is_shopping=True)
    with open('log.txt', 'a') as f: f.write(f"쇼핑몰 완료: {time.time() - t0:.2f}초\n")
    
    conn_pr.close()
    
    if not exported_rows:
        return None
        
    t0 = time.time()
    df_export = pd.DataFrame(exported_rows)
    df_export.sort_values(by="발주액(계약액)", ascending=False, inplace=True)
    
    # 엑셀 변환 (BytesIO)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_export.to_excel(writer, index=False, sheet_name='전체계약내역')
        worksheet = writer.sheets['전체계약내역']
        
        # 컬럼 너비 맟 포맷 (openpyxl 방식)
        from openpyxl.utils import get_column_letter
        for i, col in enumerate(df_export.columns):
            col_letter = get_column_letter(i + 1)
            # 대략적인 문자열 길이에 맞춤
            max_len = max(
                df_export[col].astype(str).map(len).max(),
                len(str(col))
            ) + 2
            worksheet.column_dimensions[col_letter].width = min(max_len * 1.2, 50)
            
            # 발주액 등 숫자 컬럼 콤마 포맷
            if "액" in col:
                for cell in worksheet[col_letter]:
                    if cell.row > 1:  # 헤더 제외
                        cell.number_format = '#,##0'
                
    output.seek(0)
    with open('log.txt', 'a') as f: f.write(f"엑셀 렌더링 완료: {time.time() - t0:.2f}초\n")
    return output
