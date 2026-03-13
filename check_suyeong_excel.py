import pandas as pd
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

excel_path = r'C:\Users\COMTREE\Desktop\연습\수영구 물품.xlsx'

try:
    # 엑셀 파일 로드
    df_raw = pd.read_excel(excel_path, engine='openpyxl')
    header_idx = df_raw[df_raw.apply(lambda row: row.astype(str).str.contains('계약번호', na=False).any(), axis=1)].index
    
    if len(header_idx) > 0:
        header_row = header_idx[0]
        df_excel = pd.read_excel(excel_path, engine='openpyxl', header=header_row+1)
    else:
        df_excel = pd.read_excel(excel_path, engine='openpyxl')
        
    df_excel.columns = df_excel.columns.str.replace('\n', '').str.strip()
    
    # 엑셀 데이터 파싱 (계약번호 기준 그룹화 - 물품순번 때문에 중복될 수 있으므로 금액은 계약번호별 최대값 사용)
    # 확인: 계약금액이 총액계약인지 단가계약인지에 따라 합산해야 할 수도 있음.
    # 보통 조달 데이터허브 물품내역에서 '계약금액'은 해당 건의 총 액수이거나 물품별 금액임.
    # 여기서는 계약번호별 계약금액을 합산해봄. (단가 집계 오류 피하기 위해)
    
    excel_data = {}
    for _, row in df_excel.iterrows():
        no = str(row['계약번호']).strip()
        amt = float(str(row.get('계약금액', 0)).replace(',', ''))
        name = str(row.get('계약명', ''))
        
        if no not in excel_data:
            excel_data[no] = {'amt': 0, 'name': name}
        
        # 품목별 분할된 행인 경우 계약금액이 다 똑같이 찍히는지, 아니면 나뉘어 있는지 확인
        # 만약 같은 계약번호로 여러 행이 있고 금액이 똑같다면 max()를 취해야 함.
        # 일단 합계로 집계
        excel_data[no]['amt'] += amt
        
    # 만약 같은 계약번호 행들의 금액이 모두 동일했다면 합계가 실제 * N배로 뻥튀기됨.
    # 이를 보정하기 위해 Max 방식으로도 수집해보기.
    excel_data_max = {}
    for _, row in df_excel.iterrows():
        no = str(row['계약번호']).strip()
        amt = float(str(row.get('계약금액', 0)).replace(',', ''))
        name = str(row.get('계약명', ''))
        if no not in excel_data_max:
            excel_data_max[no] = {'amt': amt, 'name': name}
        else:
            excel_data_max[no]['amt'] = max(excel_data_max[no]['amt'], amt)

    # 엑셀 총액 (Max 방식)
    excel_tot = sum(d['amt'] for d in excel_data_max.values())
    print(f"✅ 엑셀 데이터: 총 {len(excel_data_max)}건 계약, 금액 합계 {excel_tot:,.0f}원")

    # DB 데이터 로드
    DB_PROC = 'procurement_contracts.db'
    conn_pr = sqlite3.connect(DB_PROC)
    
    # DB 수영구 기관코드 추출
    conn_ag = sqlite3.connect('busan_agencies_master.db')
    df_ag = pd.read_sql(f"SELECT dminsttCd FROM agency_master WHERE dminsttNm LIKE '%부산광역시 수영구%'", conn_ag)
    conn_ag.close()
    target_codes = set(df_ag['dminsttCd'].astype(str).str.strip())
    
    def extract_dminstt_codes(dminstt_list_str):
        codes = []
        if not dminstt_list_str or str(dminstt_list_str) in ('nan', 'None', ''): return codes
        for chunk in str(dminstt_list_str).split('[')[1:]:
            parts = chunk.split(']')[0].split('^')
            if len(parts) >= 2: codes.append(str(parts[1]).strip())
        return codes

    df_db = pd.read_sql("SELECT untyCntrctNo, cntrctRefNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt, cntrctNm, dminsttList FROM thng_cntrct WHERE cntrctDate >= '2026-01-01' AND cntrctDate <= '2026-01-31'", conn_pr)
    conn_pr.close()
    
    # 중복 제거 (API 에러 방지용)
    df_db.drop_duplicates(subset=['cntrctRefNo', 'totCntrctAmt'], keep='last', inplace=True)
    
    df_db['target_cd'] = df_db['cntrctInsttCd'].astype(str).str.strip()
    mask_direct = df_db['target_cd'].isin(target_codes)
    mask_dminstt = df_db['dminsttList'].apply(lambda x: any(cd in target_codes for cd in extract_dminstt_codes(x)))
    df_suyeong_db = df_db[mask_direct | mask_dminstt]
    
    db_data = {}
    for _, row in df_suyeong_db.iterrows():
        # DB에서는 untyCntrctNo를 기본으로 쓰되, 엑셀은 대부분 cntrctRefNo(계약참조번호)나 untyCntrctNo의 일부를 사용.
        # 매칭을 위해 번호에서 문자나 바(-)를 제거하고 가장 긴 숫자로 비교
        no_unty = str(row['untyCntrctNo']).strip()
        no_ref = str(row['cntrctRefNo']).strip()
        amt = float(row['totCntrctAmt'])
        if pd.isna(amt) or amt == 0: amt = float(row['thtmCntrctAmt'])
        name = str(row['cntrctNm'])
        
        # 엑셀 계약번호 매칭을 가장 잘 하려면 둘 다 키로 저장해두고 찾음
        db_data[no_unty] = {'amt': amt, 'name': name, 'ref': no_ref}
        
    print(f"✅ DB 데이터: 총 {len(db_data)}건 계약, 금액 합계 {sum(d['amt'] for d in db_data.values()):,.0f}원")

    # 교차 검증 로직 (엑셀 번호가 DB untyCntrctNo 앞부분에 매칭되거나, cntrctRefNo와 매칭되는지 확인)
    unmatched_excel = {}
    matched_excel_db = {}
    
    # 엑셀 계약을 기준으로 DB에서 찾기
    for ex_no, ex_val in excel_data_max.items():
        found_db_key = None
        # 1. 완전 일치 (untyCntrctNo)
        if ex_no in db_data:
            found_db_key = ex_no
        else:
            # 2. 부분 일치 (엑셀 번호가 DB 번호 안에 포함됨)
            # 보통 엑셀 계약번호(예: 20260124234-00)와 DB(20260124234) 차이
            ex_no_clean = ex_no.split('-')[0]
            for db_no, db_val in db_data.items():
                if ex_no_clean in db_no or ex_no_clean in db_val['ref']:
                    found_db_key = db_no
                    break
        
        if found_db_key:
            matched_excel_db[ex_no] = {'db_key': found_db_key, 'ex_amt': ex_val['amt'], 'db_amt': db_data[found_db_key]['amt'], 'ex_name': ex_val['name'], 'db_name': db_data[found_db_key]['name']}
        else:
            unmatched_excel[ex_no] = ex_val
            
    # DB에만 있는 건 찾기
    matched_db_keys = [v['db_key'] for v in matched_excel_db.values()]
    unmatched_db = {k: v for k, v in db_data.items() if k not in matched_db_keys}
    
    print("\n" + "="*50)
    print("📊 교차 검증 상세 결과")
    print("="*50)
    
    print(f"\n[1] 엑셀에만 있는 계약 (DB 누락) -> 총 {len(unmatched_excel)}건")
    for k, v in unmatched_excel.items():
        print(f"  - {k} | {v['name'][:30]} | {v['amt']:,.0f}원")
        
    print(f"\n[2] DB에만 있는 계약 (엑셀 누락) -> 총 {len(unmatched_db)}건")
    for k, v in unmatched_db.items():
        print(f"  - {k} ({v['ref']}) | {v['name'][:30]} | {v['amt']:,.0f}원")
        
    print(f"\n[3] 금액 불일치 계약 (양쪽에는 다 있음)")
    diff_count = 0
    for ex_no, v in matched_excel_db.items():
        if abs(v['ex_amt'] - v['db_amt']) > 10: # 소수점 오차 방지
            print(f"  - 엑셀:{ex_no} vs DB:{v['db_key']} | {v['ex_name'][:15]} | 엑셀: {v['ex_amt']:,.0f}원 vs DB: {v['db_amt']:,.0f}원 (차액: {v['ex_amt']-v['db_amt']:,.0f}원)")
            diff_count += 1
    if diff_count == 0: print("  - 없음 (금액 모두 일치)")

except Exception as e:
    import traceback
    traceback.print_exc()

