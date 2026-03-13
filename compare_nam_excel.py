import pandas as pd
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

excel_path = r'C:\Users\COMTREE\Desktop\연습\남구물품 계약 상세내역.xlsx'

try:
    df_raw = pd.read_excel(excel_path, engine='openpyxl')
    header_idx = df_raw[df_raw.apply(lambda row: row.astype(str).str.contains('계약번호', na=False).any(), axis=1)].index
    
    if len(header_idx) > 0:
        header_row = header_idx[0]
        df_excel = pd.read_excel(excel_path, engine='openpyxl', header=header_row+1)
    else:
        df_excel = pd.read_excel(excel_path, engine='openpyxl')
        
    df_excel.columns = df_excel.columns.str.replace('\n', '').str.strip()
    
    excel_data = {}
    for _, row in df_excel.iterrows():
        no = str(row['계약번호']).strip()
        amt = float(str(row.get('계약금액', 0)).replace(',', ''))
        name = str(row.get('계약명', ''))
        if no not in excel_data:
            excel_data[no] = {'amt': amt, 'name': name}
        else:
            excel_data[no]['amt'] = max(excel_data[no]['amt'], amt)

    excel_tot = sum(d['amt'] for d in excel_data.values())
    print(f"✅ 엑셀 데이터: 총 {len(excel_data)}건 계약, 금액 합계 {excel_tot:,.0f}원")

    # DB 데이터 로드
    conn_pr = sqlite3.connect('procurement_contracts.db')
    
    conn_ag = sqlite3.connect('busan_agencies_master.db')
    df_ag = pd.read_sql("SELECT dminsttCd FROM agency_master WHERE dminsttNm LIKE '%부산광역시 남구%' OR dminsttNm LIKE '%부산광역시남구%'", conn_ag)
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
    
    df_db.drop_duplicates(subset=['cntrctRefNo', 'totCntrctAmt'], keep='last', inplace=True)
    
    df_db['target_cd'] = df_db['cntrctInsttCd'].astype(str).str.strip()
    mask_direct = df_db['target_cd'].isin(target_codes)
    mask_dminstt = df_db['dminsttList'].apply(lambda x: any(cd in target_codes for cd in extract_dminstt_codes(x)))
    df_nam_db = df_db[mask_direct | mask_dminstt]
    
    db_data = {}
    for _, row in df_nam_db.iterrows():
        no_unty = str(row['untyCntrctNo']).strip()
        no_ref = str(row['cntrctRefNo']).strip()
        amt = float(row['totCntrctAmt'])
        if pd.isna(amt) or amt == 0: amt = float(row['thtmCntrctAmt'])
        name = str(row['cntrctNm'])
        db_data[no_unty] = {'amt': amt, 'name': name, 'ref': no_ref}
        
    print(f"✅ DB 데이터: 총 {len(db_data)}건 계약, 금액 합계 {sum(d['amt'] for d in db_data.values()):,.0f}원")

    # 교차 검증
    unmatched_excel = {}
    matched_excel_db = {}
    
    for ex_no, ex_val in excel_data.items():
        found_db_key = None
        if ex_no in db_data:
            found_db_key = ex_no
        else:
            ex_no_clean = ex_no.split('-')[0]
            for db_no, db_val in db_data.items():
                if ex_no_clean in db_no or ex_no_clean in db_val['ref']:
                    found_db_key = db_no
                    break
        
        if found_db_key:
            matched_excel_db[ex_no] = {'db_key': found_db_key, 'ex_amt': ex_val['amt'], 'db_amt': db_data[found_db_key]['amt'], 'ex_name': ex_val['name'], 'db_name': db_data[found_db_key]['name']}
        else:
            unmatched_excel[ex_no] = ex_val
            
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
        if abs(v['ex_amt'] - v['db_amt']) > 10:
            print(f"  - 엑셀:{ex_no} vs DB:{v['db_key']} | {v['ex_name'][:15]} | 엑셀: {v['ex_amt']:,.0f}원 vs DB: {v['db_amt']:,.0f}원 (차액: {v['ex_amt']-v['db_amt']:,.0f}원)")
            diff_count += 1
    if diff_count == 0: print("  - 없음 (금액 모두 일치)")

except Exception as e:
    import traceback
    traceback.print_exc()
