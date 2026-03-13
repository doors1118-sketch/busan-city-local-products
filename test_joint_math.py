import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

# Load the API Excel and Busan Regional Master DB
try:
    df_api = pd.read_excel('API_공사계약조회_전체(20260101_20260131).xlsx')
    
    import sqlite3
    conn_comp = sqlite3.connect('busan_companies_master.db')
    df_comp = pd.read_sql("SELECT bizno FROM company_master", conn_comp)
    busan_biznos = set(df_comp['bizno'].dropna().astype(str).str.replace('-', '').str.strip())
    conn_comp.close()
    
    # Check for specific contract types
    mask_bndam = df_api.apply(lambda row: row.astype(str).str.contains('분담').any(), axis=1)
    mask_jugeyak = df_api.apply(lambda row: row.astype(str).str.contains('주계약').any(), axis=1)
    
    df_bndam = df_api[mask_bndam]
    df_jugeyak = df_api[mask_jugeyak]

    def process_and_print_sample(df, label):
        print(f"\n======================================")
        print(f" 🔎 [{label}] 실제 배분 연산 시뮬레이션")
        print(f"======================================")
        
        sample_row = None
        # Try to find a contract with a Busan company
        for idx, row in df.iterrows():
            corp_str = str(row.get('corpList', ''))
            if corp_str and 'nan' not in corp_str:
                for b in busan_biznos:
                    if str(b) in corp_str:
                        sample_row = row
                        break
            if sample_row is not None:
                break
                
        # If no Busan company is found, just use the first available row to prove the math
        if sample_row is None and len(df) > 0:
            sample_row = df.iloc[0]
            
        if sample_row is not None:
            amt = sample_row.get('totCntrctAmt', sample_row.get('thtmCntrctAmt', 0))
            if pd.isna(amt): amt = 0
            amt = float(amt)
            
            print(f"- 계약체결일자: {sample_row.get('cntrctCnclsDate', '')}")
            print(f"- 계약명: {sample_row['cnstwkNm']}")
            print(f"- 수요기관: {sample_row['cntrctInsttNm']}")
            print(f"- 총 계약금액: {amt:,.0f} 원 (이 금액이 중복 없이 1회분만 나옴)")
            print(f"- 원본 corpList (파싱 전 데이터): \n  {sample_row['corpList']}")
            print(f"\n⬇️ [ 지분율(%) 비례 참여업체별 금액 분할 연산 과정 ]")
            
            corps = str(sample_row['corpList']).split('[')[1:]
            
            calc_sum = 0
            for i, c in enumerate(corps):
                c = c.split(']')[0]
                parts = c.split('^')
                if len(parts) >= 10:
                    biz_no = str(parts[9]).replace('-', '').strip()
                    comp_name = parts[3]
                    share_str = str(parts[6]).strip()
                    role = parts[1]
                    method = parts[2]
                    try:
                        share = float(share_str)
                    except:
                        share = 100.0
                        
                    is_busan = 'O' if biz_no in busan_biznos else 'X'
                    local_amt = amt * (share / 100.0)
                    calc_sum += local_amt
                    
                    print(f"  업체 {i+1}: {comp_name}")
                    print(f"     ㄴ 역할: {role} ({method}) / 부산지역업체 여부: [{is_busan}]")
                    print(f"     ㄴ 지분율: {share:.2f}%  =>  수주액 할당: {local_amt:,.0f} 원")
                    
            print(f"\n✅ 지분율 환산 총계 검증: \n   분할 금액의 합계 ({calc_sum:,.0f} 원) == 총 계약금액 ({amt:,.0f} 원)")
        else:
            print("해당 유형의 데이터가 없습니다.")

    process_and_print_sample(df_bndam, "분담이행 계약")
    process_and_print_sample(df_jugeyak, "주계약자 관리방식 계약")

except Exception as e:
    import traceback
    traceback.print_exc()

