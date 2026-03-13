import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')

def analyze_over_100(table):
    nm_col = 'cnstwkNm' if table == 'cnstwk_cntrct' else 'cntrctNm'
    df = pd.read_sql(f"SELECT untyCntrctNo, {nm_col} as cntrctNm, cntrctDate, totCntrctAmt, corpList FROM {table} WHERE cntrctDate >= '2026-01-01'", conn)
    
    reasons = {
        'default_100': 0, # 지분율이 아예 비어있어서 기존 코드 기준 100%씩 할당된 경우
        'api_data_error': 0, # API 자체에 기입된 지분율 숫자 합산이 100%를 초과하는 경우
        'other': 0
    }
    
    for _, row in df.iterrows():
        c_list = str(row['corpList'])
        if c_list and c_list not in ('nan', 'None'):
            corps = c_list.split('[')[1:]
            
            # 파싱
            shares = []
            has_empty_share = False
            for c in corps:
                parts = c.split(']')[0].split('^')
                if len(parts) >= 10:
                    share_str = str(parts[6]).strip()
                    if not share_str:
                        has_empty_share = True
                        share_val = 100.0 # 기존 로직 재현
                    else:
                        try:
                            share_val = float(share_str)
                        except:
                            share_val = 100.0
                            has_empty_share = True
                    shares.append(share_val)
                    
            tot = sum(shares)
            
            if tot > 100.1:
                # 분류
                if has_empty_share and all(s == 100.0 for s in shares):
                    reasons['default_100'] += 1
                elif not has_empty_share:
                    reasons['api_data_error'] += 1
                else:
                    reasons['other'] += 1
                    
    return reasons

print("🔍 [100% 초과 계약 원인 분석]")
const_reasons = analyze_over_100('cnstwk_cntrct')
print(f"공사(cnstwk_cntrct): {const_reasons}")

servc_reasons = analyze_over_100('servc_cntrct')
print(f"용역(servc_cntrct): {servc_reasons}")

print("\n원인 1 (default_100): 계약에 참여한 공동수급체 업체들의 '지분율' 값이 API 원본에서 빈칸이어서, 기존 코드상 기본값 100%가 각각 부여되어 200%, 300%로 뻥튀기된 현상.")
print("원인 2 (api_data_error): API 원본 데이터 자체에 기입된 지분율 숫자를 단순히 더했을 때 100%를 초과하는 경우 (예: '40' 대신 '400' 오타 등).")

conn.close()
