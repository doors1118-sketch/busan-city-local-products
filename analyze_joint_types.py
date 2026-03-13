import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')

def analyze_contract_types(table):
    print(f"[{table}] 분석 중...")
    df = pd.read_sql(f"SELECT untyCntrctNo, corpList FROM {table} WHERE cntrctDate >= '2026-01-01'", conn)
    
    anomalies_by_type = {}
    
    for _, row in df.iterrows():
        c_list = str(row['corpList'])
        if c_list and c_list not in ('nan', 'None'):
            corps = c_list.split('[')[1:]
            if len(corps) < 2:
                continue # 단독계약은 100%를 초과할 수 없으므로(1개 업체면 100%) 스킵
                
            shares = []
            contract_types = set()
            for c in corps:
                parts = c.split(']')[0].split('^')
                if len(parts) >= 10:
                    ctype = parts[2].strip() if len(parts) > 2 else ""
                    if not ctype: ctype = "미기재"
                    contract_types.add(ctype)
                    
                    share_str = str(parts[6]).strip()
                    try:
                        share_val = float(share_str) if share_str else 100.0
                    except:
                        share_val = 100.0
                    shares.append(share_val)
                    
            tot = sum(shares)
            
            if tot > 100.1:
                ctype_str = "+".join(sorted(list(contract_types)))
                if ctype_str not in anomalies_by_type:
                    anomalies_by_type[ctype_str] = 0
                anomalies_by_type[ctype_str] += 1
                
    return anomalies_by_type

res_c = analyze_contract_types('cnstwk_cntrct')
print(f"공사 이상데이터 계약유형 분포: {res_c}")

res_s = analyze_contract_types('servc_cntrct')
print(f"용역 이상데이터 계약유형 분포: {res_s}")

res_t = analyze_contract_types('thng_cntrct')
print(f"물품 이상데이터 계약유형 분포: {res_t}")

conn.close()
