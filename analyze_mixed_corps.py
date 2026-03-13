import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

# 1. 부산 지역업체 마스터 목록 로드
conn_cp = sqlite3.connect('busan_companies_master.db')
df_cp = pd.read_sql("SELECT bizno FROM company_master", conn_cp)
conn_cp.close()
busan_comp_biznos = set(df_cp['bizno'].dropna().astype(str).str.replace('-', '', regex=False).str.strip())

# 2. 용역 계약 데이터베이스 로드
conn = sqlite3.connect('procurement_contracts.db')
df = pd.read_sql("SELECT untyCntrctNo, corpList FROM servc_cntrct WHERE cntrctDate >= '2026-01-01'", conn)
conn.close()

over_100_contracts = 0
mixed_contracts = 0
only_local = 0
only_non_local = 0

for _, row in df.iterrows():
    c_list = str(row['corpList'])
    if c_list and c_list not in ('nan', 'None'):
        corps = c_list.split('[')[1:]
        
        shares = []
        biznos = []
        
        for c in corps:
            parts = c.split(']')[0].split('^')
            if len(parts) >= 10:
                b_no = parts[9].replace('-', '').strip()
                biznos.append(b_no)
                
                share_str = str(parts[6]).strip()
                try:
                    share_val = float(share_str) if share_str else 100.0
                except:
                    share_val = 100.0
                shares.append(share_val)
                
        tot = sum(shares)
        
        # 100% 초과 이상 데이터만 대상
        if tot > 100.1:
            over_100_contracts += 1
            
            has_local = False
            has_non_local = False
            
            for b_no in biznos:
                if b_no in busan_comp_biznos:
                    has_local = True
                else:
                    has_non_local = True
                    
            if has_local and has_non_local:
                mixed_contracts += 1
            elif has_local and not has_non_local:
                only_local += 1
            elif not has_local and has_non_local:
                only_non_local += 1

print(f"📊 [용역 100% 초과 이상데이터(총 {over_100_contracts:,}건) 지역/관외 업체 혼합 분석]")
print(f" - 부산업체 + 타지역(관외)업체 혼합: {mixed_contracts:,}건 ({(mixed_contracts/over_100_contracts)*100:.1f}%)")
print(f" - 부산업체만으로 구성됨: {only_local:,}건 ({(only_local/over_100_contracts)*100:.1f}%)")
print(f" - 타지역(관외)업체로만 구성됨: {only_non_local:,}건 ({(only_non_local/over_100_contracts)*100:.1f}%)")
