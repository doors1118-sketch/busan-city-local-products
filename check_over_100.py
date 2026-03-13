import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn_cp = sqlite3.connect('busan_companies_master.db')
df_cp = pd.read_sql('SELECT bizno FROM company_master', conn_cp)
conn_cp.close()
busan_comp_biznos = set(df_cp['bizno'].dropna().astype(str).str.replace('-', '', regex=False).str.strip())

conn = sqlite3.connect('procurement_contracts.db')

def check_over_100(table):
    nm_col = 'cnstwkNm' if table == 'cnstwk_cntrct' else 'cntrctNm'
    df = pd.read_sql(f"SELECT untyCntrctNo, {nm_col} as cntrctNm, cntrctDate, totCntrctAmt, corpList FROM {table} WHERE cntrctDate >= '2026-01-01'", conn)
    
    over_100_cnt = 0
    total_scanned = 0
    
    print(f"\n[{table}] Scanning {len(df):,} records for >100% local share anomalies...")
    
    for _, row in df.iterrows():
        c_list = str(row['corpList'])
        if c_list and c_list not in ('nan', 'None'):
            total_scanned += 1
            tot_share = 0
            
            for c in c_list.split('[')[1:]:
                parts = c.split(']')[0].split('^')
                if len(parts) >= 10:
                    b_no = parts[9].replace('-', '').strip()
                    # ★ 지역업체만의 지분율 합계를 구할 것인지, 아니면 전체 업체의 지분율 합계가 100%를 초과하는 데이터 오염을 찾을 것인지
                    # 여기서는 그냥 "해당 계약의 전체 지분율 합계가 100%를 초과하는지" 확인!
                    try: share = float(parts[6].strip())
                    except: share = 100.0
                    tot_share += share
            
            # 부동소수점 오차 감안 (100.1 초과 시 이상 감지)
            if tot_share > 100.1:
                over_100_cnt += 1
                if over_100_cnt <= 5: # 처음 5개만 샘플 출력
                    print(f"  🚨 이상 감지: {row['untyCntrctNo']} | {row['cntrctNm'][:20]}... | 합계 지분: {tot_share:.1f}%")
        
    print(f"  -> {table} 완료. 초과 건수: {over_100_cnt}건 (스캔된 다수급 계약: {total_scanned:,}건)")

check_over_100('cnstwk_cntrct')
check_over_100('servc_cntrct')
check_over_100('thng_cntrct')

conn.close()
