import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

try:
    df = pd.read_excel('2026 공사 현장 공사 공동수급 계약 내역.xlsx')
    print('--- 공사 현장 부산 추출 엑셀 파일 컬럼 ---')
    print(df.columns.tolist())
    print('\n--- 데이터 샘플 (첫 3행) ---')
    
    # Check if '통합계약번호' or '계약번호' exists
    has_unty = '통합계약번호' in df.columns
    has_cntr = '계약번호' in df.columns
    
    for idx, row in df.head(3).iterrows():
        unty = row['통합계약번호'] if has_unty else 'N/A'
        cntr = row['계약번호'] if has_cntr else 'N/A'
        loc = row.get('공사현장', 'N/A')
        print(f"[{idx}] 계약번호: {cntr}, 통합계약번호: {unty}, 공사현장: {loc}")
        
    print(f'\n총 건수: {len(df)}')
except Exception as e:
    import traceback
    traceback.print_exc()
