import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

try:
    f_api = 'API_용역계약초고속_전체(20260101_20260131).xlsx'
    f_man = '2026.1월 용역 계약업체 내역.csv'

    print('1. Loading API Service Data...')
    df_api = pd.read_excel(f_api)
    api_dcsn = set(df_api['dcsnCntrctNo'].dropna().astype(str).str[:13].str.upper())
    
    print('2. Loading Manual CSV Service Data (UTF-16)...')
    df_man = pd.read_csv(f_man, encoding='utf-16', sep='\t', skiprows=67, low_memory=False)
    print(f'   - Manual Columns: {len(df_man.columns)} columns found.')
    
    # Heuristic matching of best column for manual data
    best_match = (None, 0)
    for col in df_man.columns:
        col_set = set(df_man[col].dropna().astype(str).str.strip().str.upper())
        intersection_len = len(api_dcsn.intersection(col_set))
        if intersection_len > best_match[1]:
            best_match = (col, intersection_len)
            
    print(f'\n3. Cross Validation Results')
    print(f'   - API Row Count      : {len(df_api):,}')
    print(f'   - Manual Row Count   : {len(df_man):,}')
    print(f'   - API Unique Keys    : {len(api_dcsn):,}')
    print(f'   - Best Match Column in Manual Data: {best_match[0]}')
    print(f'   - 🌟 [일치율] : {best_match[1]:,} 건의 계약 교집합 (Join) 성공!')

except Exception as e:
    import traceback
    traceback.print_exc()
