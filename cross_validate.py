import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

try:
    f1 = 'API_공사계약조회_전체(20260101_20260131).xlsx'
    f2 = '260101 공사 공동수급 계약 내역.xlsx'
    
    print('Loading API data...')
    df_api = pd.read_excel(f1)
    
    print('Loading manual data...')
    df_man = pd.read_excel(f2) 
    
    # 15번째 컬럼(Index 14)이 계약번호임 ('Unnamed: 14' 등)
    man_cntrct_col = df_man.columns[14]
    
    print(f'Match Key Mapping -> API: untyCntrctNo vs Manual: {man_cntrct_col}')
    
    # Clean strings
    api_cntrct_nos = set(df_api['untyCntrctNo'].astype(str).str.strip().str.upper())
    
    # Manual data contract nos
    df_man[man_cntrct_col] = df_man[man_cntrct_col].astype(str).str.strip().str.upper()
    man_cntrct_nos = set(df_man[man_cntrct_col])
    
    intersection = api_cntrct_nos.intersection(man_cntrct_nos)
    
    print(f'\n[Data Row Count vs Unique Identifier Count]')
    print(f'API Row Count   : {len(df_api)}')
    print(f'Manual Row Count: {len(df_man)}')
    print(f'API Unique Keys    : {len(api_cntrct_nos)}')
    print(f'Manual Unique Keys : {len(man_cntrct_nos)}')
    
    print(f'\n[Intersection Check]')
    print(f'Common Contracts : {len(intersection)}')
    print(f'Only in API      : {len(api_cntrct_nos - man_cntrct_nos)}')
    print(f'Only in Manual   : {len(man_cntrct_nos - api_cntrct_nos)}')
    
    if len(api_cntrct_nos - man_cntrct_nos) > 0:
        sample_api_only = list(api_cntrct_nos - man_cntrct_nos)[:3]
        print(f'\nSample keys (Only API): {sample_api_only}')
        
    if len(man_cntrct_nos - api_cntrct_nos) > 0:
        sample_man_only = list(man_cntrct_nos - api_cntrct_nos)[:3]
        print(f'Sample keys (Only Man): {sample_man_only}')
        
    # How many rows in Manual file actually match the intersection?
    man_matched = df_man[df_man[man_cntrct_col].isin(intersection)]
    print(f'\n[Coverage]')
    print(f'Manual File Rows matched with API: {len(man_matched)} / {len(df_man)} ({(len(man_matched)/len(df_man))*100:.1f}%)')
    
except Exception as e:
    import traceback
    traceback.print_exc()
