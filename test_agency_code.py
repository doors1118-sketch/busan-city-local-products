import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

try:
    # 1. Load API Excel Data
    df = pd.read_excel('API_공사계약조회_전체(20260101_20260131).xlsx')
    
    # Extract '부산지방국토관리청' contracts
    df_busan_gukto = df[df['cntrctInsttNm'].astype(str).str.contains('국토관리청', na=False)]
    
    print('--- 관외 사무소의 기관명 및 기관코드(cntrctInsttCd) 확인 ---')
    found_rows = []
    for ext_region in ['진주', '포항']:
        matches = df_busan_gukto[df_busan_gukto['cnstwkNm'].astype(str).str.contains(ext_region)]
        for idx, row in matches.iterrows():
            print(f"[{row['cntrctInsttNm']}] 기관코드: {row['cntrctInsttCd']} | 공사명: {row['cnstwkNm']}")
            found_rows.append(row)
            
    print('\n--- 마스터파일에 해당 기관코드가 존재하는지 대조 ---')
    try:
        master_df = pd.read_csv('부산광역시 조달 수요기관 마스터파일_최종본.csv', encoding='utf-8')
    except UnicodeDecodeError:
        master_df = pd.read_csv('부산광역시 조달 수요기관 마스터파일_최종본.csv', encoding='cp949')
    master_codes = set(master_df['수요기관코드'].astype(str).str.strip().tolist())
    
    for row in found_rows:
        cd = str(row['cntrctInsttCd']).strip()
        is_in_master = cd in master_codes
        print(f"기관명: {row['cntrctInsttNm']}, 기관코드: {cd} -> 마스터파일 존재 여부: {is_in_master}")
        
    print('\n--- [참고] 마스터파일 내 "국토관리청" 검색 ---')
    master_gukto = master_df[master_df['수요기관명'].astype(str).str.contains('국토관리청', na=False)]
    for idx, row in master_gukto.iterrows():
        print(f"마스터 존재 기관명: {row['수요기관명']}, 기관코드: {row['수요기관코드']}")
        
except Exception as e:
    import traceback
    traceback.print_exc()
