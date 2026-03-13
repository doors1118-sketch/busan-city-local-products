import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

# Load the API Excel
try:
    df = pd.read_excel('API_공사계약조회_전체(20260101_20260131).xlsx')
    
    # Extract contracts from '부산지방국토관리청' and similar Busan branches
    df_busan_gukto = df[df['cntrctInsttNm'].astype(str).str.contains('국토관리청', na=False)]
    
    print(f"======================================")
    print(f" 🔎 [관외 공사 필터링 텍스트 마이닝 시뮬레이션]")
    print(f"======================================\n")
    print(f"'부산지방국토관리청' 발주 공사 총 {len(df_busan_gukto)}건 중 타지역 현장(공사명 기준) 조회\n")
    
    outside_keywords = ['진주', '대구', '창원', '경남', '울산', '밀양', '거제', '포항', '경북']
    
    total_found = 0
    for ext_region in outside_keywords:
        matches = df_busan_gukto[df_busan_gukto['cnstwkNm'].astype(str).str.contains(ext_region)]
        if len(matches) > 0:
            print(f"--- 🚨 관외 키워드 감지: '{ext_region}' ({len(matches)} 건 발견) ---")
            total_found += len(matches)
            for idx, row in matches.head(3).iterrows():
                print(f"   [발주처: {row['cntrctInsttNm']}]")
                print(f"   [공사명: {row['cnstwkNm']}]")
                print(f"   [계약금액: {row.get('totCntrctAmt', 0):,} 원]\n")

    print(f"결론: 공사명(cnstwkNm) 텍스트 마이닝을 통해 총 {total_found}건의 '부산기관 발주 - 관외 현장 공사' 허수를 성공적으로 적발.")
except Exception as e:
    import traceback
    traceback.print_exc()
