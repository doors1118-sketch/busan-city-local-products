import pandas as pd
import sys
import re

sys.stdout.reconfigure(encoding='utf-8')
import warnings
warnings.filterwarnings('ignore', category=UserWarning)

try:
    print('--- 1단계: 데이터 로드 및 조인 ---')
    # 1. API 데이터 
    api_df = pd.read_excel('API_공사계약조회_전체(20260101_20260131).xlsx')
    api_df['dcsnCntrctNo'] = api_df['dcsnCntrctNo'].astype(str).str.strip()
    api_busan = api_df[api_df['cntrctInsttNm'].astype(str).str.contains('포항|진주|대구|창원|밀양|거제|김해|울산|경남|경북', na=False)] # To increase matches if needed, but let's just use all
    
    # 2. 공사현장 마스터 (1.1 ~ 3.3)
    loc_file = '2026 공사 현장 공사 공동수급 계약 내역.xlsx'
    
    # Just to be safe with name
    df_f = pd.read_excel(loc_file, header=None)
    start_row = 0
    for i, r in df_f.iterrows():
        if '계약번호' in str(r.values) and '조회번호' not in str(r.values):
            start_row = i
            break
            
    loc_df = pd.read_excel(loc_file, skiprows=start_row)
    loc_df['계약번호'] = loc_df['계약번호'].astype(str).str.strip()
    
    # Join!
    merged = pd.merge(api_df, loc_df, left_on='dcsnCntrctNo', right_on='계약번호', how='inner')
    print(f"API 데이터(1월)와 공사현장 엑셀 조인 성공: 총 {len(merged)}건 일치 (교집합)")
    
    # Drop duplicates if any inner join exploded
    merged = merged.drop_duplicates(subset=['dcsnCntrctNo'])
    print(f"중복 제거 후 순수 계약 건수: {len(merged)}건")
    
    print('\n--- 2단계: 크로스 체크 (Ground Truth vs Text Mining) ---')
    
    # Ground Truth: 실제 공사현장이 '부산광역시'가 아닌 경우 (관외 허수)
    # Excel Column could be '현장지역' or '현장지역시도', let's check
    loc_col = '현장지역시도' if '현장지역시도' in merged.columns else '현장지역' if '현장지역' in merged.columns else None
    
    if loc_col is None:
        print("공사현장 컬럼을 찾을 수 없습니다:", merged.columns.tolist())
        sys.exit(1)
        
    # True 관외 (허수): 부산이 아님
    merged['is_outside_truth'] = ~merged[loc_col].astype(str).str.contains('부산', na=False)
    
    # Text Mining Prediction: 공사명에 관외 키워드가 있는지
    keywords = '진주|대구|창원|경남|울산|밀양|거제|포항|경북|김해|양산'
    merged['is_outside_pred'] = merged['cnstwkNm'].astype(str).str.contains(keywords, regex=True, na=False)
    
    true_positives = len(merged[(merged['is_outside_truth'] == True) & (merged['is_outside_pred'] == True)])
    false_positives = len(merged[(merged['is_outside_truth'] == False) & (merged['is_outside_pred'] == True)])
    false_negatives = len(merged[(merged['is_outside_truth'] == True) & (merged['is_outside_pred'] == False)])
    true_negatives = len(merged[(merged['is_outside_truth'] == False) & (merged['is_outside_pred'] == False)])
    
    total = len(merged)
    accuracy = (true_positives + true_negatives) / total * 100 if total > 0 else 0
    recall = true_positives / (true_positives + false_negatives) * 100 if (true_positives + false_negatives) > 0 else 0
    precision = true_positives / (true_positives + false_positives) * 100 if (true_positives + false_positives) > 0 else 0
    
    print(f"\n[ 검증 결과 통계 ]")
    print(f"총 분석 모수: {total} 건")
    print(f"- 실제 관외 공사 (허수, Ground Truth): {true_positives + false_negatives} 건")
    print(f"- 텍스트 마이닝이 관외로 적발한 건수: {true_positives + false_positives} 건")
    print(f"\n[ 상세 매트릭스 ]")
    print(f"정답 적중 (True Positive): {true_positives} 건 (실제 관외인데 관외로 잘 잡아냄)")
    print(f"오발탄 (False Positive): {false_positives} 건 (실제 부산인데 관외로 잘못 잡음)")
    print(f"누락 (False Negative): {false_negatives} 건 (실제 관외인데 못 잡아냄)")
    
    print(f"\n[ KPI 지표 ]")
    print(f"🎯 일치율(Accuracy): {accuracy:.2f}%")
    print(f"🎯 재현율(Recall - 실제 허수를 얼마나 잡아냈는가?): {recall:.2f}%")
    print(f"🎯 정밀도(Precision - 적발한게 진짜 허수가 맞는가?): {precision:.2f}%")
    
    if false_negatives > 0:
        print("\n--- [텍스트 마이닝이 놓친 관외 공사 샘플 (False Negatives)] ---")
        missed = merged[(merged['is_outside_truth'] == True) & (merged['is_outside_pred'] == False)]
        for idx, row in missed.head(5).iterrows():
            print(f"실제현장: {row[loc_col]} | 공사명: {row['cnstwkNm']}")
            
    if false_positives > 0:
        print("\n--- [텍스트 마이닝이 부산인데 잘못 지운 공사 샘플 (False Positives)] ---")
        wrong = merged[(merged['is_outside_truth'] == False) & (merged['is_outside_pred'] == True)]
        for idx, row in wrong.head(5).iterrows():
            print(f"실제현장: {row[loc_col]} | 공사명: {row['cnstwkNm']}")

except Exception as e:
    import traceback
    traceback.print_exc()
