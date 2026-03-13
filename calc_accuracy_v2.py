import pandas as pd
import sys
import re

sys.stdout.reconfigure(encoding='utf-8')
import warnings
warnings.filterwarnings('ignore', category=UserWarning)

try:
    print('--- 1단계: 데이터 로드 및 조인 (키값 보정) ---')
    api_df = pd.read_excel('API_공사계약조회_전체(20260101_20260131).xlsx')
    
    # 1. API: dcsnCntrctNo (예: R25TA0117392900)에서 앞 13자리만 잘라서 순수 계약번호로 치환
    api_df['cntrctRefNo'] = api_df['dcsnCntrctNo'].astype(str).str[:13]
    
    # 부산지방국토관리청 발주 건만 대상으로 텍스트 마이닝 검증 (허수가 집중되는 곳)
    api_busan = api_df[api_df['cntrctInsttNm'].astype(str).str.contains('국토관리청', na=False)]
    
    # 2. 공사현장 마스터 (현장=부산광역시로 다운받은 파일)
    loc_file = '2026 공사 현장 공사 공동수급 계약 내역.xlsx'
    df_f = pd.read_excel(loc_file, header=None)
    start_row = 0
    for i, r in df_f.iterrows():
        if '계약번호' in str(r.values) and '조회번호' not in str(r.values):
            start_row = i
            break
            
    loc_df = pd.read_excel(loc_file, skiprows=start_row)
    loc_df['계약번호'] = loc_df['계약번호'].astype(str).str.strip().str[:13]
    loc_df['is_busan_site'] = True  # 이 엑셀에 있으면 공사현장이 부산임
    
    # Left Join: API 데이터(모집단) 기준으로 붙여서, 매칭안되면 '관외 공사'
    merged = pd.merge(api_busan, loc_df[['계약번호', 'is_busan_site']], left_on='cntrctRefNo', right_on='계약번호', how='left')
    merged = merged.drop_duplicates(subset=['dcsnCntrctNo'])
    
    # Ground Truth 정의
    merged['is_busan_site'] = merged['is_busan_site'].fillna(False) # 매칭 안된건 다른 지역
    merged['is_outside_truth'] = ~merged['is_busan_site'] # 관외가 True
    
    print(f"테스트 대상: 국토관리청 발주 공사 총 {len(merged)}건")
    print(f"- [실제] 공사현장이 관외(True)인 건수: {merged['is_outside_truth'].sum()} 건")
    print(f"- [실제] 공사현장이 부산(False)인 건수: {(~merged['is_outside_truth']).sum()} 건")
    
    print('\n--- 2단계: 텍스트 마이닝 예측 ---')
    keywords = '진주|대구|창원|경남|울산|밀양|거제|포항|경북|김해|양산'
    merged['is_outside_pred'] = merged['cnstwkNm'].astype(str).str.contains(keywords, regex=True, na=False)
    
    t_p = len(merged[(merged['is_outside_truth'] == True) & (merged['is_outside_pred'] == True)])
    f_p = len(merged[(merged['is_outside_truth'] == False) & (merged['is_outside_pred'] == True)])
    f_n = len(merged[(merged['is_outside_truth'] == True) & (merged['is_outside_pred'] == False)])
    t_n = len(merged[(merged['is_outside_truth'] == False) & (merged['is_outside_pred'] == False)])
    
    total = len(merged)
    accuracy = (t_p + t_n) / total * 100 if total > 0 else 0
    recall = t_p / (t_p + f_n) * 100 if (t_p + f_n) > 0 else 0
    precision = t_p / (t_p + f_p) * 100 if (t_p + f_p) > 0 else 0
    
    print(f"\n[ 텍스트 마이닝 매트릭스 ]")
    print(f"정답 적중 (True Positive): {t_p} 건 (실제 관외를 관외로 잘 잡아냄)")
    print(f"오발탄 (False Positive): {f_p} 건 (실제 부산인데 관외로 잘못 잡음)")
    print(f"누락 (False Negative): {f_n} 건 (실제 관외인데 못 잡아내고 통과됨)")
    
    print(f"\n[ 🎯 핵심 KPI ]")
    print(f"일치율(Accuracy): {accuracy:.2f}%")
    if (t_p+f_n) > 0: print(f"재현율(Recall - 실제 허수를 솎아낸 비율): {recall:.2f}%")
    if (t_p+f_p) > 0: print(f"정밀도(Precision - 적발한 허수가 진짜인가?): {precision:.2f}%")

    if f_n > 0:
        print("\n--- [텍스트 마이닝이 놓친 관외 공사 샘플 (False Negatives)] ---")
        missed = merged[(merged['is_outside_truth'] == True) & (merged['is_outside_pred'] == False)]
        for idx, row in missed.head(5).iterrows():
            print(f"계약번호: {row['cntrctRefNo']} | 공사명: {row['cnstwkNm']}")
            
except Exception as e:
    import traceback
    traceback.print_exc()
