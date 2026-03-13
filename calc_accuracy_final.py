import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')
import warnings
warnings.filterwarnings('ignore', category=UserWarning)

try:
    print('--- 1. 데이터 로드 및 전처리 ---')
    # 1. API 데이터 (1월 전체)
    api_df = pd.read_excel('API_공사계약조회_전체(20260101_20260131).xlsx')
    api_df['cntrctRefNo'] = api_df['dcsnCntrctNo'].astype(str).str[:13]
    
    # 2. 공사현장 마스터 ('공동수급', '공사현장=부산' 조건으로 다운로드 받은 파일)
    loc_file = '2026 공사 현장 공사 공동수급 계약 내역.xlsx'
    df_f = pd.read_excel(loc_file, header=None)
    start_row = 0
    for i, r in df_f.iterrows():
        if '계약번호' in str(r.values) and '조회번호' not in str(r.values):
            start_row = i
            break
    loc_df = pd.read_excel(loc_file, skiprows=start_row)
    loc_df['계약번호'] = loc_df['계약번호'].astype(str).str.strip().str[:13]
    
    # 교집합(부산 현장) 계약번호 Set
    busan_site_cntrct_nos = set(loc_df['계약번호'])
    
    # 분석 대상 필터링 (완벽한 Ground Truth를 위해 API 데이터 중 '공동수급' 계약만 발췌)
    # cmmnCntrctYn == 'Y' 이면 공동수급
    api_cmmn = api_df[api_df['cmmnCntrctYn'] == 'Y'].copy()
    
    # 여기서 수요기관이 부산인 건들만 추출하여 사용자 파이프라인과 동일한 모수(Target) 생성
    # (실제 파이프라인에서 수요기관 주소가 부산인 것만 1차 필터링 하므로)
    api_target = api_cmmn[api_cmmn['cntrctInsttNm'].astype(str).str.contains('포항|진주|대구|경남|거제|김해|울산|창원|밀양|양산|국토관리청|경북|부산', na=False)].copy()
    
    if len(api_target) == 0:
        api_target = api_cmmn.copy() # Falls back to all joint ventures if no match
    
    print(f"평가 대상: 1월 API 데이터 중 공동수급 계약 총 {len(api_target)}건")
    
    # --- 2. Ground Truth 라벨링 ---
    # 다운로드 받은 엑셀(loc_df)에 계약번호가 있으면 '부산 현장', 없으면 '관외(타지역) 현장'
    api_target['is_busan_truth'] = api_target['cntrctRefNo'].isin(busan_site_cntrct_nos)
    api_target['is_outside_truth'] = ~api_target['is_busan_truth']
    
    # --- 3. 텍스트 마이닝 (예측 로직) ---
    keywords = '진주|대구|창원|경남|울산|밀양|거제|포항|경북|김해|양산|함안|사천|통영|합천|거창|하동'
    api_target['is_outside_pred'] = api_target['cnstwkNm'].astype(str).str.contains(keywords, regex=True, na=False)
    
    # --- 4. 통계 계산 (혼동 행렬) ---
    t_p = len(api_target[(api_target['is_outside_truth'] == True) & (api_target['is_outside_pred'] == True)])
    f_p = len(api_target[(api_target['is_outside_truth'] == False) & (api_target['is_outside_pred'] == True)])
    f_n = len(api_target[(api_target['is_outside_truth'] == True) & (api_target['is_outside_pred'] == False)])
    t_n = len(api_target[(api_target['is_outside_truth'] == False) & (api_target['is_outside_pred'] == False)])
    
    total = len(api_target)
    accuracy = (t_p + t_n) / total * 100 if total > 0 else 0
    recall = t_p / (t_p + f_n) * 100 if (t_p + f_n) > 0 else 0
    precision = t_p / (t_p + f_p) * 100 if (t_p + f_p) > 0 else 0
    
    print("\n--- 5. 텍스트 마이닝 매칭 결과 ---")
    print(f"[전체 모집단] API 공동수급 계약: {total}건")
    print(f"  - 실제 부산 공사(Truth): {(~api_target['is_outside_truth']).sum()}건")
    print(f"  - 실제 관외 공사(Truth): {api_target['is_outside_truth'].sum()}건")
    
    print("\n[매칭 매트릭스]")
    print(f"  - 정답 적중(TP): {t_p}건 (실제 관외를 마이닝으로 걸러냄)")
    print(f"  - 오발탄(FP)  : {f_p}건 (부산 공사인데 이름때문에 관외로 오해함)")
    print(f"  - 누락(FN)    : {f_n}건 (관외 공사인데 이름에 지역명이 없어 통과됨)")
    print(f"  - 정상 통과(TN): {t_n}건 (실제 부산 공사를 부산으로 잘 인정함)")
    
    print("\n[최종 성능 지표 KPI]")
    print(f"🎯 일치율(Accuracy) : {accuracy:.2f}% (전체 판단의 정확도)")
    if (t_p + f_n) > 0: print(f"🎯 재현율(Recall)   : {recall:.2f}% (실제 허수 중 찾아낸 비율)")
    if (t_p + f_p) > 0: print(f"🎯 정밀도(Precision): {precision:.2f}% (적발한 허수 중 진짜 허수의 비율)")
    
except Exception as e:
    import traceback
    traceback.print_exc()
