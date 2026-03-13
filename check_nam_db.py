import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

# 1. 기관 마스터에서 남구 관련 코드 추출
conn_ag = sqlite3.connect('busan_agencies_master.db')
df_ag = pd.read_sql("SELECT dminsttCd, dminsttNm FROM agency_master WHERE dminsttNm LIKE '%부산광역시 남구%' OR dminsttNm LIKE '%부산광역시남구%'", conn_ag)
conn_ag.close()
target_codes = set(df_ag['dminsttCd'].astype(str).str.strip())

print(f"✅ 남구 관련 기관코드 {len(target_codes)}개 탐색 완료.")

# 2. 물품 계약 데이터 로드 (2026년 1월)
conn_pr = sqlite3.connect('procurement_contracts.db')
df_db = pd.read_sql("SELECT untyCntrctNo, cntrctNm, cntrctRefNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt, corpList, dminsttList FROM thng_cntrct WHERE cntrctDate >= '2026-01-01' AND cntrctDate <= '2026-01-31'", conn_pr)
conn_pr.close()

# 중복 제거 로직 적용
df_db.drop_duplicates(subset=['cntrctRefNo', 'totCntrctAmt'], keep='last', inplace=True)

def extract_dminstt_codes(dminstt_list_str):
    codes = []
    if not dminstt_list_str or str(dminstt_list_str) in ('nan', 'None', ''): return codes
    for chunk in str(dminstt_list_str).split('[')[1:]:
        parts = chunk.split(']')[0].split('^')
        if len(parts) >= 2: codes.append(str(parts[1]).strip())
    return codes

# 3. 수요기관 판별
df_db['target_cd'] = df_db['cntrctInsttCd'].astype(str).str.strip()
mask_direct = df_db['target_cd'].isin(target_codes)
mask_dminstt = df_db['dminsttList'].apply(lambda x: any(cd in target_codes for cd in extract_dminstt_codes(x)))

df_nam = df_db[mask_direct | mask_dminstt]

# 4. 결과 출력
print("\n==========================================================================")
print(" 🏢 부산광역시 남구 [물품] 계약 26년 1월치 전체 조회 결과")
print("==========================================================================\n")

total_amt = 0
for _, row in df_nam.iterrows():
    amt = float(row['totCntrctAmt'])
    if pd.isna(amt) or amt == 0: amt = float(row['thtmCntrctAmt'])
    if pd.isna(amt): amt = 0
    total_amt += amt
    
    # 참조번호 존재 여부에 따라 자체조달(TA) 여부 표시
    ref_no = str(row['cntrctRefNo'])
    is_local_발주 = 'TA' in ref_no or 'CA' in ref_no or 'SA' in ref_no
    badge = "[자체]" if is_local_발주 else "[중앙]"
    
    print(f" {badge} {row['untyCntrctNo']} (참조: {ref_no}) | {row['cntrctNm'][:20]}... | {amt:,.0f}원")

print("--------------------------------------------------------------------------")
print(f" 🎯 최종 집계: 총 {len(df_nam)}건 계약, 금액 합계 {total_amt:,.0f}원")
print("==========================================================================\n")
