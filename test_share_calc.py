import sqlite3
import pandas as pd
import json

DB_CONTRACTS = 'procurement_contracts.db'
DB_AGENCIES = 'busan_agencies_master.db'

def test_share_calculation():
    print("=== [테스트] 부산 발주 공사/용역/물품 계약 지분율 연산 로직 ===\n")
    
    # 1. 부산 수요기관 마스터 로드
    conn_ag = sqlite3.connect(DB_AGENCIES)
    df_agencies = pd.read_sql("SELECT dminsttCd, dminsttNm FROM agency_master", conn_ag)
    busan_agency_codes = set(df_agencies['dminsttCd'])
    conn_ag.close()
    
    print(f"1) 부산광역시 관내 (수요기관 DB) 기준 코드 개수: {len(busan_agency_codes)}개\n")
    
    conn_ct = sqlite3.connect(DB_CONTRACTS)
    
    # 공사 계약 (cnstwk_cntrct) 샘플 추출 (어제 날짜: 2026.03.03 기준)
    df_cnstwk = pd.read_sql("SELECT * FROM cnstwk_cntrct WHERE cntrctDate = '2026-03-03'", conn_ct)
    print(f"2) 2026.03.03 전국 공사 계약 총 건수: {len(df_cnstwk)}")
    
    # 부산 수요기관이 발주한 공사만 필터링 (cntrctInsttCd 체크 및 dminsttList 텍스트 파싱)
    def is_busan_agency(row):
        cntrct_cd = str(row.get('cntrctInsttCd', '')).strip()
        if cntrct_cd in busan_agency_codes:
            return True
        
        # dminsttList 문자열 안에 부산 기관 코드가 존재하는지 확인
        dminstt_str = str(row.get('dminsttList', ''))
        for code in busan_agency_codes:
            if code in dminstt_str:
                return True
        return False
        
    mask = df_cnstwk.apply(is_busan_agency, axis=1)
    df_busan_cnstwk = df_cnstwk[mask]
    
    print(f"   -> 그 중 '부산' 발주 공사 계약 건수: {len(df_busan_cnstwk)}건\n")
    
    if not df_busan_cnstwk.empty:
        print("3) [부산 공사발주 건] 공동도급(corpList) 파싱 및 지역업체 안분 테스트:")
        # 공동 도급 건(캐럿 기호 여러 번 등장) 또는 일반 건 확인
        joint_ventures = df_busan_cnstwk[df_busan_cnstwk['corpList'].str.count('\\^') > 10]
        
        test_samples = joint_ventures if not joint_ventures.empty else df_busan_cnstwk
        
        for idx, row in test_samples.head(3).iterrows():
            contract_no = row['untyCntrctNo']
            cntrct_amt = float(row.get('totCntrctAmt', 0))
            corp_list_str = row.get('corpList', '')
            
            print(f"\n  ■ 계약번호: {contract_no} (총 계약금: {cntrct_amt:,.0f}원)")
            print(f"    - 발주처: {row.get('dmndInsttNm')} / {row.get('cntrctInsttNm')}")
            print(f"    - 공사명: {row.get('cnstwkNm')}")
            
            if not corp_list_str or corp_list_str == 'nan':
                continue
                
            print(f"    [참여 업체(corpList) 파싱 내역]")
            try:
                # `corpList` 파싱: "['1^업체...^사업자', '2^업체...^사업자']" 형태의 문자열 처리
                clean_str = corp_list_str.strip()
                if clean_str.startswith('[') and clean_str.endswith(']'):
                    clean_str = clean_str[1:-1] # 대괄호 제거
                    
                # 각 항목은 홑따옴표(')로 감싸져 있고 쉼표(,)로 구분될 수 있음
                # 안전하게 split 후 홑따옴표 제거
                items_raw = [x.strip() for x in clean_str.split("',")]
                
                corp_data = []
                for item in items_raw:
                    # 시작/끝 홑따옴표 및 공백 제거
                    cleaned_item = item.strip("' \"")
                    if cleaned_item:
                        corp_data.append(cleaned_item)
                        
                total_share = 0.0
                for corp_item in corp_data:
                    parts = corp_item.split('^')
                    if len(parts) >= 8:
                        corp_name = parts[3]
                        # 6번째 인덱스가 지분율(%)
                        share_percent_str = parts[6]
                        biz_no = parts[-1]
                        
                        try:
                            share_pct = float(share_percent_str)
                        except ValueError:
                            share_pct = 100.0 if "단독" in parts[2] else 0.0
                            
                        # 단독 도급이거나, 퍼센트 정보가 누락된 경우 총계약액 100%
                        if share_pct == 0 and len(corp_data) == 1:
                            share_pct = 100.0
                            
                        calc_amt = cntrct_amt * (share_pct / 100.0)
                        total_share += share_pct
                        
                        print(f"      * 업체명: {corp_name} (사업자: {biz_no}) -> 지분율: {share_pct}% | 인정 실적: {calc_amt:,.0f}원")
                    else:
                        print(f"      * [Parsing Fail] 쪼개진 필드 수 부족: {len(parts)}개 -> 원본: {corp_item}")
                        
                print(f"      => 파싱된 총합 지분율: {total_share}%")
                if abs(total_share - 100.0) > 1.0:
                    print("      [경고] 합계 지분율이 100%와 크게 다릅니다. 추가 로직 보정이 필요할 수 있습니다.")
                    
            except Exception as e:
                print(f"    ! Error parsing corpList: {e}")

    conn_ct.close()

if __name__ == '__main__':
    test_share_calculation()
