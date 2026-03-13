import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'procurement_contracts.db'
AGENCY_DB = 'busan_agencies_master.db'
COMPANY_DB = 'busan_companies_master.db'

def test_location_join():
    print("================================================================")
    print(" 🔎 [공사 계약 현장위치 정밀 필터링] 부산 발주처 & 타지역 공사 적발")
    print("================================================================")
    
    # 1. 마스터 DB (수요기관, 조달업체) 로딩
    conn_ag = sqlite3.connect(AGENCY_DB)
    busan_agencies = set(pd.read_sql("SELECT dminsttCd FROM agency_master", conn_ag)['dminsttCd'].dropna().astype(str).str.strip())
    conn_ag.close()
    
    conn_co = sqlite3.connect(COMPANY_DB)
    busan_companies = set(pd.read_sql("SELECT bizno FROM company_master", conn_co)['bizno'].dropna().astype(str).str.replace('-', '').str.strip())
    conn_co.close()

    print(f"✅ 마스터 DB 로딩 완료: 부산 수요기관 {len(busan_agencies):,}개 / 부산 지역업체 {len(busan_companies):,}개")

    # 2. 계약 DB ↔ 입찰공고 DB(현장위치) JOIN
    conn = sqlite3.connect(DB_PATH)
    
    # 공사 계약(cnstwk_cntrct) 전체 테이블 구조 파악 (디버깅)
    try:
        # 실제 공사 테이블 이름은 cnstwk_cntrct 이므로 여기에서 데이터를 가져옵니다.
        # cnstrtsiteRgnNm (현장위치) 데이터는 입찰공고 DB(bid_notices_raw)에 있습니다.
        query = """
        SELECT 
            c.untyCntrctNo AS cntrctNo,
            c.cnstwkNm AS cntrctNm,
            c.cntrctInsttCd,
            c.cntrctInsttNm,
            c.totCntrctAmt,
            c.cntrctCnclsDate,
            c.corpList,
            c.ntceNo AS bidNtceNo,
            b.cnstrtsiteRgnNm AS 현장지역명
        FROM cnstwk_cntrct c
        LEFT JOIN bid_notices_raw b 
          ON c.ntceNo = b.bidNtceNo
        WHERE c.cntrctInsttCd != '' AND c.corpList != ''
        """
        
        df = pd.read_sql_query(query, conn)
        print(f"✅ 조인된 공사 계약 데이터 로딩: {len(df):,}건")
        
        # 3. 필터링 로직 구현
        # 조건 A: 발주처(수요기관)가 부산 마스터 DB에 속함
        df['is_busan_agency'] = df['cntrctInsttCd'].astype(str).str.strip().isin(busan_agencies)
        
        # 조건 B: 현장위치에 '부산'이 포함되어 있지 않음 (타지역 공사)
        # NaN 처리 및 문자열 확인
        df['site_location'] = df['현장지역명'].fillna('').astype(str)
        # 입찰공고 번호가 매핑안된 건은 일단 패스할지 여부 고민 -> 매핑된 건 중에서만 판별
        mapped_df = df[df['site_location'] != '']
        
        # 발주처는 부산인데, 공사현장은 타지역인 건 필터링
        anomaly_df = mapped_df[(mapped_df['is_busan_agency'] == True) & (~mapped_df['site_location'].str.contains('부산', na=False))]
        
        print(f"\n[분석 결과]")
        print(f"- 전체 공사 계약: {len(df):,}건")
        print(f"- 현장위치 매핑 성공: {len(mapped_df):,}건 (매핑률: {len(mapped_df)/len(df)*100:.1f}%)")
        print(f"- 발주처가 부산 지역인 공사(매핑 건 중): {mapped_df['is_busan_agency'].sum():,}건")
        print(f"🚨 [경고] 부산 발주처지만 타지역 공사인 건: {len(anomaly_df):,}건")
        
        if len(anomaly_df) > 0:
            print("\n🚨🚨 ------------------ [타지역 공사 상세 내역 TOP 10] ------------------ 🚨🚨")
            # 금액 순으로 정렬
            anomaly_df['totCntrctAmt'] = pd.to_numeric(anomaly_df['totCntrctAmt'], errors='coerce').fillna(0)
            anomaly_df = anomaly_df.sort_values(by='totCntrctAmt', ascending=False)
            
            for idx, row in anomaly_df.head(10).iterrows():
                corp_list_raw = str(row['corpList'])
                # 참여업체 분석 로직 (rate_calc와 유사)
                busan_participation = False
                corps = corp_list_raw.split('[')[1:]
                local_amt = 0
                for c in corps:
                    c = c.split(']')[0]
                    parts = c.split('^')
                    if len(parts) >= 10:
                        biz_no = str(parts[9]).replace('-', '').strip()
                        share_str = str(parts[6]).strip()
                        try:
                            share = float(share_str)
                        except:
                            share = 100.0
                        if biz_no in busan_companies:
                            busan_participation = True
                            local_amt += row['totCntrctAmt'] * (share / 100.0)
                
                print(f"📌 {row['cntrctNm']}")
                print(f"   - 계약번호: {row['cntrctNo']} ({row['cntrctCnclsDate']})")
                print(f"   - 수요기관: {row['cntrctInsttNm']} (코드: {row['cntrctInsttCd']})")
                print(f"   - 현장위치: [{row['site_location']}]  <-- OUTSIDE BUSAN!")
                print(f"   - 총계약금액: {row['totCntrctAmt']:,.0f}원")
                status = "O" if busan_participation else "X"
                amt_str = f"({local_amt:,.0f}원 할당)" if busan_participation else ""
                print(f"   - 부산 지역업체 수주 여부: {status} {amt_str}\n")
                
    except Exception as e:
        import traceback
        traceback.print_exc()
        
    finally:
        conn.close()

if __name__ == '__main__':
    test_location_join()
