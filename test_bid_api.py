import urllib.request
import json
import ssl
import pandas as pd
import sys
import time

sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

service_key = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'
base_url = 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwk'

try:
    print('--- 1단계: API 데이터 로드 및 ntceNo 샘플 추출 ---')
    df = pd.read_excel('API_공사계약조회_전체(20260101_20260131).xlsx')
    
    # ntceNo(입찰공고번호)가 존재하는 건 필터링
    df_with_ntce = df.dropna(subset=['ntceNo'])
    print(f"전체 계약 건 중 입찰공고번호가 있는 건수: {len(df_with_ntce)}")
    
    # 5건 랜덤 샘플링
    sample_df = df_with_ntce.sample(n=5, random_state=42)
    
    success_count = 0
    fail_count = 0
    
    print('\n--- 2단계: 입찰공고 API 개별 호출 테스트 ---')
    for idx, row in sample_df.iterrows():
        ntce_no_full = str(row['ntceNo']).strip()
        cntrct_no = str(row['dcsnCntrctNo'])
        
        if '-' in ntce_no_full:
            # ex: 20260100001-00
            bid_no = ntce_no_full.split('-')[0]
        else:
            bid_no = ntce_no_full
            
        params = f'?serviceKey={service_key}&inqryDiv=1&inqryBgnDt=202001010000&inqryEndDt=203012312359&bidNtceNo={bid_no}&numOfRows=10&pageNo=1&type=json'
        # Some APIs don't need dates if bidNtceNo is provided. Let's try without dates first, or with just bidNtceNo.
        params2 = f'?serviceKey={service_key}&inqryDiv=1&bidNtceNo={bid_no}&numOfRows=10&pageNo=1&type=json'
        
        req = urllib.request.Request(base_url + params2, headers={'User-Agent': 'Mozilla/5.0'})
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
                resp_text = response.read().decode('utf-8')
                data = json.loads(resp_text)
                
                header = data.get('response', {}).get('header', {})
                if header.get('resultCode') == '00':
                    items = data.get('response', {}).get('body', {}).get('items', [])
                    if items:
                        rgn_nm = items[0].get('cnstrtsiteRgnNm', 'N/A')
                        print(f"[SUCCESS] 계약번호: {cntrct_no} | 공고번호: {bid_no} -> 공사현장: {rgn_nm}")
                        success_count += 1
                    else:
                        print(f"[EMPTY] 공고번호 {bid_no}: 검색결과 0건 (totalCount=0)")
                        fail_count += 1
                else:
                    msg = header.get('resultMsg', 'UnknownError')
                    print(f"[ERROR] 공고번호 {bid_no}: API 실패 ({msg})")
                    fail_count += 1
                    
        except Exception as api_e:
            print(f"[HTTP/JSON Exception] {api_e}")
            fail_count += 1
            
        time.sleep(0.5)
        
    print(f"\n최종 결과: 성공 {success_count}건, 실패/없음 {fail_count}건")

except Exception as e:
    import traceback
    traceback.print_exc()
