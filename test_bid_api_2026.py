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
    print('--- 1단계: 1월 계약건 중 랜덤 입찰공고 5개 필터링 ---')
    df = pd.read_excel('API_공사계약조회_전체(20260101_20260131).xlsx')
    
    # ntceNo(입찰공고번호)가 존재하는 건 필터링
    df_with_ntce = df.dropna(subset=['ntceNo'])
    print(f"입찰공고번호 보유 건수: {len(df_with_ntce)}")
    
    sample_df = df_with_ntce.sample(n=5, random_state=123)
    
    success = 0
    fail = 0
    
    print('\n--- 2단계: 입찰공고 API 매칭 스캔 ---')
    for idx, row in sample_df.iterrows():
        ntce_no_full = str(row['ntceNo']).strip()
        cntrct_no = str(row['dcsnCntrctNo'])
        wk_nm = row.get('cnstwkNm', 'N/A')
        
        # 번호 뒷자리 분리: 20260123456-00
        bid_no = ntce_no_full.split('-')[0] if '-' in ntce_no_full else ntce_no_full
        
        # 1. 앞 6자리로 연월(YYYYMM) 추출하여 날짜 범위 1개월 생성
        if len(bid_no) >= 6 and bid_no[:6].isdigit():
            yyyymm = bid_no[:6]
        else:
            yyyymm = '202601' # default fallback
            
        bgn = f"{yyyymm}010000"
        end = f"{yyyymm}312359"
            
        params = f'?serviceKey={service_key}&inqryDiv=1&inqryBgnDt={bgn}&inqryEndDt={end}&bidNtceNo={bid_no}&numOfRows=10&pageNo=1&type=json'
        
        req = urllib.request.Request(base_url + params, headers={'User-Agent': 'Mozilla/5.0'})
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
                resp_text = response.read().decode('utf-8')
                data = json.loads(resp_text)
                header = data.get('response', {}).get('header', {})
                if header.get('resultCode') == '00':
                    items = data.get('response', {}).get('body', {}).get('items', [])
                    if items:
                        # Find matching sub-order if any
                        rgn_nm = items[0].get('cnstrtsiteRgnNm', 'N/A')
                        print(f"✅ [SUCCESS] 공고번호: {bid_no}")
                        print(f"   ├─ 공사명: {wk_nm}")
                        print(f"   └─ 리턴된 현장지역: {rgn_nm}\n")
                        success += 1
                    else:
                        print(f"❌ [EMPTY] 공고번호 {bid_no}: 검색결과 없음")
                        fail += 1
                else:
                    msg = header.get('resultMsg', 'Unknown')
                    print(f"⚠️ [API ERROR] 공고번호 {bid_no}: {msg}")
                    fail += 1
        except Exception as api_e:
            print(f"⚠️ [HTTP 오류] {bid_no}: {api_e}")
            fail += 1
            
        time.sleep(0.5)

    print(f"결과 통계: 성공 {success}건, 실패/없음 {fail}건")

except Exception as e:
    import traceback
    traceback.print_exc()
