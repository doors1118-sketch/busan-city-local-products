import os
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

service_key = os.environ.get('SERVICE_KEY', '')
base_url = 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkServcInfo'

try:
    print('--- 1월 공사계약 데이터 로드 ---')
    df = pd.read_excel('API_공사계약조회_전체(20260101_20260131).xlsx')
    
    # Filter for '부산지방국토관리청' (39 contracts)
    df_busan = df[df['cntrctInsttNm'].astype(str).str.contains('포항국토', na=False)]
    if df_busan.empty:
        df_busan = df[df['cntrctInsttNm'].astype(str).str.contains('진주국토', na=False)]
        
    print(f"테스트 대상 계약 건수: {len(df_busan)} 건")
    
    success_count = 0
    for idx, row in df_busan.head(3).iterrows():
        unty_no = row.get('untyCntrctNo')
        cntrct_no = row.get('cntrctNo', '')
        instt_nm = row.get('cntrctInsttNm', '')
        wk_nm = row.get('cnstwkNm', '')
        
        if pd.isna(unty_no):
            continue
            
        params = f'?serviceKey={service_key}&untyCntrctNo={unty_no}&numOfRows=10&pageNo=1&type=json'
        
        req = urllib.request.Request(base_url + params, headers={'User-Agent': 'Mozilla/5.0'})
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
                resp_text = response.read().decode('utf-8')
                data = json.loads(resp_text)
                header = data.get('response', {}).get('header', {})
                if header.get('resultCode') == '00':
                    items = data.get('response', {}).get('body', {}).get('items', [])
                    if items:
                        rgn_nm = items[0].get('cnstrtsiteRgnNm', 'N/A')
                        print(f"[SUCCESS] 발주처: {instt_nm}")
                        print(f"  └ 통합계약번호: {unty_no} -> 공사현장지역명: {rgn_nm}")
                        print(f"  └ 공사명: {wk_nm}\n")
                        success_count += 1
                    else:
                        print(f"[NO ITEMS] 통합계약번호: {unty_no}")
                else:
                    print(f"[FAIL API] {header.get('resultMsg')}")
        except Exception as api_e:
            print(f"[API ERROR] {api_e}")
            
        time.sleep(0.5) # small delay to prevent rate limiting
        
    print(f"\n최종 성공 건수: {success_count} / {min(3, len(df_busan))} (샘플 3건 중)")
except Exception as e:
    import traceback
    traceback.print_exc()
