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
    df = pd.read_excel('API_공사계약조회_전체(20260101_20260131).xlsx')
    sample_df = df.sample(n=10, random_state=42)
    
    success = 0
    fail = 0
    
    print('--- 10 Random Contracts API Check ---')
    for idx, row in sample_df.iterrows():
        unty_no = str(row.get('untyCntrctNo', ''))
        
        if not unty_no or unty_no == 'nan':
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
                        success += 1
                        rgn_nm = items[0].get('cnstrtsiteRgnNm', 'N/A')
                        print(f"[FOUND] {unty_no} -> {rgn_nm}")
                    else:
                        fail += 1
                        print(f"[EMPTY] {unty_no}")
                else:
                    fail += 1
            time.sleep(0.3)
        except Exception as api_e:
            fail += 1
            
    print(f'\nResult: FOUND {success}, EMPTY/ERROR {fail}')
except Exception as e:
    import traceback
    traceback.print_exc()
