import urllib.request
import json
import pandas as pd
import ssl
import sys
import math
import time

sys.stdout.reconfigure(encoding='utf-8')

# SSL 인증서 에러 무시
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# 사용자 제공 인증키
service_key = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'

# 파라미터 셋업
bgn_dt = '20260101'
end_dt = '20260131'
num_of_rows = 100
base_url = 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkPPSSrch'

all_items = []
page_no = 1
total_pages = 1

print(f"[{bgn_dt} ~ {end_dt}] 공사현황 API 전체 다운로드를 시작합니다...", flush=True)

while page_no <= total_pages:
    query_params = f"?serviceKey={service_key}&inqryDiv=1&inqryBgnDate={bgn_dt}&inqryEndDate={end_dt}&numOfRows={num_of_rows}&pageNo={page_no}&type=json"
    url = base_url + query_params
    
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=ctx, timeout=30) as response:
            response_data = response.read().decode('utf-8')
            
        data = json.loads(response_data)
        header = data.get('response', {}).get('header', {})
        
        if header.get('resultCode') != '00':
            print(f"API Error at page {page_no}: {header.get('resultMsg')}", flush=True)
            break
            
        body = data.get('response', {}).get('body', {})
        items = body.get('items', [])
        
        if not items:
            break
            
        all_items.extend(items)
        
        if page_no == 1:
            total_count = body.get('totalCount', 0)
            total_pages = math.ceil(total_count / num_of_rows)
            print(f"> 총 데이터 건수: {total_count}건, 예상 페이지 수: {total_pages}장", flush=True)
            
        print(f"  - {page_no}/{total_pages} 페이지 수집 완료 ({len(items)}건)", flush=True)
        
        page_no += 1
        time.sleep(0.5) # API 부하 방지용 딜레이
        
    except Exception as e:
        print(f"Error fetching page {page_no}: {e}", flush=True)
        break

if all_items:
    df = pd.DataFrame(all_items)
    output_filename = f'API_공사계약조회_전체({bgn_dt}_{end_dt}).xlsx'
    print(f"\n데이터 프레임 형태: {df.shape}", flush=True)
    
    df.to_excel(output_filename, index=False)
    print(f"\n성공적으로 [ {output_filename} ] 에 {len(df)}건의 전체 데이터가 저장되었습니다!", flush=True)
else:
    print("다운로드된 데이터가 없습니다.", flush=True)
