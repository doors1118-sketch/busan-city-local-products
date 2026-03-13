import urllib.request
import json
import pandas as pd
import ssl
import sys
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(encoding='utf-8')

# SSL Context
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

service_key = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'
base_url = 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch'
bgn_dt = '20260101'
end_dt = '20260131'
num_of_rows = 999

def fetch_page(page_no):
    query_params = f"?serviceKey={service_key}&inqryDiv=1&inqryBgnDate={bgn_dt}&inqryEndDate={end_dt}&numOfRows={num_of_rows}&pageNo={page_no}&type=json"
    url = base_url + query_params
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=ctx, timeout=30) as response:
            data = json.loads(response.read().decode('utf-8'))
            items = data.get('response', {}).get('body', {}).get('items', [])
            return page_no, items, None
    except Exception as e:
        return page_no, [], str(e)

print(f"[{bgn_dt} ~ {end_dt}] 용역계약현황 API 초고속 병렬 다운로드 테스트", flush=True)
start_time = time.time()

# 1. Get exact total count from page 1
print("초기 데이터 건수 파악 중...", flush=True)
query_params = f"?serviceKey={service_key}&inqryDiv=1&inqryBgnDate={bgn_dt}&inqryEndDate={end_dt}&numOfRows=1&pageNo=1&type=json"
req = urllib.request.Request(base_url + query_params, headers={'User-Agent': 'Mozilla/5.0'})

try:
    with urllib.request.urlopen(req, context=ctx, timeout=30) as response:
        data = json.loads(response.read().decode('utf-8'))
        total_count = data.get('response', {}).get('body', {}).get('totalCount', 0)
        
    if total_count == 0:
        print("조회된 데이터가 없습니다.", flush=True)
        sys.exit(0)
        
    total_pages = math.ceil(total_count / num_of_rows)
    print(f">> 총 데이터 건수: {total_count:,}건 (총 {total_pages} 페이지 요청 필요)", flush=True)
except Exception as e:
    print(f"초기 요쳥 실패: {e}")
    sys.exit(1)

# 2. Parallel Fetching
all_items = []
completed_pages = 0
failed_pages = []

print(f"\n최대 10개의 스레드를 가동하여 동시 병렬 수집을 시작합니다...", flush=True)
# Adjust max_workers for optimal performance vs hitting API limits. 10 is usually safe and fast.
with ThreadPoolExecutor(max_workers=10) as executor:
    # Submit all tasks
    future_to_page = {executor.submit(fetch_page, p): p for p in range(1, total_pages + 1)}
    
    for future in as_completed(future_to_page):
        page_no = future_to_page[future]
        p_no, items, err = future.result()
        
        if err:
            print(f"  [오류] Page {p_no} 수집 실패: {err}", flush=True)
            failed_pages.append(p_no)
        else:
            all_items.extend(items)
            completed_pages += 1
            if completed_pages % 5 == 0 or completed_pages == total_pages:
                print(f"  ... {completed_pages}/{total_pages} 페이지 수집 완료 (진행률: {(completed_pages/total_pages)*100:.1f}%)", flush=True)

# Retry failed pages once sequentially
if failed_pages:
    print(f"\n누락된 {len(failed_pages)}개 페이지 재요청 중...", flush=True)
    for p_no in failed_pages:
        _, items, err = fetch_page(p_no)
        if not err:
            all_items.extend(items)

end_time = time.time()
elapsed_sec = end_time - start_time

if all_items:
    print("\n데이터프레임 변환 및 엑셀 저장 중...", flush=True)
    df = pd.DataFrame(all_items)
    
    # Simple deduplication just in case
    # API might not have strict PK if it's returning empty or changing, but typically untyCntrctNo is good
    if 'untyCntrctNo' in df.columns:
        df = df.drop_duplicates(subset=['untyCntrctNo'])
        
    output_filename = f'API_용역계약초고속_전체({bgn_dt}_{end_dt}).xlsx'
    df.to_excel(output_filename, index=False)
    
    print("="*50)
    print(f"✅ 다운로드 및 저장 성공!")
    print(f"  - 최종 저장 건수: {len(df):,} 건")
    print(f"  - 걸린 시간: {elapsed_sec:.1f} 초 (약 {elapsed_sec/60:.1f} 분)")
    print(f"  - 초당 처리 건수: {len(df)/elapsed_sec:.0f} 건/초")
    print("="*50)
else:
    print("수집된 데이터가 없습니다.", flush=True)
