import urllib.request
import urllib.parse
import json
import ssl
import sys
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os

sys.stdout.reconfigure(encoding='utf-8')
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = os.environ.get('SERVICE_KEY', '')
# 용역계약, 물품계약, 공사계약, 종쇼 납품요구
API_ENDPOINTS = {
    '용역': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch',
    '공사': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkPPSSrch',
    '물품': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListGoodsPPSSrch',
    '종쇼': 'https://apis.data.go.kr/1230000/ao/ShopDlvrRqstInfoService/getPpsDlvrRqstListSrch'
}

def fetch_data_for_date(api_name, url, target_date):
    """지정된 날짜 1일치 데이터를 파싱합니다 (페이지 1번, 최대 100row라고 가정)"""
    if api_name == '종쇼':
        params = f"?serviceKey={SERVICE_KEY}&srchOptBgnDate={target_date}&srchOptEndDate={target_date}&numOfRows=999&pageNo=1&type=json"
    else:
        params = f"?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDate={target_date}&inqryEndDate={target_date}&numOfRows=999&pageNo=1&type=json"
    
    full_url = url + params
    try:
        req = urllib.request.Request(full_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=ctx, timeout=30) as response:
            res_str = response.read().decode('utf-8')
            res_json = json.loads(res_str)
            
            # 응답 구조 파싱 로직 (API에 따라 body 위치가 다를 수 있으나 조달청은 대개 비슷함)
             # 여기서 실제 스펙에 맞게 추출 로직을 정교화할 예정. (이번 스크립트는 뼈대 테스트용)
            items = res_json.get('response', {}).get('body', {}).get('items', [])
            return (api_name, target_date, len(items), items)
            
    except Exception as e:
        return (api_name, target_date, 0, f"Error: {str(e)}")

def run_historical_load(start_date_str, end_date_str):
    start_dt = datetime.strptime(start_date_str, '%Y%m%d')
    end_dt = datetime.strptime(end_date_str, '%Y%m%d')
    
    date_list = []
    curr = start_dt
    while curr <= end_dt:
        date_list.append(curr.strftime('%Y%m%d'))
        curr += timedelta(days=1)
        
    print(f"총 {len(date_list)}일치 데이터 수집을 시작합니다. ({start_date_str} ~ {end_date_str})")
    
    # 임시 테스트: 3일치만 우선 테스트
    test_dates = date_list[:3] 
    
    results = []
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for d in test_dates:
            for api_name, url in API_ENDPOINTS.items():
                futures.append(executor.submit(fetch_data_for_date, api_name, url, d))
                
        for future in as_completed(futures):
            api_name, dt, count, data = future.result()
            print(f"완료: [{api_name}] {dt} -> {count if isinstance(count, int) else '에러'} 건")
            if isinstance(count, int) and count > 0:
                results.extend(data)
                
    elapsed = time.time() - start_time
    print(f"\\n수집 완료! 소요시간: {elapsed:.2f}초. 총 확보 데이터: {len(results)}건")
    
if __name__ == '__main__':
    # 테스트용 3일 기간 설정
    run_historical_load('20260101', '20260131')
