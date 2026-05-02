import os
import urllib.request
import json
import ssl
import sqlite3
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import datetime
import sys

sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = os.environ.get('SERVICE_KEY', '')
DB_PATH = 'procurement_contracts.db'
# 용역 API Endpoints (조달청 대행 + 자체 발주)
API_ENDPOINTS = [
    'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch',
    'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcSrch'
]

def fetch_data(api_url, bgn_date, end_date, page_no=1, num_of_rows=999):
    query = f"?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDate={bgn_date}&inqryEndDate={end_date}&numOfRows={num_of_rows}&pageNo={page_no}&type=json"
    url = api_url + query
    retry = 0
    while retry < 3:
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, context=ctx, timeout=30) as res:
                text = res.read().decode('utf-8')
                data = json.loads(text)
                header = data.get('response', {}).get('header', {})
                if header.get('resultCode') == '00':
                    body = data.get('response', {}).get('body', {})
                    return body.get('items', []), body.get('totalCount', 0)
                else:
                    return [], 0
        except Exception as e:
            time.sleep(1)
            retry += 1
    return [], 0
    
def download_for_date(date_str, api_url):
    print(f"[용역] {date_str} 조회 중... ({'중앙' if 'PPS' in api_url else '자체'})")
    items, total_count = fetch_data(api_url, date_str, date_str, page_no=1)
    if total_count == 0:
        return date_str, []
        
    all_items = list(items)
    total = int(total_count)
    if total > 999:
        total_pages = (total // 999) + 1
        with ThreadPoolExecutor(max_workers=3) as p_executor:
            futures = [p_executor.submit(fetch_data, api_url, date_str, date_str, p) for p in range(2, total_pages + 1)]
            for future in as_completed(futures):
                p_items, _ = future.result()
                all_items.extend(p_items)
                
    return date_str, all_items

def main():
    print("==========================================================")
    print(" 🚀 조달청 [용역] 계약 1차 초기 적재 (Historical Load)")
    print("    - 대상: 용역계약현황 검색기능 API (getCntrctInfoListServc PPSSrch + Srch)")
    end_date = datetime.date.today() - datetime.timedelta(days=1)  # D-1 (어제)
    start_date = datetime.date(2026, 1, 1)
    print(f"    - 기간: {start_date} ~ {end_date}")
    print("==========================================================\\n")
    
    dates = []
    curr = start_date
    while curr <= end_date:
        dates.append(curr.strftime("%Y%m%d"))
        curr += datetime.timedelta(days=1)
        
    all_data = []
    start_time = time.time()
    
    print(f"총 [{len(dates)}일] 간의 과거 용역 데이터를 양대 API에서 병렬 다운로드합니다...")
    
    completed_days = 0
    total_tasks = len(dates) * len(API_ENDPOINTS)
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for d in dates:
            for api_url in API_ENDPOINTS:
                futures.append(executor.submit(download_for_date, d, api_url))
                
        for future in as_completed(futures):
            date_str, items = future.result()
            if items:
                all_data.extend(items)
            completed_days += 1
            if completed_days % 20 == 0:
                print(f"   -> 진행률: {completed_days}/{total_tasks} 완료...")
                
    fetch_time = time.time()
    print(f"\\n✅ 1. 병렬 다운로드 완료! (요청 소요시간: {fetch_time - start_time:.1f}초)")
    
    print(f"\\n✅ 2. 로컬 SQLite DB ({DB_PATH}) -> 'servc_cntrct' (용역) 테이블 저장 시작...")
    if all_data:
        conn = sqlite3.connect(DB_PATH)
        df = pd.DataFrame(all_data)
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].astype(str)
                
        # 부산 수요기관 필터 적용
        agency_conn = sqlite3.connect('busan_agencies_master.db')
        busan_codes = set(str(r[0]).strip() for r in agency_conn.execute("SELECT dminsttCd FROM agency_master").fetchall())
        agency_conn.close()
        
        n_before = len(df)
        if 'dminsttCd' in df.columns:
            df = df[df['dminsttCd'].astype(str).str.strip().isin(busan_codes)]
        print(f"   - [용역] 전국 {n_before:,} → 부산 {len(df):,}건 필터 적용")
        
        df.to_sql('servc_cntrct', conn, if_exists='replace', index=False)
        print(f"   - [용역] 총 {len(df):,}건 적재 성공.")
        conn.close()
    else:
        print("다운로드된 데이터가 없습니다.")
    
    end_time = time.time()
    print("==========================================================")
    print(f"🎉 용역계약 로드 작업 성공 종료. (총 소요시간: {end_time - start_time:.1f}초)")
    print("==========================================================")

if __name__ == '__main__':
    main()
