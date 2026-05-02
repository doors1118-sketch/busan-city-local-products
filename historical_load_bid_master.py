import os
import urllib.request
import json
import ssl
import sqlite3
import datetime
import math
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = os.environ.get('SERVICE_KEY', '')
DB_PATH = 'c:/Users/COMTREE/Desktop/연습/procurement_contracts.db'
BASE_URL = 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwk'

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bid_notices_raw (
            bidNtceNo TEXT,
            bidNtceOrd TEXT,
            bidNtceNm TEXT,
            dminsttCd TEXT,
            dminsttNm TEXT,
            cnstrtsiteRgnNm TEXT,
            bidNtceDt TEXT,
            PRIMARY KEY (bidNtceNo, bidNtceOrd)
        )
    ''')
    # 현장 지역명 검색을 위한 인덱스 생성
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_bid_no ON bid_notices_raw (bidNtceNo)')
    conn.commit()
    conn.close()

def load_bid_notices_for_date(target_date):
    """지정된 날짜 하루치(00:00 ~ 23:59) 입찰공고 데이터를 수집하여 반환합니다."""
    date_str_start = f"{target_date}0000"
    date_str_end = f"{target_date}2359"
    
    page_no = 1
    num_of_rows = 100
    daily_results = []
    
    while True:
        url = f'{BASE_URL}?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt={date_str_start}&inqryEndDt={date_str_end}&numOfRows={num_of_rows}&pageNo={page_no}&type=json'
        
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            res = urllib.request.urlopen(req, context=ctx, timeout=30)
            text = res.read().decode('utf-8')
            data = json.loads(text)
            
            header = data.get('response', {}).get('header', {})
            if header.get('resultCode') != '00':
                print(f"[Error] API Error on {target_date}: {header.get('resultMsg')}")
                break
                
            items = data.get('response', {}).get('body', {}).get('items', [])
            
            if not items:
                break
            
            for item in items:
                bid_no = item.get('bidNtceNo', '')
                bid_ord = item.get('bidNtceOrd', '00')
                bid_nm = item.get('bidNtceNm', '')
                dm_cd = item.get('dminsttCd', '')
                dm_nm = item.get('dminsttNm', '')
                rgn_nm = item.get('cnstrtsiteRgnNm', '')
                dt = item.get('bidNtceDt', '')
                
                if bid_no:
                    daily_results.append((bid_no, bid_ord, bid_nm, dm_cd, dm_nm, rgn_nm, dt))
                    
            total_count = data.get('response', {}).get('body', {}).get('totalCount', 0)
            if page_no * num_of_rows >= total_count:
                break
                
            page_no += 1
            time.sleep(0.05)
            
        except Exception as e:
            print(f"[Exception] {target_date} page {page_no}: {e}")
            break
            
    return target_date, daily_results

def bulk_load_historical(start_date, end_date):
    init_db()
    
    # 생성할 날짜 리스트 만들기
    curr = start_date
    date_list = []
    while curr <= end_date:
        date_list.append(curr.strftime('%Y%m%d'))
        curr += datetime.timedelta(days=1)
        
    print(f"총 {len(date_list)}일치 입찰공고 데이터 병렬 수집 시작... ({start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')})")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    total_inserted = 0
    total_errors = 0
    
    # 스레드풀을 통한 병렬 수집
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_date = {executor.submit(load_bid_notices_for_date, d): d for d in date_list}
        
        for future in as_completed(future_to_date):
            d = future_to_date[future]
            try:
                dt, results = future.result()
                if results:
                    cursor.executemany('''
                        INSERT OR IGNORE INTO bid_notices_raw
                        (bidNtceNo, bidNtceOrd, bidNtceNm, dminsttCd, dminsttNm, cnstrtsiteRgnNm, bidNtceDt)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', results)
                    conn.commit()
                    total_inserted += len(results)
                    print(f"[{dt}] 완료: {len(results):,}건 적재 누적합계: {total_inserted:,}건")
                else:
                    print(f"[{dt}] 완료: 0건")
            except Exception as exc:
                print(f"[{d}] 병렬 처리 중 에러 발생: {exc}")
                total_errors += 1
                
    conn.close()
    elapsed = time.time() - start_time
    print(f"\\n데이터 수집 완료! 총 {total_inserted:,}건 적재됨.")
    print(f"소요 시간: {elapsed:.2f}초, 에러 발생: {total_errors}건")

if __name__ == '__main__':
    # 장기계속계약 현장위치 보완을 위한 과거 데이터 소급 수집
    # 기존에 2025-11-01 ~ 2026-03-04 는 이미 적재 완료됨
    # 이번에는 2020-01-01 ~ 2025-10-31 소급 수집
    dt_bgn = datetime.datetime(2020, 1, 1)
    dt_end = datetime.datetime(2025, 10, 31)
    bulk_load_historical(dt_bgn, dt_end)
