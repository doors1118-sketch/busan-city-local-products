import os
import urllib.request
import json
import ssl
import sqlite3
import datetime
import sys
import time
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = os.environ.get('SERVICE_KEY', '')
DB_PATH = 'procurement_contracts.db'

APIS = {
    'ServcNm': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServc',
    'ThngNm': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThng'
}

def load_targeted_bid_notices(target_date, api_type):
    date_str_start = f"{target_date}0000"
    date_str_end = f"{target_date}2359"
    base_url = APIS[api_type]
    
    # 부산 지역 관련 키워드로 한정 (수요기관명 기준)
    query_str = quote('부산')
    
    page_no = 1
    num_of_rows = 100
    daily_results = []
    
    while True:
        url = f'{base_url}?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt={date_str_start}&inqryEndDt={date_str_end}&numOfRows={num_of_rows}&pageNo={page_no}&type=json&dminsttNm={query_str}'
        
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            res = urllib.request.urlopen(req, context=ctx, timeout=30)
            text = res.read().decode('utf-8')
            data = json.loads(text)
            
            header = data.get('response', {}).get('header', {})
            if header.get('resultCode') != '00':
                print(f"[Error] API Error on {target_date} ({api_type}): {header.get('resultMsg')}")
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
                
                # Extract any regional restriction field
                rgn_info = {}
                for k, v in item.items():
                    if ('rgn' in k.lower() or 'lmt' in k.lower() or 'locplc' in k.lower()) and v and str(v).strip() not in ('N', '0'):
                        rgn_info[k] = v
                rgn_json = json.dumps(rgn_info, ensure_ascii=False) if rgn_info else None
                
                real_type = 'Servc' if 'Servc' in api_type else 'Thng'
                
                if bid_no:
                    daily_results.append((bid_no, bid_ord, bid_nm, dm_cd, dm_nm, rgn_nm, dt, real_type, rgn_json))
                    
            total_count = data.get('response', {}).get('body', {}).get('totalCount', 0)
            if page_no * num_of_rows >= total_count:
                break
                
            page_no += 1
            time.sleep(0.05)
            
        except Exception as e:
            if '429' in str(e):
                print(f"[RateLimit] 429 Error on {target_date}. Stopping.")
                return target_date, api_type, []
            print(f"[Exception] {target_date} ({api_type}) page {page_no}: {e}")
            break
            
    return target_date, api_type, daily_results

def bulk_load_targeted_historical(start_date, end_date):
    curr = start_date
    date_list = []
    while curr <= end_date:
        date_list.append(curr.strftime('%Y%m%d'))
        curr += datetime.timedelta(days=1)
        
    for api_type in ['ServcNm', 'ThngNm']:
        print(f"\\n🚀 [{api_type}] 총 {len(date_list)}일치 '부산' 타겟 수집 시작... ({start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')})")
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        total_inserted = 0
        total_errors = 0
        
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=5) as executor:  # 워커 수 줄임 (안전하게)
            future_to_date = {executor.submit(load_targeted_bid_notices, d, api_type): d for d in date_list}
            
            for future in as_completed(future_to_date):
                d = future_to_date[future]
                try:
                    dt, t, results = future.result()
                    if results:
                        cursor.executemany('''
                            INSERT OR IGNORE INTO bid_notices_raw
                            (bidNtceNo, bidNtceOrd, bidNtceNm, dminsttCd, dminsttNm, cnstrtsiteRgnNm, bidNtceDt, type, rgnLmtInfo)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', results)
                        conn.commit()
                        total_inserted += len(results)
                        print(f"[{dt}] 완료: {len(results)}건 / 누적 {total_inserted:,}건")
                except Exception as exc:
                    print(f"[{d}] 에러 발생: {exc}")
                    total_errors += 1
                    
        conn.close()
        elapsed = time.time() - start_time
        print(f"✅ [{api_type}] 수집 완료! 총 {total_inserted:,}건 적재됨. ({elapsed:.1f}초)")

if __name__ == '__main__':
    dt_bgn = datetime.datetime(2020, 1, 1)
    # 현재 날짜 기준
    dt_end = datetime.datetime(2026, 3, 5)
    bulk_load_targeted_historical(dt_bgn, dt_end)
