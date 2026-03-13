import urllib.request
import json
import ssl
import sqlite3
import pandas as pd
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import sys
import threading

sys.stdout.reconfigure(encoding='utf-8')

# SSL Context
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'
BASE_URL = 'https://apis.data.go.kr/1230000/ao/UsrInfoService02/getDminsttInfo02'
DB_PATH = 'busan_agencies_master.db'

# SQLite DB Setup
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS agency_master (
            dminsttCd TEXT PRIMARY KEY,
            dminsttNm TEXT,
            bizno TEXT,
            rgnNm TEXT,
            adrs TEXT,
            dltYn TEXT,
            rgstDt TEXT,
            chgDt TEXT
        )
    ''')
    conn.commit()
    conn.close()

# Fetch data for a specific year
def fetch_agencies_for_year(year):
    bgn_dt = f"{year}01010000"
    end_dt = f"{year}12312359"
    # To handle pagination we first get total count
    query_params = f"?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt={bgn_dt}&inqryEndDt={end_dt}&numOfRows=1&pageNo=1&type=json"
    
    try:
        req = urllib.request.Request(BASE_URL + query_params, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=ctx, timeout=15) as response:
            data = json.loads(response.read().decode('utf-8'))
            header = data.get('response', {}).get('header', {})
            if header.get('resultCode') != '00':
                return []
            
            total_count = data.get('response', {}).get('body', {}).get('totalCount', 0)
            if not total_count or int(total_count) == 0:
                return []
    except Exception as e:
        print(f"[{year}] Initial check error: {e}")
        return []

    # Now fetch all at once or in big chunks (up to 999 is standard max usually, but let's see if 9999 works. Let's use 999 just in case)
    total = int(total_count)
    all_items = []
    num_of_rows = 999
    total_pages = (total // num_of_rows) + 1
    
    for page in range(1, total_pages + 1):
        qp = f"?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt={bgn_dt}&inqryEndDt={end_dt}&numOfRows={num_of_rows}&pageNo={page}&type=json"
        retry = 0
        while retry < 3:
            try:
                rq = urllib.request.Request(BASE_URL + qp, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(rq, context=ctx, timeout=30) as res:
                    d = json.loads(res.read().decode('utf-8'))
                    items = d.get('response', {}).get('body', {}).get('items', [])
                    all_items.extend(items)
                    break
            except Exception as e:
                retry += 1
                time.sleep(1)
        
    # Filter Busan agencies
    busan_agencies = []
    for item in all_items:
        rgn = str(item.get('rgnNm', ''))
        adrs = str(item.get('adrs', ''))
        if '부산' in rgn or '부산광역시' in adrs:
            busan_agencies.append((
                item.get('dminsttCd'),
                item.get('dminsttNm'),
                item.get('bizno'),
                item.get('rgnNm'),
                item.get('adrs'),
                item.get('dltYn'),
                item.get('rgstDt'),
                item.get('chgDt')
            ))
            
    print(f"[{year}] Total items: {total} | Busan agencies found: {len(busan_agencies)}")
    return busan_agencies

def main():
    print("==================================================")
    print(" 🚀 부산 관내 수요기관 마스터 SQLite DB 최초 구축")
    print("    - 조달청 전역 호출 -> 부산 지역만 필터링 저장")
    print("==================================================\n")
    
    init_db()
    current_year = datetime.datetime.now().year
    # Nara Marketplace started around 2002. Let's start from 2000.
    years = list(range(1995, current_year + 1))
    
    all_busan_agencies = []
    db_lock = threading.Lock()
    
    start_time = time.time()
    
    # We will use multithreading to fetch multiple years concurrently
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_year = {executor.submit(fetch_agencies_for_year, yr): yr for yr in years}
        for future in as_completed(future_to_year):
            yr = future_to_year[future]
            try:
                result = future.result()
                if result:
                    with db_lock:
                        conn = sqlite3.connect(DB_PATH)
                        cursor = conn.cursor()
                        cursor.executemany('''
                            INSERT OR REPLACE INTO agency_master 
                            (dminsttCd, dminsttNm, bizno, rgnNm, adrs, dltYn, rgstDt, chgDt)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', result)
                        conn.commit()
                        conn.close()
                    all_busan_agencies.extend(result)
            except Exception as e:
                print(f"[{yr}] Error occurred: {e}")

    end_time = time.time()
    
    print("\n==================================================")
    print(f"✅ 구축 완료! 총 소요시간: {end_time - start_time:.1f}초")
    print(f"✅ 부산 전체 수요기관 누적 확보 (역대 모든 연도): {len(all_busan_agencies)}개 기관")
    print(f"✅ 결과 DB 파일: {DB_PATH}")
    print("==================================================")

if __name__ == "__main__":
    main()
