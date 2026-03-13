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

SERVICE_KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'
DB_PATH = 'procurement_contracts.db'
BUSAN = quote('부산광역시')

def init_award_table():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS busan_award_servc (
            bidNtceNo TEXT,
            bidNtceOrd TEXT,
            bidNtceNm TEXT,
            dminsttCd TEXT,
            dminsttNm TEXT,
            bidwinnrBizno TEXT,
            bidwinnrNm TEXT,
            bidwinnrAdrs TEXT,
            sucsfbidAmt TEXT,
            fnlSucsfDate TEXT,
            PRIMARY KEY (bidNtceNo, bidNtceOrd)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_award_bizno ON busan_award_servc (bidwinnrBizno)')
    conn.commit()
    conn.close()
    print("✅ busan_award_servc 테이블 준비 완료")

def fetch_awards_for_period(start_dt, end_dt):
    """한 기간의 부산 지역제한 용역 낙찰 데이터를 모두 가져옴"""
    page_no = 1
    num_of_rows = 100
    results = []
    
    while True:
        url = (f'https://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusServcPPSSrch'
               f'?serviceKey={SERVICE_KEY}&inqryDiv=1'
               f'&inqryBgnDt={start_dt}&inqryEndDt={end_dt}'
               f'&numOfRows={num_of_rows}&pageNo={page_no}&type=json'
               f'&prtcptLmtRgnNm={BUSAN}')
        
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            res = urllib.request.urlopen(req, context=ctx, timeout=30)
            data = json.loads(res.read().decode('utf-8'))
            
            header = data.get('response', {}).get('header', {})
            if header.get('resultCode') != '00':
                break
                
            items = data.get('response', {}).get('body', {}).get('items', [])
            if not items:
                break
            
            for item in items:
                results.append((
                    item.get('bidNtceNo', ''),
                    item.get('bidNtceOrd', '00'),
                    item.get('bidNtceNm', ''),
                    item.get('dminsttCd', ''),
                    item.get('dminsttNm', ''),
                    item.get('bidwinnrBizno', ''),
                    item.get('bidwinnrNm', ''),
                    item.get('bidwinnrAdrs', ''),
                    item.get('sucsfbidAmt', ''),
                    item.get('fnlSucsfDate', ''),
                ))
                    
            total_count = data.get('response', {}).get('body', {}).get('totalCount', 0)
            if page_no * num_of_rows >= total_count:
                break
                
            page_no += 1
            time.sleep(0.05)
            
        except Exception as e:
            if '429' in str(e):
                print(f"  ⚠️ 429 Rate Limit! {start_dt}~{end_dt}")
                return None  # 429 시그널
            print(f"  [Error] {start_dt}~{end_dt} page {page_no}: {e}")
            break
            
    return results

def collect_all():
    init_award_table()
    
    conn = sqlite3.connect(DB_PATH)
    total_inserted = 0
    
    # 월 단위로 수집 (API 호출 최소화)
    start_year, start_month = 2020, 1
    end_year, end_month = 2026, 3
    
    current = datetime.date(start_year, start_month, 1)
    end_date = datetime.date(end_year, end_month, 28)
    
    while current <= end_date:
        # 월의 마지막 날 계산
        if current.month == 12:
            next_month = datetime.date(current.year + 1, 1, 1)
        else:
            next_month = datetime.date(current.year, current.month + 1, 1)
        last_day = next_month - datetime.timedelta(days=1)
        
        start_str = current.strftime('%Y%m%d') + '0000'
        end_str = last_day.strftime('%Y%m%d') + '2359'
        
        results = fetch_awards_for_period(start_str, end_str)
        
        if results is None:
            print("❌ API 한도 초과! 수집 중단.")
            break
        
        if results:
            conn.executemany('''
                INSERT OR IGNORE INTO busan_award_servc
                (bidNtceNo, bidNtceOrd, bidNtceNm, dminsttCd, dminsttNm, 
                 bidwinnrBizno, bidwinnrNm, bidwinnrAdrs, sucsfbidAmt, fnlSucsfDate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', results)
            conn.commit()
            total_inserted += len(results)
            print(f"  [{current.strftime('%Y-%m')}] {len(results)}건 수집 / 누적 {total_inserted:,}건")
        else:
            print(f"  [{current.strftime('%Y-%m')}] 0건")
        
        current = next_month
        time.sleep(0.1)
    
    conn.close()
    print(f"\n✅ 수집 완료! 총 {total_inserted:,}건 적재")

if __name__ == '__main__':
    collect_all()
