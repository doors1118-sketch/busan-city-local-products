import os
import urllib.request
import json
import ssl
import sqlite3
import datetime
import sys
import time
from urllib.parse import quote

sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = os.environ.get('SERVICE_KEY', '')
DB_PATH = 'procurement_contracts.db'
BUSAN = quote('부산광역시')

TARGETS = {
    'busan_award_cnstwk': {
        'url': 'https://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusCnstwkPPSSrch',
        'label': '공사',
    },
    'busan_award_thng': {
        'url': 'https://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusThngPPSSrch',
        'label': '물품',
    },
}

def init_table(conn, table_name):
    conn.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
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
    conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_bizno ON {table_name} (bidwinnrBizno)')
    conn.commit()

def fetch_awards(base_url, start_dt, end_dt):
    page_no = 1
    results = []
    
    while True:
        url = (f'{base_url}?serviceKey={SERVICE_KEY}&inqryDiv=1'
               f'&inqryBgnDt={start_dt}&inqryEndDt={end_dt}'
               f'&numOfRows=100&pageNo={page_no}&type=json'
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
                    item.get('bidNtceNo', ''), item.get('bidNtceOrd', '00'),
                    item.get('bidNtceNm', ''), item.get('dminsttCd', ''),
                    item.get('dminsttNm', ''), item.get('bidwinnrBizno', ''),
                    item.get('bidwinnrNm', ''), item.get('bidwinnrAdrs', ''),
                    item.get('sucsfbidAmt', ''), item.get('fnlSucsfDate', ''),
                ))
            total = data.get('response', {}).get('body', {}).get('totalCount', 0)
            if page_no * 100 >= total:
                break
            page_no += 1
            time.sleep(0.05)
        except Exception as e:
            if '429' in str(e):
                print(f"  ⚠️ 429! 중단.")
                return None
            print(f"  [Error] {e}")
            break
    return results

def collect_all():
    conn = sqlite3.connect(DB_PATH)
    
    for table_name, cfg in TARGETS.items():
        init_table(conn, table_name)
        label = cfg['label']
        base_url = cfg['url']
        total_inserted = 0
        
        print(f"\n🚀 [{label}] 부산 지역제한 낙찰정보 수집 시작...")
        
        current = datetime.date(2020, 1, 1)
        end_date = datetime.date(2026, 3, 28)
        
        while current <= end_date:
            if current.month == 12:
                next_month = datetime.date(current.year + 1, 1, 1)
            else:
                next_month = datetime.date(current.year, current.month + 1, 1)
            last_day = next_month - datetime.timedelta(days=1)
            
            start_str = current.strftime('%Y%m%d') + '0000'
            end_str = last_day.strftime('%Y%m%d') + '2359'
            
            results = fetch_awards(base_url, start_str, end_str)
            if results is None:
                print("❌ API 한도 초과!")
                break
            
            if results:
                conn.executemany(f'''
                    INSERT OR IGNORE INTO {table_name}
                    (bidNtceNo, bidNtceOrd, bidNtceNm, dminsttCd, dminsttNm,
                     bidwinnrBizno, bidwinnrNm, bidwinnrAdrs, sucsfbidAmt, fnlSucsfDate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', results)
                conn.commit()
                total_inserted += len(results)
                print(f"  [{current.strftime('%Y-%m')}] {len(results)}건 / 누적 {total_inserted:,}건")
            else:
                pass
            
            current = next_month
            time.sleep(0.1)
        
        print(f"✅ [{label}] 수집 완료! 총 {total_inserted:,}건")
    
    conn.close()

if __name__ == '__main__':
    collect_all()
