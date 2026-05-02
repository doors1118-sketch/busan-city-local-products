"""
입찰공고 추정가격 DB 수집 (Bid Notice Price Loader)
=====================================================
공사/용역/물품 3개 분야 입찰공고에서 추정가격(presmptPrce) 등
가격 정보를 수집하여 SQLite DB에 저장

기간: 2025-10-01 ~ D-1(어제)
목적: 지역제한경쟁 가능 여부 분석을 위한 추정가격 확보
"""
import urllib.request
import json
import ssl
import sqlite3
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import datetime
import sys
import os

# stdout 버퍼링 해제
sys.stdout.reconfigure(encoding='utf-8')
os.environ['PYTHONUNBUFFERED'] = '1'

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = os.environ.get('SERVICE_KEY', '')
DB_PATH = 'procurement_contracts.db'

# 3개 분야 입찰공고 API
BID_APIS = {
    '공사': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwk',
    '용역': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServc',
    '물품': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThng',
}

# 저장할 핵심 필드 (추정가격 + 계약방식 + 기관 + 공고정보)
KEEP_FIELDS = [
    'bidNtceNo',        # 공고번호
    'bidNtceOrd',       # 공고차수
    'bidNtceNm',        # 공고명
    'ntceInsttCd',      # 공고기관코드
    'ntceInsttNm',      # 공고기관명
    'dminsttCd',        # 수요기관코드
    'dminsttNm',        # 수요기관명
    'presmptPrce',      # ★ 추정가격
    'bdgtAmt',          # 설계금액(예산액)
    'cntrctCnclsMthdNm',  # 계약체결방식 (제한경쟁/일반경쟁 등)
    'bidNtceDt',        # 공고일시
    'rgstDt',           # 등록일시
    'sucsfbidLwltRate',   # 낙찰하한율
    'sucsfbidMthdNm',    # 낙찰방법명
]

# 공사 전용 추가 필드
CNSTWK_EXTRA = [
    'cnstrtsiteRgnNm',  # 현장지역명
]

# 용역/물품 전용 추가 필드
NON_CNSTWK_EXTRA = [
    'prtcptLmtRgnNm',   # 참가제한지역명 (있으면)
]

def fetch_bid_notices(api_url, bgn_dt, end_dt, page_no=1, num_of_rows=999):
    """입찰공고 API 호출"""
    query = (f"?serviceKey={SERVICE_KEY}"
             f"&inqryDiv=1&inqryBgnDt={bgn_dt}&inqryEndDt={end_dt}"
             f"&numOfRows={num_of_rows}&pageNo={page_no}&type=json")
    url = api_url + query
    retry = 0
    while retry < 5:
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, context=ctx, timeout=30) as res:
                data = json.loads(res.read().decode('utf-8'))
                header = data.get('response', {}).get('header', {})
                if header.get('resultCode') == '00':
                    body = data.get('response', {}).get('body', {})
                    return body.get('items', []), body.get('totalCount', 0)
                else:
                    return [], 0
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = min(30 * (retry + 1), 120)
                print(f"      ⚠️ 429 한도 초과, {wait}초 대기 후 재시도...")
                time.sleep(wait)
            else:
                time.sleep(2)
        except Exception:
            time.sleep(2)
        retry += 1
    return [], 0

def collect_day(api_url, sector, date_str, keep_fields):
    """하루치 입찰공고 수집"""
    bgn_dt = f"{date_str}0000"
    end_dt = f"{date_str}2359"
    
    items, total_count = fetch_bid_notices(api_url, bgn_dt, end_dt, page_no=1)
    if total_count == 0:
        return []
    
    all_items = list(items) if items else []
    total = int(total_count)
    
    if total > 999:
        total_pages = (total // 999) + 1
        for p in range(2, total_pages + 1):
            p_items, _ = fetch_bid_notices(api_url, bgn_dt, end_dt, page_no=p)
            if p_items:
                all_items.extend(p_items)
            time.sleep(0.3)
    
    # 필요한 필드만 추출
    filtered = []
    for item in all_items:
        row = {f: item.get(f, '') for f in keep_fields}
        row['sector'] = sector
        filtered.append(row)
    
    return filtered

def main():
    start_date = datetime.date(2025, 10, 1)
    end_date = datetime.date.today() - datetime.timedelta(days=1)  # D-1
    
    print("==========================================================")
    print(" 🚀 입찰공고 추정가격 DB 수집 (Bid Notice Price Loader)")
    print(f"    - 기간: {start_date} ~ {end_date}")
    print(f"    - 대상: 공사/용역/물품 입찰공고")
    print(f"    - 목적: 추정가격(presmptPrce) 확보")
    print("==========================================================\n")
    
    # 날짜 목록 생성
    dates = []
    curr = start_date
    while curr <= end_date:
        dates.append(curr.strftime("%Y%m%d"))
        curr += datetime.timedelta(days=1)
    
    print(f"총 {len(dates)}일 × 3개 분야 = {len(dates)*3}개 작업\n")
    
    conn = sqlite3.connect(DB_PATH)
    
    # 테이블 초기화 후 재생성
    conn.execute("DROP TABLE IF EXISTS bid_notices_price")
    conn.execute("""
        CREATE TABLE bid_notices_price (
            bidNtceNo TEXT,
            bidNtceOrd TEXT,
            bidNtceNm TEXT,
            ntceInsttCd TEXT,
            ntceInsttNm TEXT,
            dminsttCd TEXT,
            dminsttNm TEXT,
            presmptPrce TEXT,
            bdgtAmt TEXT,
            cntrctCnclsMthdNm TEXT,
            bidNtceDt TEXT,
            rgstDt TEXT,
            sucsfbidLwltRate TEXT,
            sucsfbidMthdNm TEXT,
            cnstrtsiteRgnNm TEXT,
            prtcptLmtRgnNm TEXT,
            sector TEXT,
            PRIMARY KEY (bidNtceNo, bidNtceOrd, sector)
        )
    """)
    conn.commit()
    
    start_time = time.time()
    total_inserted = 0
    
    # 분야별 순차 수집 (API 한도 관리)
    for sector, api_url in BID_APIS.items():
        if sector == '공사':
            fields = KEEP_FIELDS + CNSTWK_EXTRA
        else:
            fields = KEEP_FIELDS + NON_CNSTWK_EXTRA
        
        print(f"\n{'─'*60}")
        print(f"  📋 [{sector}] 입찰공고 수집 시작")
        print(f"{'─'*60}")
        
        sector_count = 0
        batch_data = []
        
        all_cols = ['bidNtceNo','bidNtceOrd','bidNtceNm','ntceInsttCd','ntceInsttNm',
                     'dminsttCd','dminsttNm','presmptPrce','bdgtAmt','cntrctCnclsMthdNm',
                     'bidNtceDt','rgstDt','sucsfbidLwltRate','sucsfbidMthdNm',
                     'cnstrtsiteRgnNm','prtcptLmtRgnNm','sector']
        placeholders = ','.join(['?'] * len(all_cols))
        insert_sql = f"INSERT OR IGNORE INTO bid_notices_price ({','.join(all_cols)}) VALUES ({placeholders})"
        
        for i, date_str in enumerate(dates):
            items = collect_day(api_url, sector, date_str, fields)
            if items:
                batch_data.extend(items)
                sector_count += len(items)
            
            # 30일마다 또는 마지막에 DB 저장
            if (i + 1) % 30 == 0 or i == len(dates) - 1:
                if batch_data:
                    rows = []
                    for row in batch_data:
                        rows.append(tuple(row.get(c, '') for c in all_cols))
                    conn.executemany(insert_sql, rows)
                    conn.commit()
                    batch_data = []
                elapsed = time.time() - start_time
                print(f"   [{sector}] {i+1}/{len(dates)}일 완료 | 누적 {sector_count:,}건 | 경과 {elapsed:.0f}초")
            
            # API 한도 관리: 호출 간격
            time.sleep(0.2)
        
        total_inserted += sector_count
        print(f"   ✅ [{sector}] 완료: {sector_count:,}건 적재")
    
    # 중복 제거 (혹시 재실행 시)
    conn.execute("""
        DELETE FROM bid_notices_price WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM bid_notices_price 
            GROUP BY bidNtceNo, bidNtceOrd, sector
        )
    """)
    conn.commit()
    
    # 결과 확인
    cur = conn.cursor()
    cur.execute("SELECT sector, COUNT(1) FROM bid_notices_price GROUP BY sector")
    print(f"\n{'='*60}")
    print(f"  📊 bid_notices_price 테이블 현황")
    print(f"{'='*60}")
    for sector, cnt in cur.fetchall():
        print(f"  {sector}: {cnt:,}건")
    
    # 추정가격 채워진 비율
    cur.execute("""
        SELECT sector, 
               COUNT(1) as total,
               SUM(CASE WHEN presmptPrce IS NOT NULL AND presmptPrce != '' AND presmptPrce != '0' THEN 1 ELSE 0 END) as filled
        FROM bid_notices_price GROUP BY sector
    """)
    print(f"\n  추정가격(presmptPrce) 유효율:")
    for sector, total, filled in cur.fetchall():
        rate = filled / total * 100 if total > 0 else 0
        print(f"  {sector}: {filled:,}/{total:,} ({rate:.1f}%)")
    
    conn.close()
    
    end_time = time.time()
    print(f"\n{'='*60}")
    print(f"🎉 총 {total_inserted:,}건 수집 완료 (소요시간: {end_time - start_time:.0f}초)")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
