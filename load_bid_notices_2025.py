import os
"""
공사 공고 추가 수집 (최신→과거 역순, 월 단위 저장)
====================================================
2025-10 → 2025-01 → 2024-12 → 2024-11 → ... → 2020-10 순서
에러 나면 중단, 재실행하면 이미 수집된 월은 건너뜀
"""
import urllib.request, json, ssl, sqlite3, time, datetime, sys, calendar
sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = os.environ.get('SERVICE_KEY', '')
DB_PATH = 'procurement_contracts.db'
API_URL = 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwk'

KEEP_FIELDS = ['bidNtceNo','bidNtceOrd','bidNtceNm','dminsttCd','dminsttNm',
               'cnstrtsiteRgnNm','bidNtceDt','rgnLmtInfo']

def fetch(bgn_dt, end_dt, page=1):
    query = (f"?serviceKey={SERVICE_KEY}"
             f"&inqryDiv=1&inqryBgnDt={bgn_dt}&inqryEndDt={end_dt}"
             f"&numOfRows=999&pageNo={page}&type=json")
    retry = 0
    while retry < 5:
        try:
            req = urllib.request.Request(API_URL + query, headers={'User-Agent':'Mozilla/5.0'})
            with urllib.request.urlopen(req, context=ctx, timeout=30) as res:
                data = json.loads(res.read().decode('utf-8'))
                header = data.get('response',{}).get('header',{})
                if header.get('resultCode') == '00':
                    body = data.get('response',{}).get('body',{})
                    return body.get('items',[]), body.get('totalCount',0)
                return [], 0
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = min(60*(retry+1), 180)
                print(f"    ⚠️ 429 한도초과, {wait}초 대기...")
                time.sleep(wait)
            else:
                time.sleep(3)
        except:
            time.sleep(3)
        retry += 1
    return [], 0

def collect_day(date_str):
    bgn = f"{date_str}0000"
    end = f"{date_str}2359"
    items, total = fetch(bgn, end)
    if total == 0: return []
    all_items = list(items) if items else []
    if total > 999:
        for p in range(2, (total//999)+2):
            pi, _ = fetch(bgn, end, page=p)
            if pi: all_items.extend(pi)
            time.sleep(0.8)
    result = []
    for item in all_items:
        row = {f: item.get(f,'') for f in KEEP_FIELDS}
        row['type'] = 'Cnstwk'
        rgn = item.get('rgnLmtInfo')
        if rgn and isinstance(rgn, dict):
            row['rgnLmtInfo'] = json.dumps(rgn, ensure_ascii=False)
        elif rgn:
            row['rgnLmtInfo'] = str(rgn)
        result.append(row)
    return result

def generate_months():
    """2025-10 → 2025-01 → 2024-12 → ... → 2020-10 역순 월 생성"""
    months = []
    # 2025-10 ~ 2025-01
    for m in range(10, 0, -1):
        months.append((2025, m))
    # 2024-12 ~ 2020-10
    for y in range(2024, 2019, -1):
        end_m = 12
        start_m = 10 if y == 2020 else 1
        for m in range(end_m, start_m - 1, -1):
            months.append((y, m))
    return months

def main():
    conn = sqlite3.connect(DB_PATH)
    
    # 이미 있는 공고번호
    existing = set(r[0] for r in conn.execute("SELECT bidNtceNo FROM bid_notices_raw").fetchall())
    print(f"기존 bid_notices_raw: {len(existing):,}건\n")
    
    insert_sql = """INSERT OR IGNORE INTO bid_notices_raw 
        (bidNtceNo, bidNtceOrd, bidNtceNm, dminsttCd, dminsttNm,
         cnstrtsiteRgnNm, bidNtceDt, type, rgnLmtInfo) 
        VALUES (?,?,?,?,?,?,?,?,?)"""
    
    grand_total = 0
    
    for year, month in generate_months():
        # 해당 월의 날짜 범위
        _, last_day = calendar.monthrange(year, month)
        start_d = datetime.date(year, month, 1)
        end_d = datetime.date(year, month, last_day)
        
        # 이미 이 월에 데이터가 충분히 있는지 체크
        prefix1 = f"{year}{month:02d}"
        prefix2 = f"R{str(year)[2:]}"
        cnt = conn.execute(f"""SELECT COUNT(*) FROM bid_notices_raw 
            WHERE (bidNtceNo LIKE '{prefix1}%' OR bidNtceNo LIKE '{prefix2}%')
            AND bidNtceDt LIKE '{year}-{month:02d}%'""").fetchone()[0]
        if cnt > 100:
            print(f"  [{year}-{month:02d}] 이미 {cnt:,}건 있음 → 건너뜀")
            continue
        
        print(f"\n{'='*50}")
        print(f"  [{year}-{month:02d}] 수집 시작 ({start_d} ~ {end_d})")
        print(f"{'='*50}")
        
        month_new = 0
        month_skip = 0
        batch = []
        
        curr = start_d
        while curr <= end_d:
            ds = curr.strftime("%Y%m%d")
            items = collect_day(ds)
            
            for item in items:
                if item['bidNtceNo'] in existing:
                    month_skip += 1
                    continue
                existing.add(item['bidNtceNo'])
                batch.append((item['bidNtceNo'], item.get('bidNtceOrd',''),
                             item['bidNtceNm'], item.get('dminsttCd',''),
                             item.get('dminsttNm',''), item.get('cnstrtsiteRgnNm',''),
                             item.get('bidNtceDt',''), item.get('type',''),
                             item.get('rgnLmtInfo','')))
                month_new += 1
            
            curr += datetime.timedelta(days=1)
            time.sleep(0.8)
        
        # 월 단위 저장
        if batch:
            conn.executemany(insert_sql, batch)
            conn.commit()
        
        grand_total += month_new
        print(f"  ✅ [{year}-{month:02d}] 완료: 신규 {month_new:,}건, 중복 {month_skip:,}건")
        print(f"     누적 신규: {grand_total:,}건")
    
    total = conn.execute("SELECT COUNT(*) FROM bid_notices_raw").fetchone()[0]
    print(f"\n{'='*50}")
    print(f"  🎉 전체 완료! 신규 {grand_total:,}건 추가")
    print(f"     bid_notices_raw 총: {total:,}건")
    print(f"{'='*50}")
    conn.close()

if __name__ == '__main__':
    main()
