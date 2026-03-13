"""
bid_notices_price 공사 건에 업종분류 필드 백필 수집 (v3)
================================================
mainCnsttyNm (주공종명: 건축공사업, 전기공사업 등)
mtltyAdvcPsblYnCnstwkNm (종합/전문 구분: 종합공사-신설공사 등)

v3 변경: 진행상황 페이지별 표시, 플러시 강제
"""
import urllib.request, json, ssl, sqlite3, sys, time, datetime, os

# 강제 플러시 출력
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', encoding='utf-8', buffering=1)

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'
DB = 'procurement_contracts.db'

SLEEP_BETWEEN_PAGES = 1.2
SLEEP_BETWEEN_MONTHS = 1.0
SLEEP_ON_429 = 180
MAX_RETRIES = 5

conn = sqlite3.connect(DB, timeout=60)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA busy_timeout=60000")

# 컬럼 존재 확인
for col in ['mainCnsttyNm', 'mtltyAdvcPsblYnCnstwkNm']:
    try:
        conn.execute(f"ALTER TABLE bid_notices_price ADD COLUMN {col} TEXT DEFAULT ''")
    except:
        pass
conn.commit()

# 현재 상태
filled_main = conn.execute(
    "SELECT COUNT(*) FROM bid_notices_price WHERE mainCnsttyNm != '' AND mainCnsttyNm IS NOT NULL AND sector='공사'"
).fetchone()[0]
filled_mtl = conn.execute(
    "SELECT COUNT(*) FROM bid_notices_price WHERE mtltyAdvcPsblYnCnstwkNm != '' AND mtltyAdvcPsblYnCnstwkNm IS NOT NULL AND sector='공사'"
).fetchone()[0]
total = conn.execute("SELECT COUNT(*) FROM bid_notices_price WHERE sector='공사'").fetchone()[0]
print(f"시작 상태: mainCnsttyNm {filled_main:,}/{total:,} ({filled_main/total*100:.1f}%), mtltyAdvc {filled_mtl:,}/{total:,} ({filled_mtl/total*100:.1f}%)")

# 미채워진 월 확인 (mtltyAdvcPsblYnCnstwkNm 기준 - 더 높은 커버리지)
unfilled = conn.execute("""
    SELECT substr(bidNtceDt, 1, 7) ym, COUNT(*) cnt
    FROM bid_notices_price 
    WHERE sector='공사' AND (mtltyAdvcPsblYnCnstwkNm IS NULL OR mtltyAdvcPsblYnCnstwkNm = '')
    GROUP BY ym ORDER BY ym
""").fetchall()

if not unfilled:
    print("mtltyAdvcPsblYnCnstwkNm 전부 채워짐!")
    conn.close()
    sys.exit(0)

print(f"미채워진 월: {len(unfilled)}개")
for ym, cnt in unfilled:
    print(f"  {ym}: {cnt:,}건")

# 수집
start = datetime.date(2020, 1, 1)
end = datetime.date(2026, 3, 31)
current = start
updated_total = 0
api_calls = 0
t0 = time.time()

while current <= end:
    if current.month == 12:
        next_month = datetime.date(current.year + 1, 1, 1)
    else:
        next_month = datetime.date(current.year, current.month + 1, 1)
    last_day = next_month - datetime.timedelta(days=1)
    ym = current.strftime('%Y-%m')

    unfilled_cnt = conn.execute("""
        SELECT COUNT(*) FROM bid_notices_price 
        WHERE sector='공사' AND substr(bidNtceDt, 1, 7) = ?
        AND (mtltyAdvcPsblYnCnstwkNm IS NULL OR mtltyAdvcPsblYnCnstwkNm = '')
    """, (ym,)).fetchone()[0]

    if unfilled_cnt == 0:
        current = next_month
        continue

    start_str = current.strftime('%Y%m%d') + '0000'
    end_str = last_day.strftime('%Y%m%d') + '2359'
    
    print(f"\n[{ym}] 미채워진 {unfilled_cnt:,}건 수집 시작...", flush=True)
    page_no = 1
    month_updated = 0

    while True:
        url = (f'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwk'
               f'?serviceKey={KEY}&inqryDiv=1'
               f'&inqryBgnDt={start_str}&inqryEndDt={end_str}'
               f'&numOfRows=100&pageNo={page_no}&type=json')

        retry = 0
        success = False
        total_count = 0
        while retry < MAX_RETRIES:
            try:
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                res = urllib.request.urlopen(req, context=ctx, timeout=30)
                data = json.loads(res.read().decode('utf-8'))
                api_calls += 1

                header = data.get('response', {}).get('header', {})
                if header.get('resultCode') != '00':
                    break

                body = data.get('response', {}).get('body', {})
                items = body.get('items', [])
                total_count = int(body.get('totalCount', 0))
                
                if not items:
                    success = True
                    break

                batch = []
                for item in items:
                    ntce_no = item.get('bidNtceNo', '')
                    main = item.get('mainCnsttyNm', '') or ''
                    cnstwk_type = item.get('mtltyAdvcPsblYnCnstwkNm', '') or ''
                    if ntce_no and (main or cnstwk_type):
                        batch.append((main, cnstwk_type, ntce_no))

                if batch:
                    conn.executemany("""UPDATE bid_notices_price 
                        SET mainCnsttyNm = CASE WHEN ? != '' THEN ? ELSE mainCnsttyNm END,
                            mtltyAdvcPsblYnCnstwkNm = CASE WHEN ? != '' THEN ? ELSE mtltyAdvcPsblYnCnstwkNm END
                        WHERE bidNtceNo=? AND sector='공사'""",
                        [(m, m, c, c, n) for m, c, n in batch])
                    month_updated += len(batch)

                success = True
                break

            except Exception as e:
                err_str = str(e)
                if '429' in err_str:
                    retry += 1
                    wait = SLEEP_ON_429
                    print(f"  429! {wait}초 대기 (시도{retry}/{MAX_RETRIES})...", flush=True)
                    time.sleep(wait)
                    continue
                else:
                    retry += 1
                    print(f"  Error p{page_no}: {err_str[:60]} (시도{retry})", flush=True)
                    time.sleep(10)
                    continue

        if not success:
            print(f"  FAIL page {page_no}", flush=True)
            break

        # 진행상황
        if page_no % 10 == 0:
            print(f"  p{page_no}/{(total_count+99)//100} ({month_updated:,}건)", flush=True)

        if total_count > 0 and page_no * 100 >= total_count:
            break

        page_no += 1
        time.sleep(SLEEP_BETWEEN_PAGES)

    conn.commit()
    updated_total += month_updated
    elapsed = time.time() - t0
    print(f"  => {ym}: {month_updated:,}건 / 누적 {updated_total:,}건 ({elapsed:.0f}s)", flush=True)

    current = next_month
    time.sleep(SLEEP_BETWEEN_MONTHS)

conn.commit()

# 최종
filled2_main = conn.execute(
    "SELECT COUNT(*) FROM bid_notices_price WHERE mainCnsttyNm != '' AND mainCnsttyNm IS NOT NULL AND sector='공사'"
).fetchone()[0]
filled2_mtl = conn.execute(
    "SELECT COUNT(*) FROM bid_notices_price WHERE mtltyAdvcPsblYnCnstwkNm != '' AND mtltyAdvcPsblYnCnstwkNm IS NOT NULL AND sector='공사'"
).fetchone()[0]
total2 = conn.execute("SELECT COUNT(*) FROM bid_notices_price WHERE sector='공사'").fetchone()[0]

print(f"\n{'='*60}")
print(f"완료! mainCnsttyNm: {filled2_main:,}/{total2:,} ({filled2_main/total2*100:.1f}%)")
print(f"     mtltyAdvc: {filled2_mtl:,}/{total2:,} ({filled2_mtl/total2*100:.1f}%)")
print(f"     업데이트: {updated_total:,}건, API: {api_calls}회, {time.time()-t0:.0f}초")

# 분포
print("\n=== mainCnsttyNm Top 10 ===")
for r in conn.execute("""SELECT mainCnsttyNm, COUNT(*) c FROM bid_notices_price 
    WHERE sector='공사' AND mainCnsttyNm != '' GROUP BY mainCnsttyNm ORDER BY c DESC LIMIT 10""").fetchall():
    print(f"  {r[0]}: {r[1]:,}건")

print("\n=== mtltyAdvcPsblYnCnstwkNm ===")
for r in conn.execute("""SELECT mtltyAdvcPsblYnCnstwkNm, COUNT(*) c FROM bid_notices_price 
    WHERE sector='공사' AND mtltyAdvcPsblYnCnstwkNm != '' GROUP BY mtltyAdvcPsblYnCnstwkNm ORDER BY c DESC""").fetchall():
    print(f"  {r[0]}: {r[1]:,}건")

conn.close()
