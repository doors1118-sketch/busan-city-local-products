"""용역 조달요청 API에서 현장지역(cnstrtsiteRgnNm) 수집 → DB 저장"""
import urllib.request, json, ssl, sqlite3, sys, time
from datetime import datetime, timedelta
sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SK = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'
BASE = 'https://apis.data.go.kr/1230000/ao/PrcrmntReqInfoService/getPrcrmntReqInfoListTechServc'
DB = 'servc_site.db'

conn = sqlite3.connect(DB, timeout=30)

# 테이블 생성
conn.execute("""CREATE TABLE IF NOT EXISTS servc_req_site (
    prcrmntReqNo TEXT PRIMARY KEY,
    prcrmntReqNm TEXT,
    cnstrtsiteRgnNm TEXT,
    orderInsttCd TEXT,
    orderInsttNm TEXT,
    rcptDt TEXT,
    totCnstwkScleAmt TEXT,
    cntrctCnclsMthdNm TEXT
)""")
conn.commit()

existing = set(r[0] for r in conn.execute("SELECT prcrmntReqNo FROM servc_req_site").fetchall())
print(f"기존 수집: {len(existing):,}건")

# 2020-01부터 2025-12까지 월별 수집
total_new = 0
total_api = 0

start = datetime(2020, 1, 1)
end = datetime(2025, 12, 31)
cur = start

while cur <= end:
    # 월 시작~끝
    m_start = cur.strftime('%Y%m01') + '0000'
    if cur.month == 12:
        m_end_dt = datetime(cur.year+1, 1, 1) - timedelta(days=1)
    else:
        m_end_dt = datetime(cur.year, cur.month+1, 1) - timedelta(days=1)
    m_end = m_end_dt.strftime('%Y%m%d') + '2359'
    
    page = 1
    month_count = 0
    
    while True:
        url = f'{BASE}?serviceKey={SK}&inqryDiv=1&inqryBgnDt={m_start}&inqryEndDt={m_end}&numOfRows=100&pageNo={page}&type=json'
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, context=ctx, timeout=15) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                body = data.get('response', {}).get('body', {})
                header = data.get('response', {}).get('header', {})
                
                if header.get('resultCode') not in ['00', None]:
                    print(f"  {cur.strftime('%Y-%m')} p{page}: code={header.get('resultCode')}, msg={header.get('resultMsg')}")
                    break
                
                cnt = int(body.get('totalCount', 0))
                items = body.get('items', [])
                
                if not items:
                    break
                
                for item in items:
                    req_no = str(item.get('prcrmntReqNo', '')).strip()
                    if not req_no or req_no in existing:
                        continue
                    conn.execute("""INSERT OR REPLACE INTO servc_req_site 
                        (prcrmntReqNo, prcrmntReqNm, cnstrtsiteRgnNm, orderInsttCd, orderInsttNm, rcptDt, totCnstwkScleAmt, cntrctCnclsMthdNm)
                        VALUES (?,?,?,?,?,?,?,?)""", (
                        req_no,
                        str(item.get('prcrmntReqNm', '')),
                        str(item.get('cnstrtsiteRgnNm', '')),
                        str(item.get('orderInsttCd', '')),
                        str(item.get('orderInsttNm', '')),
                        str(item.get('rcptDt', '')),
                        str(item.get('totCnstwkScleAmt', '')),
                        str(item.get('cntrctCnclsMthdNm', '')),
                    ))
                    existing.add(req_no)
                    month_count += 1
                
                total_api += 1
                
                if page * 100 >= cnt:
                    break
                page += 1
                time.sleep(0.3)
                
        except urllib.error.HTTPError as e:
            if e.code == 429:
                print(f"\n  ⚠️ 429 Rate Limited! {cur.strftime('%Y-%m')} p{page}")
                conn.commit()
                print(f"  저장 완료. 총 {total_new + month_count:,}건")
                conn.close()
                sys.exit(1)
            print(f"  {cur.strftime('%Y-%m')} p{page}: HTTP {e.code}")
            break
        except Exception as e:
            print(f"  {cur.strftime('%Y-%m')} p{page}: {e}")
            break
    
    conn.commit()
    total_new += month_count
    if month_count > 0:
        print(f"  {cur.strftime('%Y-%m')}: +{month_count:,}건 (API {total_api}회)")
    
    # 다음 달
    if cur.month == 12:
        cur = datetime(cur.year + 1, 1, 1)
    else:
        cur = datetime(cur.year, cur.month + 1, 1)
    
    time.sleep(0.2)

# 최종 결과
total = conn.execute("SELECT COUNT(*) FROM servc_req_site").fetchone()[0]
busan = conn.execute("SELECT COUNT(*) FROM servc_req_site WHERE cnstrtsiteRgnNm LIKE '%부산%'").fetchone()[0]
print(f"\n{'='*60}")
print(f"  완료! 총 {total:,}건 (신규 {total_new:,}건, API {total_api}회)")
print(f"  부산 현장: {busan:,}건")
print(f"{'='*60}")

print(f"\n=== 현장지역 Top 15 ===")
for r in conn.execute("""SELECT cnstrtsiteRgnNm, COUNT(*) c FROM servc_req_site 
    WHERE cnstrtsiteRgnNm != '' GROUP BY cnstrtsiteRgnNm ORDER BY c DESC LIMIT 15""").fetchall():
    print(f"  {r[0]}: {r[1]:,}")

conn.close()
