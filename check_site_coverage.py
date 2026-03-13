# -*- coding: utf-8 -*-
"""
공사 조달요청 API 수집 (2025.10~12) → 계약DB 복합키 매칭 테스트
두 API 모두 수집 (Cnstwk + PPS)
"""
import urllib.request, json, ssl, sqlite3, sys, time
from datetime import datetime, timedelta
sys.stdout.reconfigure(encoding='utf-8')
import os; os.chdir(r'c:\Users\COMTREE\Desktop\연습')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SK = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'

APIS = [
    ('Cnstwk', 'https://apis.data.go.kr/1230000/ao/PrcrmntReqInfoService/getPrcrmntReqInfoListCnstwk'),
    ('PPS', 'https://apis.data.go.kr/1230000/ao/PrcrmntReqInfoService/getPrcrmntReqInfoListCnstwkPPSSrch'),
]

# 수집
all_items = []
seen = set()

for api_name, base in APIS:
    print(f"\n{'=' * 60}")
    print(f"  [{api_name}] 수집 중 (2025.10~12)")
    print(f"{'=' * 60}")
    
    for month in [10, 11, 12]:
        if month == 12:
            end_day = 31
        elif month in [10]:
            end_day = 31
        else:
            end_day = 30
        
        m_start = f'2025{month:02d}010000'
        m_end = f'2025{month:02d}{end_day}2359'
        
        page = 1
        month_count = 0
        
        while True:
            url = f'{base}?serviceKey={SK}&inqryDiv=1&inqryBgnDt={m_start}&inqryEndDt={m_end}&numOfRows=100&pageNo={page}&type=json'
            try:
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, context=ctx, timeout=15) as resp:
                    data = json.loads(resp.read().decode('utf-8'))
                    body = data.get('response', {}).get('body', {})
                    cnt = int(body.get('totalCount', 0))
                    items = body.get('items', [])
                    
                    if not items:
                        break
                    
                    for item in items:
                        rno = str(item.get('prcrmntReqNo', ''))
                        if rno and rno not in seen:
                            seen.add(rno)
                            all_items.append({
                                'prcrmntReqNo': rno,
                                'prcrmntReqNm': str(item.get('prcrmntReqNm', '')),
                                'orderInsttCd': str(item.get('orderInsttCd', '')),
                                'orderInsttNm': str(item.get('orderInsttNm', '')),
                                'cnstrtsiteRgnNm': str(item.get('cnstrtsiteRgnNm', '')),
                                'totCnstwkScleAmt': str(item.get('totCnstwkScleAmt', '')),
                                'rcptDt': str(item.get('rcptDt', '')),
                                'api': api_name,
                            })
                            month_count += 1
                    
                    if page * 100 >= cnt:
                        break
                    page += 1
                    time.sleep(0.3)
            except Exception as e:
                print(f"    에러: {e}")
                break
        
        print(f"  2025-{month:02d}: {month_count:,}건 (총 {cnt})")
        time.sleep(0.2)

print(f"\n  총 수집: {len(all_items):,}건")

# 현장소재지 분포
site_dist = {}
for item in all_items:
    s = item['cnstrtsiteRgnNm'] or '(빈값)'
    site_dist[s] = site_dist.get(s, 0) + 1
print(f"\n  현장소재지 Top 10:")
for s, c in sorted(site_dist.items(), key=lambda x: -x[1])[:10]:
    print(f"    {s}: {c}건")

busan_items = [i for i in all_items if '부산' in i['cnstrtsiteRgnNm']]
print(f"\n  부산 현장: {len(busan_items):,}건")

# ============================================================
# 계약DB 매칭 테스트
# ============================================================
print(f"\n{'=' * 60}")
print(f"  계약DB 매칭 테스트")
print(f"{'=' * 60}")

conn = sqlite3.connect('procurement_contracts.db')

# 1) reqNo 직접 매칭
req_nos = set(i['prcrmntReqNo'] for i in all_items)
db_reqs = set(r[0] for r in conn.execute(
    "SELECT DISTINCT reqNo FROM cnstwk_cntrct WHERE reqNo IS NOT NULL AND reqNo != ''"
).fetchall())

direct_match = req_nos & db_reqs
print(f"\n  1) reqNo 직접 매칭: {len(direct_match):,}건")

# 매칭된 건 중 수의계약 비율
if direct_match:
    suui_matched = 0
    for rn in direct_match:
        row = conn.execute("SELECT cntrctCnclsMthdNm FROM cnstwk_cntrct WHERE reqNo = ? LIMIT 1", (rn,)).fetchone()
        if row and '수의' in str(row[0]):
            suui_matched += 1
    print(f"     그 중 수의계약: {suui_matched:,}건")

# 2) 기관명+공사명 텍스트 매칭
print(f"\n  2) 기관명+공사명 복합 매칭 (수의계약+ntceNo없음 대상)...")

# 계약DB에서 수의계약+ntceNo없음 로드
suui_rows = conn.execute("""
    SELECT cnstwkNm, cntrctInsttNm, totCntrctAmt, thtmCntrctAmt, 
           cntrctCnclsDate, dminsttCd, untyCntrctNo, reqNo
    FROM cnstwk_cntrct 
    WHERE cntrctCnclsMthdNm LIKE '%수의%' 
    AND (ntceNo IS NULL OR ntceNo = '')
    AND cntrctCnclsDate >= '2025-09-01'
    AND cntrctCnclsDate <= '2026-01-31'
""").fetchall()
print(f"     대상 수의계약(2025.09~2026.01): {len(suui_rows):,}건")

# 매칭 시도: 공사명이 동일하거나 포함관계
matched_pairs = []
for item in all_items:
    req_nm = item['prcrmntReqNm'].strip()
    if not req_nm:
        continue
    
    for row in suui_rows:
        db_nm = str(row[0] or '').strip()
        if not db_nm:
            continue
        
        # 공사명 비교: 포함관계 또는 동일
        if req_nm == db_nm or (len(req_nm) > 5 and req_nm in db_nm) or (len(db_nm) > 5 and db_nm in req_nm):
            matched_pairs.append({
                'req_no': item['prcrmntReqNo'],
                'req_nm': req_nm,
                'req_site': item['cnstrtsiteRgnNm'],
                'req_agency': item['orderInsttNm'],
                'db_nm': db_nm,
                'db_agency': str(row[1] or ''),
                'db_amt': row[2],
                'db_unty': row[6],
                'db_reqno': row[7],
            })

print(f"     공사명 매칭: {len(matched_pairs):,}건")

# 매칭 결과 샘플
if matched_pairs:
    print(f"\n  === 매칭 샘플 (최대 10건) ===")
    for p in matched_pairs[:10]:
        print(f"    조달요청: {p['req_nm'][:40]}")
        print(f"      현장: {p['req_site']}, 기관: {p['req_agency'][:25]}")
        print(f"    계약DB:  {p['db_nm'][:40]}")
        print(f"      기관: {p['db_agency'][:25]}, reqNo: {p['db_reqno']}")
        print()
    
    # 부산 현장 매칭 건
    busan_matched = [p for p in matched_pairs if '부산' in p['req_site']]
    non_busan = [p for p in matched_pairs if p['req_site'] and '부산' not in p['req_site']]
    print(f"  부산 현장 매칭: {len(busan_matched):,}건")
    print(f"  타지역 현장 매칭: {len(non_busan):,}건")

conn.close()
print("\n완료!")
