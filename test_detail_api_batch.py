import os
import sqlite3
import urllib.request
import json
import ssl
import sys
import time

sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = os.environ.get('SERVICE_KEY', '')

conn = sqlite3.connect('procurement_contracts.db')

# ntceNo가 비어있는 공사계약 10건 랜덤 추출 (장기계속계약 가능성 높음)
c = conn.cursor()
c.execute("""
    SELECT untyCntrctNo, cnstwkNm, cntrctInsttNm 
    FROM cnstwk_cntrct 
    WHERE cntrctDate >= '2026-01-01' AND cntrctDate <= '2026-01-31'
      AND (ntceNo IS NULL OR ntceNo = '')
    LIMIT 10
""")
rows = c.fetchall()
conn.close()

print(f"🔍 ntceNo가 비어있는 공사계약 {len(rows)}건 상세 API 테스트")

success = 0
fail = 0

for r in rows:
    unty = r[0]
    name = r[1][:30] if r[1] else ''
    instt = r[2][:15] if r[2] else ''
    
    url = f'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkServcInfo?serviceKey={SERVICE_KEY}&untyCntrctNo={unty}&numOfRows=10&pageNo=1&type=json'
    
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=ctx, timeout=10) as res:
            text = res.read().decode('utf-8')
            data = json.loads(text)
            header = data.get('response', {}).get('header', {})
            
            if header.get('resultCode') == '00':
                items = data.get('response', {}).get('body', {}).get('items', [])
                if items:
                    rgn = items[0].get('cnstrtsiteRgnNm', 'N/A')
                    print(f"  ✅ {unty} | {instt} | {name} -> 현장: {rgn}")
                    success += 1
                else:
                    print(f"  ⚠️ {unty} | {instt} | {name} -> items 비어있음")
                    fail += 1
            else:
                print(f"  ❌ {unty} | {instt} | {name} -> 에러: {header.get('resultMsg')}")
                fail += 1
    except Exception as e:
        print(f"  ❌ {unty} | {instt} | {name} -> 호출실패: {e}")
        fail += 1
    
    time.sleep(0.3)

print(f"\n📊 결과: 성공 {success}건 / 실패 {fail}건 (총 {len(rows)}건)")
