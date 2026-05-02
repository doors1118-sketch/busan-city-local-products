import os
import sqlite3
import urllib.request
import json
import ssl
import sys

sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = os.environ.get('SERVICE_KEY', '')

conn = sqlite3.connect('procurement_contracts.db')
c = conn.cursor()
c.execute("SELECT untyCntrctNo, dcsnCntrctNo, cntrctRefNo FROM cnstwk_cntrct WHERE cnstwkNm LIKE '%울릉공항%' AND cntrctDate >= '2026-01-01'")
rows = c.fetchall()
conn.close()

print(f"울릉공항 DB 내역: {len(rows)}건")
for r in rows:
    print(f"  untyCntrctNo={r[0]}, dcsnCntrctNo={r[1]}, cntrctRefNo={r[2]}")

# 각 번호로 상세 API 호출 시도
for r in rows:
    unty = r[0]
    
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
                    item = items[0]
                    print(f"\n✅ [{unty}] API 호출 성공!")
                    print(f"  - 공사현장지역명(cnstrtsiteRgnNm): {item.get('cnstrtsiteRgnNm')}")
                    print(f"  - 공사현장주소(cnstwkdRgnAdrs): {item.get('cnstwkdRgnAdrs')}")
                    print(f"  - 공사현장(cnstwkFld): {item.get('cnstwkFld')}")
                    print(f"  - 계약명: {item.get('cnstwkNm')}")
                else:
                    print(f"\n⚠️ [{unty}] items 비어있음")
            else:
                print(f"\n❌ [{unty}] API 에러: {header.get('resultMsg')}")
    except Exception as e:
        print(f"\n❌ [{unty}] 호출 실패: {e}")
