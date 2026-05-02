import os
import urllib.request
import json
import ssl
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = os.environ.get('SERVICE_KEY', '')

conn = sqlite3.connect('procurement_contracts.db')
cursor = conn.cursor()
# 1월 계약건 중 20건 선택 (오래된 계약)
cursor.execute("SELECT untyCntrctNo FROM cnstwk_cntrct WHERE cntrctDate LIKE '2026-01-%' LIMIT 20")
rows = cursor.fetchall()
conn.close()

success_count = 0

for r in rows:
    unty_no = r[0]
    cntrct_no = unty_no[:11] if unty_no and len(unty_no) >= 11 else ''
    chg_ord = unty_no[11:] if unty_no and len(unty_no) > 11 else '00'
    
    url = f'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkServcInfo?serviceKey={SERVICE_KEY}&inqryDiv=2&cntrctNo={cntrct_no}&cntrctChgOrd={chg_ord}&type=json'
    
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=ctx, timeout=10) as res:
            text = res.read().decode('utf-8')
            data = json.loads(text)
            items = data.get('response', {}).get('body', {}).get('items', [])
            if items:
                print(f"=====================================")
                print(f"[성공] 원본계약번호: {unty_no}")
                print(f"공사현장(cnstwkFld): {items[0].get('cnstwkFld')}")
                print(f"현장소재지(cnstwkdRgnAdrs): {items[0].get('cnstwkdRgnAdrs')}")
                success_count += 1
                if success_count >= 3: # 3건만 성공하면 스탑
                    break
    except Exception:
        pass

if success_count == 0:
    print("1월 데이터 20건에서도 상세 정보가 조회되지 않았습니다.")
