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
# 공사계약 랜덤 10건
cursor.execute("SELECT untyCntrctNo, cntrctRefNo, dcsnCntrctNo, cmmnCntrctYn FROM cnstwk_cntrct LIMIT 10")
rows = cursor.fetchall()
conn.close()

success_count = 0

for r in rows:
    unty_no = r[0]
    cntrct_no = unty_no[:11] if unty_no and len(unty_no) >= 11 else ''
    chg_ord = unty_no[11:] if unty_no and len(unty_no) > 11 else '00'
    
    # 두 가지 호출 방식
    url1 = f'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkServcInfo?serviceKey={SERVICE_KEY}&inqryDiv=2&cntrctNo={cntrct_no}&type=json'
    url2 = f'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkServcInfo?serviceKey={SERVICE_KEY}&inqryDiv=2&cntrctNo={cntrct_no}&cntrctChgOrd={chg_ord}&type=json'
    
    for idx, url in enumerate([url1, url2]):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, context=ctx, timeout=10) as res:
                text = res.read().decode('utf-8')
                data = json.loads(text)
                items = data.get('response', {}).get('body', {}).get('items', [])
                if items:
                    print(f"=====================================")
                    print(f"[성공] 방식{idx+1} | 원본계약번호: {unty_no}")
                    print(f"공사현장(cnstwkFld): {items[0].get('cnstwkFld')}")
                    print(f"현장소재지(cnstwkdRgnAdrs): {items[0].get('cnstwkdRgnAdrs')}")
                    # 다른 주요 정보가 있는지
                    keys = list(items[0].keys())
                    print(f"제공 필드 목록 미리보기: {keys[:10]}...")
                    success_count += 1
                    break
        except Exception:
            pass

if success_count == 0:
    print("모든 테스트가 실패했거나 현장 정보가 없습니다.")
