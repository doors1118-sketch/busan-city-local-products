import os
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

apis = {
    '물품(중앙)': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThngPPSSrch',
    '종합쇼핑몰': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListEtcPPSSrch',
}

for label, base_url in apis.items():
    url = f'{base_url}?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDate=20260301&inqryEndDate=20260301&numOfRows=1&pageNo=1&type=json'
    
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=ctx, timeout=10) as res:
            data = json.loads(res.read().decode('utf-8'))
            items = data.get('response', {}).get('body', {}).get('items', [])
            if items:
                print(f"🔍 [{label}] API 전체 필드 ({len(items[0])}개)")
                for k, v in sorted(items[0].items()):
                    marker = "💰" if 'amt' in k.lower() or 'prc' in k.lower() or 'mny' in k.lower() else "  "
                    print(f"  {marker} {k}: {str(v)[:70]}")
            else:
                print(f"❌ [{label}] items 비어있음")
    except Exception as e:
        print(f"❌ [{label}] 에러: {e}")
    
    time.sleep(1)
