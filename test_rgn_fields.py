import os
import urllib.request
import json
import ssl
import sys

sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = os.environ.get('SERVICE_KEY', '')

# 공사 입찰공고 API에서 필드 확인
url = f'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwk?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt=202601010000&inqryEndDt=202601152359&numOfRows=50&pageNo=1&type=json'

req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
try:
    with urllib.request.urlopen(req, context=ctx, timeout=10) as res:
        data = json.loads(res.read().decode('utf-8'))
        items = data.get('response', {}).get('body', {}).get('items', [])
        found = False
        print("🔍 [공사 입찰공고 API 지역제한 관련 필드]")
        for item in items:
            for k, v in item.items():
                if 'rgn' in k.lower() or 'lmt' in k.lower() or 'locplc' in k.lower() or 'area' in k.lower():
                    if v and str(v).strip() != 'N':
                        print(f"  [{item.get('bidNtceNo')} / {item.get('bidNtceNm')[:20]}] {k}: {v}")
                        found = True
        if not found:
            print("  지역제한관련 값이 있는(Y나 지역명) 필드를 50건 내에서 못찾음.")
except Exception as e:
    print(e)
