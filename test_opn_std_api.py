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

url = f'https://apis.data.go.kr/1230000/PubDataOpnStdService/getDataSetOpnStdPblancInfo?serviceKey={SERVICE_KEY}&pageNo=1&numOfRows=10&type=json&inqryBgnDt=202601010000&inqryEndDt=202601152359'

try:
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    res = urllib.request.urlopen(req, context=ctx, timeout=10)
    data = json.loads(res.read().decode('utf-8'))
    items = data.get('response', {}).get('body', {}).get('items', [])
    if items:
        print("🔍 [공개표준 입찰공고 API 결과]")
        for item in items:
            print(f"{item.get('bidNtceNo')} | {item.get('bidNtceNm')[:20]}")
            print(f"  - rgnLmtYn: {item.get('rgnLmtYn')}")
            print(f"  - prtcptPsblRgnNm: {item.get('prtcptPsblRgnNm')}")
    else:
        print("결과 없음")
except Exception as e:
    print(e)
