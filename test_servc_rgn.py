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
url='https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServcPPSSrch?serviceKey=' + SERVICE_KEY + '&inqryDiv=1&inqryBgnDt=202601010000&inqryEndDt=202601152359&numOfRows=50&pageNo=1&type=json'

try:
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    res = urllib.request.urlopen(req, context=ctx, timeout=10)
    data = json.loads(res.read().decode('utf-8'))
    items = data.get('response', {}).get('body', {}).get('items', [])
    if items:
        for item in items:
            fields = []
            for k, v in item.items():
                if ('rgn' in k.lower() or 'lmt' in k.lower() or 'locplc' in k.lower()) and v and str(v).strip() != 'N' and str(v).strip() != '0':
                    fields.append(f"{k}={v}")
            if fields:
                print(f"[{item.get('bidNtceNo')} / {item.get('bidNtceNm')[:15]}]: " + ", ".join(fields))
    else:
        print('No items')
except Exception as e:
    print(e)
