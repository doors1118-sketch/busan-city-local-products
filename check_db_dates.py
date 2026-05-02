import os
"""공사 공고 API 응답에서 업종분류 필드 확인"""
import urllib.request, json, ssl, sys
sys.stdout.reconfigure(encoding='utf-8')
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

KEY = os.environ.get('SERVICE_KEY', '')

# 기존 스크립트와 동일한 URL 사용
url = (f'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwk'
       f'?serviceKey={KEY}&inqryDiv=1&inqryBgnDt=202601010000&inqryEndDt=202601312359'
       f'&numOfRows=2&pageNo=1&type=json')

req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
res = urllib.request.urlopen(req, context=ctx, timeout=15)
data = json.loads(res.read().decode('utf-8'))
items = data.get('response', {}).get('body', {}).get('items', [])

if items:
    item = items[0]
    print("=== 공사 공고 전체 필드 ===")
    for k, v in sorted(item.items()):
        mark = ' ★' if any(x in k.lower() for x in ['indstry','clsfc','bsns','div','lcns']) else ''
        print(f"  {k}: {str(v)[:80]}{mark}")
else:
    print("데이터 없음")
