import urllib.request
import json
import ssl
import sys

sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'

# 공사 입찰공고 API에서 필드 확인
url = f'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwk?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt=202601010000&inqryEndDt=202601012359&numOfRows=1&pageNo=1&type=json'

req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
with urllib.request.urlopen(req, context=ctx, timeout=10) as res:
    data = json.loads(res.read().decode('utf-8'))
    items = data.get('response', {}).get('body', {}).get('items', [])
    if items:
        print("🔍 [공사 입찰공고 API 전체 필드 목록]")
        for k, v in items[0].items():
            marker = "★" if 'rgn' in k.lower() or 'limit' in k.lower() or 'lmt' in k.lower() or 'area' in k.lower() or 'locplc' in k.lower() else " "
            print(f"  {marker} {k}: {str(v)[:60]}")

# 용역 입찰공고 API도 확인
print("\n")
url2 = f'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServc?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt=202601010000&inqryEndDt=202601012359&numOfRows=1&pageNo=1&type=json'

req2 = urllib.request.Request(url2, headers={'User-Agent': 'Mozilla/5.0'})
with urllib.request.urlopen(req2, context=ctx, timeout=10) as res2:
    data2 = json.loads(res2.read().decode('utf-8'))
    items2 = data2.get('response', {}).get('body', {}).get('items', [])
    if items2:
        print("🔍 [용역 입찰공고 API - 지역제한 관련 필드만]")
        for k, v in items2[0].items():
            if 'rgn' in k.lower() or 'lmt' in k.lower() or 'locplc' in k.lower() or 'area' in k.lower():
                print(f"  ★ {k}: {str(v)[:80]}")
