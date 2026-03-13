import urllib.request
import json
import ssl
import sys

sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'

def check_fields(api_name, url):
    print(f"\n🔍 [{api_name} 지역제한 관련 필드]")
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=10) as res:
            data = json.loads(res.read().decode('utf-8'))
            items = data.get('response', {}).get('body', {}).get('items', [])
            for item in items:
                has_rgn = False
                for k, v in item.items():
                    if 'rgn' in k.lower() or 'lmt' in k.lower() or 'locplc' in k.lower() or 'area' in k.lower():
                        if v and str(v).strip() != 'N' and str(v).strip() != '0':
                            print(f"  [{item.get('bidNtceNo')} / {item.get('bidNtceNm')[:20]}] {k}: {v}")
                            has_rgn = True
            if not items:
                print("  데이터 없음")
    except Exception as e:
        print(f"  Error: {e}")

# 용역
url_servc = f'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServc?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt=202601010000&inqryEndDt=202601152359&numOfRows=20&pageNo=1&type=json'
check_fields("용역", url_servc)

# 물품
url_thng = f'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThng?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt=202601010000&inqryEndDt=202601152359&numOfRows=20&pageNo=1&type=json'
check_fields("물품", url_thng)
