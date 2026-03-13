import urllib.request
import json
import ssl
import sys
from urllib.parse import quote

sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'
busan = quote('부산광역시')

apis = [
    ('용역 낙찰 (부산 지역제한)', 
     f'https://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusServcPPSSrch?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt=202601010000&inqryEndDt=202601152359&numOfRows=3&pageNo=1&type=json&prtcptLmtRgnNm={busan}'),
    ('물품 낙찰 (부산 지역제한)',
     f'https://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusThngPPSSrch?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt=202601010000&inqryEndDt=202601152359&numOfRows=3&pageNo=1&type=json&prtcptLmtRgnNm={busan}'),
    ('용역 낙찰 (필터없이 비교용)',
     f'https://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusServcPPSSrch?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt=202601010000&inqryEndDt=202601152359&numOfRows=1&pageNo=1&type=json'),
]

for name, url in apis:
    print(f"\n{'='*60}")
    print(f"[{name}]")
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        res = urllib.request.urlopen(req, context=ctx, timeout=10)
        data = json.loads(res.read().decode('utf-8'))
        body = data.get('response', {}).get('body', {})
        items = body.get('items', [])
        total = body.get('totalCount', 0)
        print(f"  totalCount: {total}")
        if items:
            for i, item in enumerate(items):
                print(f"\n  --- 건 {i+1} ---")
                print(f"  공고번호: {item.get('bidNtceNo')}")
                print(f"  공고명: {item.get('bidNtceNm')}")
                print(f"  수요기관: {item.get('dminsttNm')}")
                print(f"  낙찰업체: {item.get('bidwinnrNm')}")
                print(f"  낙찰업체 주소: {item.get('bidwinnrAdrs')}")
                print(f"  낙찰금액: {item.get('sucsfbidAmt')}")
                # 응답에 prtcptLmtRgnNm이 있는지 확인!
                for k, v in item.items():
                    kl = k.lower()
                    if any(x in kl for x in ['rgn', 'lmt', 'prtcpt', 'area', 'locplc']):
                        print(f"  🔴 {k}: {v}")
        else:
            print("  (데이터 없음)")
    except Exception as e:
        print(f"  ❌ Error: {e}")
