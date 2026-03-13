import urllib.request
import json
import ssl

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

service_key = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'
base_url = 'https://apis.data.go.kr/1230000/ao/UsrInfoService02/getDminsttInfo02'

query_params = f'?serviceKey={service_key}&inqryDiv=1&inqryBgnDt=202401010000&inqryEndDt=202401312359&numOfRows=10&pageNo=1&type=json'

try:
    req = urllib.request.Request(base_url + query_params, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
        text = response.read().decode('utf-8')
        try:
            data = json.loads(text)
            header = data.get('response', {}).get('header', {})
            if header.get('resultCode') == '00':
                body = data.get('response', {}).get('body', {})
                total_count = body.get('totalCount')
                print(f'[SUCCESS] API returns totalCount: {total_count}')
                items = body.get('items', [])
                if items:
                    print('--- Sample Item ---')
                    for k, v in items[0].items():
                        if v:
                            print(f"{k}: {v}")
            else:
                print(f'[FAIL] Result: {header}')
        except json.JSONDecodeError:
            print("Response is not JSON:")
            print(text[:500])
except Exception as e:
    print('Error:', e)
