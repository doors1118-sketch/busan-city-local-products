import os
import urllib.request
import json
import ssl
import sys

sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

service_key = os.environ.get('SERVICE_KEY', '')
base_url = 'https://apis.data.go.kr/1741000/StanOrgCd2/getStanOrgCdList2'

test_cases = [('부산본청', '1613281'), ('포항사무소', '1613306'), ('진주사무소', '1613305'), ('항공철도사고조사위', '1613432')]

print('--- 행정안전부 행정표준기관코드 소재지(법정동코드) 검증 ---')
for name, code in test_cases:
    params = f'?serviceKey={service_key}&org_cd={code}&type=json'
    try:
        req = urllib.request.Request(base_url + params, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            rows = data.get('StanOrgCd', [])[1].get('row', [])
            if rows:
                r = rows[0]
                loc = r.get('locatstd_cd', '')
                print(f"[{name}] {r.get('full_nm')}")
                print(f"  -> 법정동코드(locatstd_cd): '{loc}'")
            else:
                print(f'[{name}] Data not found.')
    except Exception as e:
        print(f'[{name}] Error: {e}')
