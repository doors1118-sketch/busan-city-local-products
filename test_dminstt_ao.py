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
base_url = 'https://apis.data.go.kr/1230000/ao/UsrInfoService02/getDminsttInfo02'

# dminsttCd=1613306 (포항), 1613281 (부산)
for t_name, t_code in [('포항사무소', '1613306'), ('부산총국', '1613281')]:
    test_url = f'{base_url}?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt=200001010000&inqryEndDt=202612312359&dminsttCd={t_code}&numOfRows=10&pageNo=1&type=json'
    try:
        req = urllib.request.Request(test_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
            res_str = response.read().decode('utf-8')
            res_json = json.loads(res_str)
            items = res_json.get('response', {}).get('body', {}).get('items', [])
            if items:
                for item in items:
                    print(f'\n[{t_name} 성공] {item.get("dminsttNm")} -> {item.get("adrs")}')
            else:
                hdr = res_json.get('response', {}).get('header', {}).get('resultMsg', '')
                print(f'\n[{t_name} 검색 성공이나 데이터 없음] {hdr}')
    except Exception as e:
        print(f'\n[{t_name} 에러] {e}')
