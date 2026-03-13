import urllib.request
import re
import ssl
import sys

sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

url = 'https://www.data.go.kr/data/15058815/openapi.do'
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
try:
    with urllib.request.urlopen(req, context=ctx) as response:
        html = response.read().decode('utf-8')
        
        # We can extract all property names and descriptions using simple regex from the swagger/json part if present
        # In data.go.kr, usually there's a JSON string embedded for the API definitions.
        matches = re.finditer(r'"([a-zA-Z0-9_]+)"\s*:\s*\{\s*"type"\s*:\s*"[^"]+"\s*,\s*"description"\s*:\s*"([^"]+)"', html)
        
        results = {}
        for m in matches:
            results[m.group(1)] = m.group(2)
            
        print('--- API Response Fields matching 사업자, 업체, 상호, 낙찰 ---')
        count = 0
        for k, v in results.items():
            if any(term in v for term in ['사업자', '업체', '상호', '낙찰']):
                print(f'{k}: {v}')
                count += 1
        print(f'\nTotal matching fields: {count}')

except Exception as e:
    print(f'Error: {e}')
