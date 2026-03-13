import urllib.request
import json
import ssl
import sys

sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

service_key = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'
base_url = 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkServcInfo'

params_to_test = [
    # 조달청 주요 날짜 파라미터 총출동
    f'?serviceKey={service_key}&inqryDiv=1&inqryBgnDt=20260101&inqryEndDt=20260110&numOfRows=5&pageNo=1&type=json',
    f'?serviceKey={service_key}&inqryBgnDate=20260101&inqryEndDate=20260110&numOfRows=5&pageNo=1&type=json',
    f'?serviceKey={service_key}&cntrctBgnDate=20260101&cntrctEndDate=20260110&numOfRows=5&pageNo=1&type=json',
    f'?serviceKey={service_key}&rgstBgnDate=20260101&rgstEndDate=20260110&numOfRows=5&pageNo=1&type=json',
    f'?serviceKey={service_key}&chgBgnDate=20260101&chgEndDate=20260110&numOfRows=5&pageNo=1&type=json',
    f'?serviceKey={service_key}&inqryBgnDt=20260101&inqryEndDt=20260110&numOfRows=5&pageNo=1&type=json',
    f'?serviceKey={service_key}&searchBgnDate=20260101&searchEndDate=20260110&numOfRows=5&pageNo=1&type=json',
    # 특정 계약번호(단건 쿼리)가 필수인지 확인하기 위한 파라미터
    f'?serviceKey={service_key}&dcsnCntrctNo=R25TA0117392900&type=json',
    f'?serviceKey={service_key}&untyCntrctNo=R25TE09875269&type=json',
    f'?serviceKey={service_key}&cntrctRefNo=R25TA01173929&type=json'
]

print("Testing Date Parameters for getCntrctInfoListCnstwkServcInfo...\n")
for params in params_to_test:
    try:
        req = urllib.request.Request(base_url + params, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            header = data.get('response', {}).get('header', {})
            body = data.get('response', {}).get('body', {})
            
            p_parts = params.split('&')
            p_display = ' & '.join(p_parts[1:3])
            
            if header.get('resultCode') == '00':
                count = body.get('totalCount')
                print(f"[SUCCESS] Params: {p_display} -> totalCount: {count}")
                if count and int(count) > 0:
                    items = body.get('items', [])
                    print("  - Available Fields:", list(items[0].keys()))
            else:
                print(f"[FAIL] Params: {p_display} -> Code: {header.get('resultCode')}, Msg: {header.get('resultMsg')}")
    except Exception as e:
        print(f"[ERROR] Params: {p_display} -> {e}")
