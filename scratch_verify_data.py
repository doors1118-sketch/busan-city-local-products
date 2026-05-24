import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

script = r'''
import os, json, urllib.request, ssl, sqlite3

with open('/opt/busan/.env') as f:
    for line in f:
        line = line.strip()
        if line and '=' in line:
            k,v = line.split('=',1)
            os.environ.setdefault(k,v)

KEY = os.environ.get('SERVICE_KEY','')
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# 검증 대상 날짜 (성공적으로 적재된 최근 5일)
test_dates = ['20260521','20260520','20260519','20260518','20260517']

# 1. DB 적재 건수 확인
conn = sqlite3.connect('/opt/busan/procurement_contracts.db')
print("=== DB 적재 건수 (부산 필터 후) ===")
for tbl in ['cnstwk_cntrct','servc_cntrct','thng_cntrct']:
    for d in test_dates:
        # rgstDt 기준으로 해당 날짜 건수
        cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl} WHERE rgstDt LIKE '{d[:4]}-{d[4:6]}-{d[6:8]}%'").fetchone()[0]
        if cnt > 0:
            print(f"  {tbl} / {d}: {cnt}건")
conn.close()

# 2. 새 API(inqryDiv=1)에서 totalCount 확인 (전국 기준)
print("\n=== API totalCount (전국, inqryDiv=1) ===")
apis = {
    'thng':    'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThng',
    'cnstwk':  'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwk',
    'servc':   'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServc',
    'thng_PPSSrch':   'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThngPPSSrch',
    'cnstwk_PPSSrch': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkPPSSrch',
    'servc_PPSSrch':  'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch',
}

for d in test_dates[:3]:  # 최근 3일만
    bgn = f"{d}0000"
    end = f"{d}2359"
    print(f"\n--- {d} ---")
    for name, base_url in apis.items():
        # PPSSrch는 inqryDiv=1이 계약체결일자 기준
        url = f"{base_url}?serviceKey={KEY}&numOfRows=1&pageNo=1&type=json&inqryDiv=1&inqryBgnDt={bgn}&inqryEndDt={end}"
        try:
            req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0'})
            with urllib.request.urlopen(req, context=ctx, timeout=15) as res:
                data = json.loads(res.read().decode('utf-8'))
                hdr = data.get('response',{}).get('header',{})
                if hdr.get('resultCode') == '00':
                    total = data.get('response',{}).get('body',{}).get('totalCount',0)
                    print(f"  {name:20s}: {total:>6,}건 (전국)")
                else:
                    err_hdr = data.get('nkoneps.com.response.ResponseError',{}).get('header',{})
                    print(f"  {name:20s}: ERROR {err_hdr.get('resultCode')} {err_hdr.get('resultMsg','')}")
        except Exception as e:
            print(f"  {name:20s}: FAIL {e}")
'''

stdin, stdout, stderr = client.exec_command(
    f"cat > /tmp/verify_data.py << 'PYEOF'\n{script}\nPYEOF\n/opt/busan/venv/bin/python3 /tmp/verify_data.py",
    timeout=120
)
print(stdout.read().decode('utf-8', 'replace'))
err = stderr.read().decode('utf-8', 'replace').strip()
if err:
    print(f"STDERR: {err}")

client.close()
