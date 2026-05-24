import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'

# 문서의 샘플 URL 패턴:
# getCntrctInfoListThng?inqryDiv=1&inqryBgnDt=201605010000&inqryEndDt=201605052359
# inqryDiv=1은 등록일시 기준, inqryDiv=2는 통합계약번호 기준
# 날짜 형식: YYYYMMDDHHMM (12자리)

script = f'''
import urllib.request, urllib.error

key = "{KEY}"

tests = [
    # 1. inqryDiv=1 + 12자리 날짜 (문서 형식)
    ("div1_12digit",
     f"https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThng?serviceKey={{key}}&numOfRows=1&pageNo=1&type=json&inqryDiv=1&inqryBgnDt=202605200000&inqryEndDt=202605202359"),
    # 2. inqryDiv=2 + 8자리 날짜 (기존 방식)
    ("div2_8digit",
     f"https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThng?serviceKey={{key}}&numOfRows=1&pageNo=1&type=json&inqryDiv=2&inqryBgnDt=20260520&inqryEndDt=20260520"),
    # 3. inqryDiv=2 + 12자리 날짜
    ("div2_12digit",
     f"https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThng?serviceKey={{key}}&numOfRows=1&pageNo=1&type=json&inqryDiv=2&inqryBgnDt=202605200000&inqryEndDt=202605202359"),
    # 4. 서버 코드의 실제 헬스체크 URL 패턴 재현
    ("healthcheck",
     f"https://apis.data.go.kr/1230000/ao/UsrInfoService02/getDminsttInfo02?serviceKey={{key}}&inqryDiv=2&numOfRows=1&pageNo=1&type=json"),
    # 5. PPSSrch (중앙조달 검색 - 서버가 실제 사용하는 것)
    ("PPSSrch_thng",
     f"https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThngPPSSrch?serviceKey={{key}}&numOfRows=1&pageNo=1&type=json&inqryDiv=1&inqryBgnDt=202605200000&inqryEndDt=202605202359"),
]

for label, url in tests:
    url = url.format(key=key)
    try:
        resp = urllib.request.urlopen(url, timeout=15)
        data = resp.read().decode("utf-8")[:300]
        print(f"[{{label}}] HTTP {{resp.status}} => {{data}}")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8","replace")[:200]
        print(f"[{{label}}] HTTP {{e.code}} {{e.reason}} => {{body}}")
    except Exception as e:
        print(f"[{{label}}] Error: {{e}}")
    print()
'''

stdin, stdout, stderr = client.exec_command(
    f"cat > /tmp/test_params.py << 'PYEOF'\n{script}\nPYEOF\npython3 /tmp/test_params.py",
    timeout=60
)
print(stdout.read().decode('utf-8', 'replace'))
err = stderr.read().decode('utf-8', 'replace').strip()
if err:
    print(f"STDERR: {err}")

client.close()
