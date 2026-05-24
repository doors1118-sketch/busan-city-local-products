import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

script = r'''
import urllib.request, urllib.error

key = "c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865"

# 1. 키 없이 호출 (401이 나와야 정상)
# 2. 잘못된 키로 호출 (401이 나와야 정상)
# 3. 제공된 키로 호출
# 4. 다른 공공데이터 API로 테스트 (조달청 서버 문제인지 확인)

tests = [
    ("no_key", "https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusThng?serviceKey=&numOfRows=1&pageNo=1&type=json&inqryDiv=2&inqryBgnDt=20260520&inqryEndDt=20260520"),
    ("bad_key", "https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusThng?serviceKey=INVALID_KEY_TEST&numOfRows=1&pageNo=1&type=json&inqryDiv=2&inqryBgnDt=20260520&inqryEndDt=20260520"),
    ("user_key", f"https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusThng?serviceKey={key}&numOfRows=1&pageNo=1&type=json&inqryDiv=2&inqryBgnDt=20260520&inqryEndDt=20260520"),
    ("old_date", f"https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusThng?serviceKey={key}&numOfRows=1&pageNo=1&type=json&inqryDiv=2&inqryBgnDt=20260501&inqryEndDt=20260501"),
    ("bid_api", f"https://apis.data.go.kr/1230000/BidPublicInfoService04/getBidPblancListInfoThngPPSSrch01?serviceKey={key}&numOfRows=1&pageNo=1&type=json&inqryBgnDt=202605010000&inqryEndDt=202605012359"),
]

for label, url in tests:
    try:
        resp = urllib.request.urlopen(url, timeout=15)
        data = resp.read().decode("utf-8")[:200]
        print(f"[{label}] HTTP {resp.status} => {data[:150]}")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:200]
        print(f"[{label}] HTTP {e.code} {e.reason} => {body}")
    except Exception as e:
        print(f"[{label}] Error: {e}")
'''

stdin, stdout, stderr = client.exec_command(
    f"cat > /tmp/test_api2.py << 'PYEOF'\n{script}\nPYEOF\npython3 /tmp/test_api2.py",
    timeout=60
)
print(stdout.read().decode('utf-8', 'replace'))
err = stderr.read().decode('utf-8', 'replace').strip()
if err:
    print(f"STDERR: {err}")

client.close()
