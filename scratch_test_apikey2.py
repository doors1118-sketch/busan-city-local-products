import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'

script = f'''
import urllib.request, urllib.error, urllib.parse

key = "{KEY}"

tests = [
    ("thng", "https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusThng"),
    ("cnstwk", "https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusCnstwk"),
    ("servc", "https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusServc"),
]

for nm, base in tests:
    params = urllib.parse.urlencode({{
        "serviceKey": key, "numOfRows": "1", "pageNo": "1",
        "type": "json", "inqryDiv": "2",
        "inqryBgnDt": "20260520", "inqryEndDt": "20260520"
    }})
    url = base + "?" + params
    try:
        resp = urllib.request.urlopen(url, timeout=10)
        data = resp.read().decode("utf-8")[:200]
        print(nm, "HTTP", resp.status, data[:150])
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:200]
        print(nm, "HTTP", e.code, e.reason, body)
    except Exception as e:
        print(nm, "Error:", e)
'''

# 서버에 스크립트 파일 쓰고 실행
stdin, stdout, stderr = client.exec_command(f"cat > /tmp/test_api.py << 'PYEOF'\n{script}\nPYEOF\npython3 /tmp/test_api.py", timeout=30)
print(stdout.read().decode('utf-8', 'replace'))
err = stderr.read().decode('utf-8', 'replace').strip()
if err:
    print(f"STDERR: {err}")

client.close()
