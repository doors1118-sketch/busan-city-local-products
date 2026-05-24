import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'

# 헬스체크 URL을 수정된 코드와 동일하게 직접 테스트
cmd = f"""cd /opt/busan && /opt/busan/venv/bin/python3 -c "
import urllib.request, urllib.error, json, ssl, os
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

key = '{KEY}'
print(f'Using key directly: len={{len(key)}}')

# 1. 헬스체크 URL (수정 후)
url1 = f'https://apis.data.go.kr/1230000/ao/UsrInfoService02/getDminsttInfo02?serviceKey={{key}}&inqryDiv=1&inqryBgnDt=202601010000&inqryEndDt=202601010100&numOfRows=1&pageNo=1&type=json'
try:
    req = urllib.request.Request(url1, headers={{'User-Agent':'Mozilla/5.0'}})
    with urllib.request.urlopen(req, context=ctx, timeout=10) as res:
        data = json.loads(res.read().decode('utf-8'))
        code = data.get('response',{{}}).get('header',{{}}).get('resultCode')
        msg = data.get('response',{{}}).get('header',{{}}).get('resultMsg')
        print(f'[healthcheck] code={{code}} msg={{msg}}')
except urllib.error.HTTPError as e:
    print(f'[healthcheck] HTTP {{e.code}} {{e.reason}}')
except Exception as e:
    print(f'[healthcheck] Error: {{e}}')

# 2. 계약 API (성공했던 것)
url2 = f'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThng?serviceKey={{key}}&numOfRows=1&pageNo=1&type=json&inqryDiv=1&inqryBgnDt=202605200000&inqryEndDt=202605202359'
try:
    req = urllib.request.Request(url2, headers={{'User-Agent':'Mozilla/5.0'}})
    with urllib.request.urlopen(req, context=ctx, timeout=10) as res:
        data = json.loads(res.read().decode('utf-8'))
        code = data.get('response',{{}}).get('header',{{}}).get('resultCode')
        msg = data.get('response',{{}}).get('header',{{}}).get('resultMsg')
        cnt = data.get('response',{{}}).get('body',{{}}).get('totalCount',0)
        print(f'[CntrctInfo] code={{code}} msg={{msg}} total={{cnt}}')
except urllib.error.HTTPError as e:
    print(f'[CntrctInfo] HTTP {{e.code}} {{e.reason}}')
except Exception as e:
    print(f'[CntrctInfo] Error: {{e}}')

# 3. 환경변수 SERVICE_KEY 확인
env_key = os.environ.get('SERVICE_KEY','')
print(f'[env] SERVICE_KEY len={{len(env_key)}}')
"
"""

stdin, stdout, stderr = client.exec_command(cmd, timeout=30)
print(stdout.read().decode('utf-8', 'replace'))
err = stderr.read().decode('utf-8', 'replace').strip()
if err and 'apport' not in err:
    print(f"STDERR: {err}")

client.close()
