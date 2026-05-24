import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# check_api_health()의 로직을 그대로 재현
cmd = """cd /opt/busan && /opt/busan/venv/bin/python3 << 'PYEOF'
import os, sys, urllib.request, json, ssl

# .env 로더
_env_path = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])) if sys.argv[0] else '.', '.env')
# 직접 로드
with open('/opt/busan/.env', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            os.environ.setdefault(k.strip(), v.strip())

SERVICE_KEY = os.environ.get('SERVICE_KEY', '')
print(f"SERVICE_KEY: len={len(SERVICE_KEY)}, first8={SERVICE_KEY[:8]}")

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# check_api_health 재현
test_url = (f"https://apis.data.go.kr/1230000/ao/UsrInfoService02/getDminsttInfo02"
            f"?serviceKey={SERVICE_KEY}&inqryDiv=1"
            f"&inqryBgnDt=202601010000&inqryEndDt=202601010100"
            f"&numOfRows=1&pageNo=1&type=json")

print(f"URL length: {len(test_url)}")
print(f"URL serviceKey param: ...{test_url.split('serviceKey=')[1][:20]}...")

for attempt in range(3):
    try:
        req = urllib.request.Request(test_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=ctx, timeout=10) as res:
            raw = res.read().decode('utf-8')
            data = json.loads(raw)
            code = data.get('response', {}).get('header', {}).get('resultCode')
            msg = data.get('response', {}).get('header', {}).get('resultMsg')
            print(f"Attempt {attempt+1}: code={code} msg={msg}")
            if code == '00':
                print("SUCCESS!")
                break
    except Exception as e:
        print(f"Attempt {attempt+1}: Exception type={type(e).__name__} msg={e}")
        import traceback
        traceback.print_exc()
PYEOF
"""

stdin, stdout, stderr = client.exec_command(cmd, timeout=30)
print(stdout.read().decode('utf-8', 'replace'))
err = stderr.read().decode('utf-8', 'replace').strip()
if err:
    print(f"STDERR: {err}")

client.close()
