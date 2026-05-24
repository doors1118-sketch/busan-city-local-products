import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

commands = [
    # 1. git pull
    "cd /opt/busan && git pull origin main --ff-only",
    # 2. .env 확인
    "cat /opt/busan/.env",
    # 3. 헬스체크 테스트 (수정된 코드로)
    """cd /opt/busan && . .env && /opt/busan/venv/bin/python3 -c "
import os, urllib.request, urllib.error, json, ssl
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
key = os.environ.get('SERVICE_KEY','')
print(f'KEY loaded: len={len(key)}')
url = f'https://apis.data.go.kr/1230000/ao/UsrInfoService02/getDminsttInfo02?serviceKey={key}&inqryDiv=1&inqryBgnDt=202601010000&inqryEndDt=202601010100&numOfRows=1&pageNo=1&type=json'
try:
    req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0'})
    with urllib.request.urlopen(req, context=ctx, timeout=10) as res:
        data = json.loads(res.read().decode('utf-8'))
        code = data.get('response',{}).get('header',{}).get('resultCode')
        msg = data.get('response',{}).get('header',{}).get('resultMsg')
        print(f'Health check: code={code} msg={msg}')
except Exception as e:
    print(f'Error: {e}')
" """,
    # 4. API 서버 재시작
    "systemctl restart busan-api && sleep 2 && systemctl status busan-api --no-pager | head -10",
]

for i, cmd in enumerate(commands, 1):
    print(f"\n[{i}] {'='*50}")
    stdin, stdout, stderr = client.exec_command(cmd, timeout=30)
    out = stdout.read().decode('utf-8', 'replace').strip()
    err = stderr.read().decode('utf-8', 'replace').strip()
    if out:
        print(out)
    if err and 'apport' not in err:
        print(f"STDERR: {err}")

client.close()
