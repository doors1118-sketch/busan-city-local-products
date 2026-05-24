import paramiko, time

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# 기존 프로세스 전부 종료
client.exec_command("pkill -9 -f 'daily_pipeline_sync' 2>/dev/null")
time.sleep(2)

# strace로 확인: 어디서 block되는지
cmd = """cd /opt/busan && timeout 20 /opt/busan/venv/bin/python3 -c "
import os, sys, ssl, json, urllib.request, time

# .env 로드
with open('/opt/busan/.env') as f:
    for line in f:
        line = line.strip()
        if line and '=' in line:
            k,v = line.split('=',1)
            os.environ.setdefault(k,v)

SERVICE_KEY = os.environ.get('SERVICE_KEY','')
print(f'KEY: {len(SERVICE_KEY)}', flush=True)

# sys.stdout.reconfigure 테스트
sys.stdout.reconfigure(encoding='utf-8')
print('After reconfigure: OK', flush=True)

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# 헬스체크 재현 (정확히 check_api_health 코드)
test_url = (f'https://apis.data.go.kr/1230000/ao/UsrInfoService02/getDminsttInfo02'
            f'?serviceKey={SERVICE_KEY}&inqryDiv=1'
            f'&inqryBgnDt=202601010000&inqryEndDt=202601010100'
            f'&numOfRows=1&pageNo=1&type=json')
print(f'Calling healthcheck...', flush=True)
start = time.time()
for attempt in range(3):
    try:
        req = urllib.request.Request(test_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=ctx, timeout=10) as res:
            data = json.loads(res.read().decode('utf-8'))
            code = data.get('response', {}).get('header', {}).get('resultCode')
            if code == '00':
                print(f'Attempt {attempt+1}: OK ({time.time()-start:.1f}s)', flush=True)
                break
            msg = data.get('response', {}).get('header', {}).get('resultMsg', '')
            print(f'Attempt {attempt+1}: code={code} msg={msg}', flush=True)
            break
    except Exception as e:
        elapsed = time.time()-start
        print(f'Attempt {attempt+1}: {type(e).__name__}: {e} ({elapsed:.1f}s)', flush=True)
        if attempt < 2:
            time.sleep(2 * (attempt + 1))
print(f'Total: {time.time()-start:.1f}s', flush=True)
" 2>&1
"""

stdin, stdout, stderr = client.exec_command(cmd, timeout=60)
print(stdout.read().decode('utf-8', 'replace'))
err = stderr.read().decode('utf-8', 'replace').strip()
if err:
    print(f"STDERR: {err}")

client.close()
