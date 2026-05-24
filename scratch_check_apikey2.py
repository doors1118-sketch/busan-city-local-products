import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

commands = [
    # 1. .env 파일 존재 여부 + 권한 + 크기
    "ls -la /opt/busan/.env 2>/dev/null; wc -l /opt/busan/.env 2>/dev/null",
    # 2. .env 내용 (키 마스킹)
    """python3 -c "
with open('/opt/busan/.env') as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith('#'):
            print(line)
            continue
        if '=' in line:
            k, v = line.split('=', 1)
            k = k.strip()
            v = v.strip().strip(\"'\").strip('\"')
            if len(v) > 20:
                print(f'{k} = {v[:8]}...{v[-8:]} (len={len(v)})')
            else:
                print(f'{k} = {v}')
        else:
            print(line)
" """,
    # 3. busan-monitor 유저의 환경변수에 SERVICE_KEY 있는지
    "sudo -u busan-monitor bash -c 'echo SERVICE_KEY=$SERVICE_KEY' 2>/dev/null",
    # 4. busan-monitor의 .bashrc/.profile에 키가 있는지
    "grep -i 'SERVICE_KEY\\|SERVICEKEY' /home/busan-monitor/.bashrc /home/busan-monitor/.profile /home/busan-monitor/.bash_profile 2>/dev/null || echo 'no key in profile'",
    # 5. 실제 API 호출 테스트 - .env 로드 후 호출
    """cd /opt/busan && python3 -c "
import os, urllib.request, urllib.error

# .env 로드
with open('/opt/busan/.env') as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            os.environ[k.strip()] = v.strip().strip(chr(39)).strip(chr(34))

key = os.environ.get('SERVICE_KEY', '')
print(f'SERVICE_KEY loaded: {bool(key)}, len={len(key)}')

if key:
    # 간단한 API 호출 테스트
    test_url = f'https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusThng?serviceKey={key}&numOfRows=1&pageNo=1&type=json&inqryDiv=2&inqryBgnDt=20260520&inqryEndDt=20260520'
    try:
        req = urllib.request.Request(test_url)
        resp = urllib.request.urlopen(req, timeout=10)
        print(f'HTTP {resp.status}: API 호출 성공')
        data = resp.read().decode('utf-8')[:200]
        print(data)
    except urllib.error.HTTPError as e:
        print(f'HTTP {e.code}: {e.reason}')
        print(e.read().decode('utf-8','replace')[:300])
    except Exception as e:
        print(f'Error: {e}')
else:
    print('SERVICE_KEY가 비어있음!')
" """,
]

for i, cmd in enumerate(commands, 1):
    print(f"\n[{i}] {'='*50}")
    stdin, stdout, stderr = client.exec_command(cmd, timeout=20)
    out = stdout.read().decode('utf-8', 'replace').strip()
    err = stderr.read().decode('utf-8', 'replace').strip()
    if out:
        print(out)
    if err and 'apport' not in err:
        print(f"STDERR: {err}")

client.close()
