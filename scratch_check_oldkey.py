import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# 1. git log에서 이전 .env 기록 찾기
cmds = [
    # .env가 git에 있었는지
    "cd /opt/busan && git log --all --oneline -- .env 2>/dev/null | head -5",
    # daily_pipeline_sync.py에 기본값 하드코딩 있는지
    "grep -n 'SERVICE_KEY.*=' /opt/busan/daily_pipeline_sync.py | head -5",
    # .env.bak 또는 이전 백업
    "ls -la /opt/busan/.env* /opt/busan/env* 2>/dev/null",
    # bash_history에서 SERVICE_KEY 설정 흔적
    "grep -i 'SERVICE_KEY\\|\.env' /root/.bash_history 2>/dev/null | tail -20",
    # .env 변경 이력 (stat)
    "stat /opt/busan/.env 2>/dev/null",
    # 로컬에서 URL Encoding된 키로 테스트 (공공데이터포털은 URL-encoded key를 줄 때가 있음)
    """python3 -c "
import urllib.request, urllib.error, urllib.parse

# 원본 키 (hex string)
raw_key = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'

# URL encode 버전 테스트
encoded_key = urllib.parse.quote_plus(raw_key)
print(f'raw_key == encoded_key: {raw_key == encoded_key}')
print(f'key length: {len(raw_key)}')

# 혹시 이 키가 base64가 아닌 hex string인지 확인
# 공공데이터포털의 일반 키는 보통 URL-encoded base64로 50~100자
print(f'all hex chars: {all(c in \"0123456789abcdef\" for c in raw_key)}')
print()

# 기존 키 없이 빈 키로 호출했을 때와 비교
for label, k in [('new_key', raw_key), ('empty', '')]:
    url = f'https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusThng?serviceKey={k}&numOfRows=1&pageNo=1&type=json&inqryDiv=2&inqryBgnDt=20260520&inqryEndDt=20260520'
    try:
        resp = urllib.request.urlopen(url, timeout=10)
        print(f'[{label}] HTTP {resp.status}')
    except urllib.error.HTTPError as e:
        print(f'[{label}] HTTP {e.code} {e.reason}')
    except Exception as e:
        print(f'[{label}] Error: {e}')
"
""",
]

for i, cmd in enumerate(cmds, 1):
    print(f"\n[{i}] {'='*50}")
    stdin, stdout, stderr = client.exec_command(cmd, timeout=15)
    out = stdout.read().decode('utf-8', 'replace').strip()
    err = stderr.read().decode('utf-8', 'replace').strip()
    if out:
        print(out)
    if err and 'apport' not in err:
        print(f"STDERR: {err}")

client.close()
