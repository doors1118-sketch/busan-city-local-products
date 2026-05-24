import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

commands = [
    # 1. 환경변수 파일 (.env) 확인
    "cat /opt/busan/.env 2>/dev/null || echo 'NO .env file'",
    # 2. daily_pipeline_sync.py에서 API키 참조 방식 확인
    "grep -n -i 'service_key\\|servicekey\\|api_key\\|apikey\\|encoding\\|decoding' /opt/busan/daily_pipeline_sync.py 2>/dev/null | head -20",
    # 3. 환경변수에서 키 확인
    "cd /opt/busan && python3 -c \"import os; [print(k,'=',v[:20]+'...' if len(v)>20 else v) for k,v in os.environ.items() if 'KEY' in k.upper() or 'API' in k.upper() or 'SERVICE' in k.upper()]\" 2>/dev/null",
    # 4. venv activate에 키가 있는지
    "grep -i 'key\\|service' /opt/busan/venv/bin/activate 2>/dev/null | head -10 || echo 'no key in venv activate'",
    # 5. systemd 환경 파일
    "cat /etc/systemd/system/busan-api.service 2>/dev/null",
    # 6. 실제 API 호출 테스트 (헬스체크 재현)
    """cd /opt/busan && python3 -c "
import os, sys
sys.path.insert(0, '.')
# .env 로드 시도
env_file = '/opt/busan/.env'
if os.path.exists(env_file):
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                os.environ[k.strip()] = v.strip()

# 키 찾기
for k in ['SERVICE_KEY', 'SERVICEKEY', 'API_KEY', 'PPS_API_KEY', 'DATA_GO_KR_KEY']:
    v = os.environ.get(k, '')
    if v:
        print(f'{k} = {v[:15]}...{v[-10:]} (len={len(v)})')

# daily_pipeline_sync에서 키 로드 방식 확인
import importlib.util
spec = importlib.util.spec_from_file_location('dps', '/opt/busan/daily_pipeline_sync.py')
mod = importlib.util.module_from_spec(spec)
# 소스만 읽기
with open('/opt/busan/daily_pipeline_sync.py') as f:
    src = f.read()
import re
key_patterns = re.findall(r'(?:SERVICE_KEY|service_key|serviceKey|api_key|API_KEY)\s*=\s*.+', src)
for p in key_patterns[:5]:
    print('CODE:', p.strip()[:100])
" """,
    # 7. config 파일 확인
    "ls /opt/busan/config* /opt/busan/*.json 2>/dev/null | grep -v cache | grep -v node",
    "cat /opt/busan/config.json 2>/dev/null || cat /opt/busan/api_config.json 2>/dev/null || echo 'no config.json'",
]

for i, cmd in enumerate(commands, 1):
    print(f"\n[{i}] {'='*50}")
    stdin, stdout, stderr = client.exec_command(cmd, timeout=15)
    out = stdout.read().decode('utf-8', 'replace').strip()
    err = stderr.read().decode('utf-8', 'replace').strip()
    if out:
        print(out)
    if err and 'apport' not in err:
        print(f"STDERR: {err}")

client.close()
