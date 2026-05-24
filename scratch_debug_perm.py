import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# daily_pipeline_sync.py를 import하여 SERVICE_KEY 확인
cmd = """cd /opt/busan && /opt/busan/venv/bin/python3 -c "
import sys
sys.path.insert(0, '.')
# daily_pipeline_sync.py의 상단만 실행하여 SERVICE_KEY 확인
import importlib.util
spec = importlib.util.spec_from_file_location('dps', '/opt/busan/daily_pipeline_sync.py')
# 실제로 import하면 전체가 실행되므로, 대신 파일에서 SERVICE_KEY 로딩 부분만 재현

import os
# __file__ = '/opt/busan/daily_pipeline_sync.py' 시뮬레이션
_env_path = os.path.join(os.path.dirname(os.path.abspath('/opt/busan/daily_pipeline_sync.py')), '.env')
print(f'_env_path = {_env_path}')
print(f'exists = {os.path.exists(_env_path)}')
if os.path.exists(_env_path):
    with open(_env_path, encoding='utf-8') as f:
        content = f.read()
        print(f'content len={len(content)}')
        print(f'content: {repr(content[:80])}')

# os.environ 초기화 없이 직접 로딩 테스트
for line in content.strip().split(chr(10)):
    line = line.strip()
    if line and not line.startswith('#') and '=' in line:
        k, v = line.split('=', 1)
        os.environ.setdefault(k.strip(), v.strip())
        print(f'Set: {k.strip()} = {v.strip()[:10]}... (via setdefault)')

key = os.environ.get('SERVICE_KEY','')
print(f'Final SERVICE_KEY: len={len(key)}')
"
"""

stdin, stdout, stderr = client.exec_command(cmd, timeout=15)
print(stdout.read().decode('utf-8', 'replace'))
err = stderr.read().decode('utf-8', 'replace').strip()
if err and 'apport' not in err:
    print(f"STDERR: {err}")

# 또한 busan-monitor 유저로 실행해보기
cmd2 = "sudo -u busan-monitor bash -c 'cd /opt/busan && /opt/busan/venv/bin/python3 -c \"import os; exec(open(chr(46)+chr(101)+chr(110)+chr(118)).read().replace(chr(61),chr(10)).split(chr(10))[0]); print(os.path.exists(chr(46)+chr(101)+chr(110)+chr(118)))\"' 2>/dev/null || echo 'sudo failed'"
stdin, stdout, stderr = client.exec_command(cmd2, timeout=10)

# 더 간단한 테스트: busan-monitor로 직접 daily_pipeline head 실행
cmd3 = "sudo -u busan-monitor bash -c 'cd /opt/busan && /opt/busan/venv/bin/python3 -c \"import os; print(os.path.exists(\\'/opt/busan/.env\\'))\"'"
stdin, stdout, stderr = client.exec_command(cmd3, timeout=10)
print("\n[busan-monitor .env access]", stdout.read().decode('utf-8','replace').strip())

client.close()
