import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

# 서버에서 .env 로더 + SERVICE_KEY가 실제로 로딩되는지 확인
cmd = """cd /opt/busan && /opt/busan/venv/bin/python3 -c "
import os, sys
# .env 로더 (daily_pipeline_sync.py 코드 동일)
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
print(f'_env_path = {_env_path}')
print(f'exists = {os.path.exists(_env_path)}')

# 실제 __file__ 값 확인
print(f'__file__ = {__file__}')
print(f'abspath = {os.path.abspath(__file__)}')
print(f'dirname = {os.path.dirname(os.path.abspath(__file__))}')

if os.path.exists(_env_path):
    with open(_env_path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                os.environ.setdefault(k.strip(), v.strip())
                print(f'Loaded: {k.strip()} = {v.strip()[:8]}...')

key = os.environ.get('SERVICE_KEY', '')
print(f'SERVICE_KEY len={len(key)}')
"
"""

stdin, stdout, stderr = client.exec_command(cmd, timeout=15)
print(stdout.read().decode('utf-8', 'replace'))
err = stderr.read().decode('utf-8', 'replace').strip()
if err and 'apport' not in err:
    print(f"STDERR: {err}")

# 서버 daily_pipeline_sync.py의 .env 로더 코드 확인
stdin, stdout, stderr = client.exec_command("head -40 /opt/busan/daily_pipeline_sync.py", timeout=10)
print("\n=== Server code head ===")
print(stdout.read().decode('utf-8', 'replace'))

client.close()
