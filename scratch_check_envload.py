import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

commands = [
    # 1. 크론탭에서 .env 로딩 방식 확인
    "crontab -u busan-monitor -l 2>/dev/null | grep daily_pipeline",
    # 2. daily_pipeline_sync.py가 SERVICE_KEY를 어떻게 로드하는지
    "head -30 /opt/busan/daily_pipeline_sync.py | grep -i 'SERVICE_KEY\\|env\\|dotenv'",
    # 3. .env 파일 내용 (hexdump로 정확히)
    "xxd /opt/busan/.env | head -5",
    # 4. systemd EnvironmentFile 형식 테스트 (bash에서 export)
    "export $(cat /opt/busan/.env | xargs) && echo SERVICE_KEY=$SERVICE_KEY | head -c 30",
    # 5. 파이프라인을 직접 실행할 때 환경변수 전달 테스트
    """cd /opt/busan && SERVICE_KEY=$(grep SERVICE_KEY .env | cut -d= -f2) /opt/busan/venv/bin/python3 -c "
import os
k = os.environ.get('SERVICE_KEY','')
print(f'KEY len={len(k)}, first8={k[:8]}')
" """,
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
