import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'

# SERVICE_KEY를 직접 환경변수로 전달하여 수동 실행
cmd = f"cd /opt/busan && SERVICE_KEY={KEY} /opt/busan/venv/bin/python3 daily_pipeline_sync.py 20260522 >> /opt/busan/sync_log/daily.log 2>&1 &"

stdin, stdout, stderr = client.exec_command(cmd, timeout=10)
print("실행 시작:", stdout.read().decode('utf-8','replace'))
print("STDERR:", stderr.read().decode('utf-8','replace'))

client.close()
print("파이프라인 수동 실행 시작 (SERVICE_KEY 직접 전달)")
