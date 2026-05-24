import paramiko, sys, os
from scp import SCPClient
sys.stdout.reconfigure(encoding='utf-8')

HOST = "49.50.133.160"
USER = "root"
PASSWORD = "back9900@@"
LOCAL_DIR = os.path.dirname(os.path.abspath(__file__)).replace('\\scratch', '')

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, username=USER, password=PASSWORD, timeout=15)

scp = SCPClient(ssh.get_transport())
print("Uploading updated nts_batch_sync.py...")
scp.put(os.path.join(LOCAL_DIR, "nts_batch_sync.py"), "/opt/busan/nts_batch_sync.py")
scp.close()

# nts_batch_sync 쿼리 카운트 재확인
cmd = f"cd /opt/busan && source /opt/busan/.env && CHATBOT_DB=/opt/busan/chatbot_company.db /opt/busan/venv/bin/python3 nts_batch_sync.py --dry-run"
stdin, stdout, stderr = ssh.exec_command(cmd, timeout=30)
print(stdout.read().decode())
err = stderr.read().decode()
if err: print("stderr:", err)

ssh.close()
