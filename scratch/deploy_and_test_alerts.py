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
print("Uploading nts_batch_sync.py...")
scp.put(os.path.join(LOCAL_DIR, "nts_batch_sync.py"), "/opt/busan/nts_batch_sync.py")
print("Uploading alert_check.py...")
scp.put(os.path.join(LOCAL_DIR, "alert_check.py"), "/opt/busan/alert_check.py")
scp.close()

# nts_batch_sync 쿼리 카운트 확인
cmd = f"cd /opt/busan && source /opt/busan/.env && CHATBOT_DB=/opt/busan/chatbot_company.db /opt/busan/venv/bin/python3 nts_batch_sync.py --dry-run"
stdin, stdout, stderr = ssh.exec_command(cmd, timeout=30)
print(stdout.read().decode())
print(stderr.read().decode())

# alert_check.py 로컬 실행 시뮬레이션 (경보 발생 확인)
# 실제 전송을 막기 위해 텔레그램 설정이 없으므로 콘솔 로그만 봅니다.
cmd2 = f"cd /opt/busan && /opt/busan/venv/bin/python3 alert_check.py"
stdin2, stdout2, stderr2 = ssh.exec_command(cmd2, timeout=30)
print(stdout2.read().decode())

ssh.close()
