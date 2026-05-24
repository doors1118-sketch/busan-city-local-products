import paramiko, sys
from scp import SCPClient
import os
sys.stdout.reconfigure(encoding='utf-8')

HOST = "49.50.133.160"
USER = "root"
PASSWORD = "back9900@@"
SERVICE_KEY = "c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865"
LOCAL_DIR = os.path.dirname(os.path.abspath(__file__)).replace('\\scratch', '')

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, username=USER, password=PASSWORD, timeout=15)

# 1. 업데이트된 파일 업로드
print("[1/2] import_mas_product_api.py 업로드...")
scp = SCPClient(ssh.get_transport())
scp.put(os.path.join(LOCAL_DIR, "import_mas_product_api.py"), "/opt/busan/import_mas_product_api.py")
scp.close()
print("  ✅ 업로드 완료")

# 2. probe 테스트
print("[2/2] Probe 테스트...")
cmd = f"cd /opt/busan && SHOPPING_MALL_PRDCT_SERVICE_KEY={SERVICE_KEY} CHATBOT_DB=/opt/busan/chatbot_company.db /opt/busan/venv/bin/python3 import_mas_product_api.py --probe"
stdin, stdout, stderr = ssh.exec_command(cmd, timeout=60)
out = stdout.read().decode().strip()
err = stderr.read().decode().strip()
print(f"  {out}")
if err: print(f"  stderr: {err}")

ssh.close()
