import paramiko, sys
sys.stdout.reconfigure(encoding='utf-8')

HOST = "49.50.133.160"
USER = "root"
PASSWORD = "back9900@@"
SERVICE_KEY = "c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, username=USER, password=PASSWORD, timeout=15)

# 1일치(어제~오늘) 데이터만 드라이런으로 파싱되는 라벨 개수 확인
cmd = f"cd /opt/busan && SHOPPING_MALL_PRDCT_SERVICE_KEY={SERVICE_KEY} CHATBOT_DB=/opt/busan/chatbot_company.db /opt/busan/venv/bin/python3 import_mas_product_api.py --dry-run --days 1"
stdin, stdout, stderr = ssh.exec_command(cmd, timeout=120)
out = stdout.read().decode().strip()
err = stderr.read().decode().strip()
print(f"stdout:\\n{out}")
if err: print(f"stderr:\\n{err}")

ssh.close()
