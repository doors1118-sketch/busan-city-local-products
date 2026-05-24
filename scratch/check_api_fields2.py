import paramiko, sys
sys.stdout.reconfigure(encoding='utf-8')

HOST = "49.50.133.160"
USER = "root"
PASSWORD = "back9900@@"
SERVICE_KEY = "c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, username=USER, password=PASSWORD, timeout=15)

# 인라인 Python 스크립트를 서버에 임시 파일로 올려서 실행
script = """
import requests, xml.etree.ElementTree as ET, os
url = 'https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getMASCntrctPrdctInfoList'
key = os.environ['SHOPPING_MALL_PRDCT_SERVICE_KEY']
resp = requests.get(url, params={'serviceKey': key, 'numOfRows': '3', 'pageNo': '1'}, timeout=30)
root = ET.fromstring(resp.content)
items = root.findall('.//item')
tc = root.findtext('.//totalCount')
print('totalCount:', tc)
print('items:', len(items))
for i, item in enumerate(items):
    print()
    print('=== Item', i+1, '===')
    for child in item:
        print(' ', child.tag, ':', child.text)
"""

# 서버에 임시 스크립트 작성
stdin, stdout, stderr = ssh.exec_command(f"cat > /tmp/check_fields.py << 'PYEOF'\n{script}\nPYEOF")
stdout.read()

cmd = f"cd /opt/busan && SHOPPING_MALL_PRDCT_SERVICE_KEY={SERVICE_KEY} /opt/busan/venv/bin/python3 /tmp/check_fields.py"
stdin, stdout, stderr = ssh.exec_command(cmd, timeout=30)
print(stdout.read().decode())
err = stderr.read().decode()
if err: print("stderr:", err)

ssh.close()
