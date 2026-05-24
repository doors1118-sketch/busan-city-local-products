import paramiko, sys
sys.stdout.reconfigure(encoding='utf-8')

HOST = "49.50.133.160"
USER = "root"
PASSWORD = "back9900@@"
SERVICE_KEY = "c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, username=USER, password=PASSWORD, timeout=15)

# API 첫 페이지 1건만 가져와서 전체 필드 확인
cmd = f"""cd /opt/busan && SHOPPING_MALL_PRDCT_SERVICE_KEY={SERVICE_KEY} /opt/busan/venv/bin/python3 -c "
import requests, xml.etree.ElementTree as ET, os
url = 'https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getMASCntrctPrdctInfoList'
key = os.environ['SHOPPING_MALL_PRDCT_SERVICE_KEY']
resp = requests.get(url, params={{'serviceKey': key, 'numOfRows': '3', 'pageNo': '1'}}, timeout=30)
root = ET.fromstring(resp.content)
items = root.findall('.//item')
print(f'totalCount: {{root.findtext(\".//totalCount\")}}')
print(f'items: {{len(items)}}')
for i, item in enumerate(items):
    print(f'\\n=== Item {{i+1}} ===')
    for child in item:
        print(f'  {{child.tag}}: {{child.text}}')
"
"""

stdin, stdout, stderr = ssh.exec_command(cmd, timeout=30)
out = stdout.read().decode()
err = stderr.read().decode()
print(out)
if err and 'WARNING' not in err:
    print(f"stderr: {err}")

ssh.close()
