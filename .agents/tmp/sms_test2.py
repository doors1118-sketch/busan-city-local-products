import paramiko

HOST = "49.50.133.160"
USER = "root"
PASSWORD = "U7$B%U5843m"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, username=USER, password=PASSWORD, timeout=10)

remote_script = r"""
import sys, json, urllib.request, hashlib, hmac, base64, time, ssl

with open('/opt/busan/alert_config.json') as f:
    config = json.load(f)

sms_cfg = config.get('ncp_sms', {})
access_key = sms_cfg['access_key']
secret_key = sms_cfg['secret_key']
service_id = sms_cfg['service_id']
from_number = sms_cfg['from_number']
recipients = sms_cfg['recipients']

message = '[테스트] 부산 조달 경보 시스템 정상 작동 확인'

timestamp = str(int(time.time() * 1000))
uri = f'/sms/v2/services/{service_id}/messages'
sign_str = f"POST {uri}\n{timestamp}\n{access_key}"
signature = base64.b64encode(
    hmac.new(secret_key.encode('utf-8'), sign_str.encode('utf-8'), hashlib.sha256).digest()
).decode('utf-8')

body = json.dumps({
    "type": "SMS",
    "from": from_number,
    "content": message,
    "messages": [{"to": r.replace('-','')} for r in recipients],
}).encode('utf-8')

url = f"https://sens.apigw.ntruss.com{uri}"
req = urllib.request.Request(url, data=body, method='POST')
req.add_header('Content-Type', 'application/json; charset=utf-8')
req.add_header('x-ncp-apigw-timestamp', timestamp)
req.add_header('x-ncp-iam-access-key', access_key)
req.add_header('x-ncp-apigw-signature-v2', signature)

ctx = ssl.create_default_context()

try:
    with urllib.request.urlopen(req, timeout=30, context=ctx) as res:
        result = json.loads(res.read().decode('utf-8'))
        print(f"SUCCESS: status={result.get('statusCode')}, requestId={result.get('requestId')}")
except urllib.error.HTTPError as e:
    body_err = e.read().decode('utf-8')
    print(f"HTTP {e.code}: {e.reason}")
    print(f"Body: {body_err}")
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
"""

# Write and execute on server directly
stdin, stdout, stderr = ssh.exec_command(
    f"""cat << 'PYEOF' > /tmp/sms_test2.py
{remote_script}
PYEOF
cd /opt/busan && /opt/busan/venv/bin/python3 /tmp/sms_test2.py"""
)

import time
time.sleep(35)  # DNS timeout + 여유

print("STDOUT:", stdout.read().decode('utf-8'))
err = stderr.read().decode('utf-8')
if err:
    print("STDERR:", err)

ssh.close()
