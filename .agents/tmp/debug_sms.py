import paramiko, json

HOST = "49.50.133.160"
USER = "root"
PASSWORD = "U7$B%U5843m"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, username=USER, password=PASSWORD, timeout=10)

# 1) alert_config.json 내용 확인
stdin, stdout, stderr = ssh.exec_command("cat /opt/busan/alert_config.json")
config_raw = stdout.read().decode('utf-8')
print("=== alert_config.json ===")
config = json.loads(config_raw)
sms = config.get('ncp_sms', {})
print(f"  enabled: {sms.get('enabled')}")
print(f"  service_id: {sms.get('service_id', '')[:10]}...")
print(f"  from_number: {sms.get('from_number')}")
print(f"  recipients: {sms.get('recipients')}")
print(f"  access_key len: {len(sms.get('access_key',''))}")
print(f"  secret_key len: {len(sms.get('secret_key',''))}")

# 2) 상세 디버그: 실제 API 호출 과정 + 응답 body까지 출력
remote_script = r"""
import sys, json, urllib.request, urllib.parse, hashlib, hmac, base64, time

sys.path.append('/opt/busan')
with open('/opt/busan/alert_config.json') as f:
    config = json.load(f)

sms_cfg = config.get('ncp_sms', {})
access_key = sms_cfg.get('access_key', '')
secret_key = sms_cfg.get('secret_key', '')
service_id = sms_cfg.get('service_id', '')
from_number = sms_cfg.get('from_number', '')
recipients = sms_cfg.get('recipients', [])

message = '[테스트] 부산 조달 경보 시스템 정상 작동 확인'

timestamp = str(int(time.time() * 1000))
uri = f'/sms/v2/services/{service_id}/messages'
sign_str = f"POST {uri}\n{timestamp}\n{access_key}"
signature = base64.b64encode(
    hmac.new(secret_key.encode('utf-8'), sign_str.encode('utf-8'), hashlib.sha256).digest()
).decode('utf-8')

body = {
    "type": "SMS",
    "from": from_number,
    "content": message,
    "messages": [{"to": r.replace('-','')} for r in recipients],
}

url = f"https://sens.apigw.ntruss.com{uri}"
data = json.dumps(body).encode('utf-8')

print(f"URL: {url}")
print(f"Body: {json.dumps(body, ensure_ascii=False)}")
print(f"Timestamp: {timestamp}")

req = urllib.request.Request(url, data=data, method='POST')
req.add_header('Content-Type', 'application/json; charset=utf-8')
req.add_header('x-ncp-apigw-timestamp', timestamp)
req.add_header('x-ncp-iam-access-key', access_key)
req.add_header('x-ncp-apigw-signature-v2', signature)

try:
    with urllib.request.urlopen(req, timeout=10) as res:
        result = json.loads(res.read().decode('utf-8'))
        print(f"SUCCESS: {result}")
except urllib.error.HTTPError as e:
    print(f"HTTP Error {e.code}: {e.reason}")
    print(f"Response body: {e.read().decode('utf-8')}")
except Exception as e:
    print(f"Error: {e}")
"""

stdin, stdout, stderr = ssh.exec_command(
    f"""cat << 'PYEOF' > /tmp/debug_sms.py
{remote_script}
PYEOF
cd /opt/busan && /opt/busan/venv/bin/python3 /tmp/debug_sms.py"""
)
print("\n=== SMS Debug ===")
print(stdout.read().decode('utf-8'))
err = stderr.read().decode('utf-8')
if err:
    print("STDERR:", err)

ssh.close()
