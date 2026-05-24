import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

NEW_KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'

commands = [
    # 1. .env에 SERVICE_KEY 기록
    f"echo 'SERVICE_KEY={NEW_KEY}' > /opt/busan/.env && chmod 644 /opt/busan/.env",
    # 2. 기록 확인
    "cat /opt/busan/.env",
    # 3. 실제 API 호출 테스트
    f"""cd /opt/busan && python3 -c "
import urllib.request, urllib.error
key = '{NEW_KEY}'
url = f'https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusThng?serviceKey={{key}}&numOfRows=1&pageNo=1&type=json&inqryDiv=2&inqryBgnDt=20260520&inqryEndDt=20260520'
try:
    resp = urllib.request.urlopen(url, timeout=10)
    print(f'HTTP {{resp.status}}: API 호출 성공')
    data = resp.read().decode('utf-8')[:300]
    print(data)
except urllib.error.HTTPError as e:
    print(f'HTTP {{e.code}}: {{e.reason}}')
    body = e.read().decode('utf-8','replace')[:300]
    print(body)
except Exception as e:
    print(f'Error: {{e}}')
" """,
]

for i, cmd in enumerate(commands, 1):
    print(f"\n[{i}] {'='*50}")
    stdin, stdout, stderr = client.exec_command(cmd, timeout=20)
    out = stdout.read().decode('utf-8', 'replace').strip()
    err = stderr.read().decode('utf-8', 'replace').strip()
    if out:
        print(out)
    if err and 'apport' not in err:
        print(f"STDERR: {err}")

client.close()
