import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

NEW_KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'

# 여러 엔드포인트로 테스트
cmd = f"""cd /opt/busan && python3 -c "
import urllib.request, urllib.error, urllib.parse

key = '{NEW_KEY}'

tests = [
    ('물품 계약', 'https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusThng'),
    ('공사 계약', 'https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusCnstwk'),
    ('용역 계약', 'https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusServc'),
    ('수요기관', 'https://apis.data.go.kr/1230000/HrcspSsstmDataInfoService/getPublicPrcureEntyInfoList'),
]

for name, base in tests:
    params = dict(serviceKey=key, numOfRows='1', pageNo='1', type='json', inqryDiv='2', inqryBgnDt='20260520', inqryEndDt='20260520')
    if 'HrcspSsstm' in base:
        params = dict(serviceKey=key, numOfRows='1', pageNo='1', type='json')
    url = base + '?' + urllib.parse.urlencode(params)
    try:
        resp = urllib.request.urlopen(url, timeout=10)
        data = resp.read().decode('utf-8')[:150]
        print(f'[{name}] HTTP {resp.status} OK : {data[:100]}')
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8','replace')[:200]
        print(f'[{name}] HTTP {e.code} {e.reason} : {body}')
    except Exception as e:
        print(f'[{name}] Error: {{e}}')
"
"""

stdin, stdout, stderr = client.exec_command(cmd, timeout=30)
print(stdout.read().decode('utf-8', 'replace'))
err = stderr.read().decode('utf-8', 'replace').strip()
if err and 'apport' not in err:
    print(f"STDERR: {err}")

client.close()
