import paramiko

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('49.50.133.160', port=22, username='root', password='back9900@@', timeout=10)

commands = [
    # 1. 서버 daily_pipeline_sync.py에서 실제 사용 중인 API URL 확인
    "grep -n 'apis.data.go.kr' /opt/busan/daily_pipeline_sync.py | head -20",
    # 2. 새 API(ao/CntrctInfoService)로 테스트
    """python3 -c "
import urllib.request, urllib.error

key = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'

tests = [
    ('OLD ScsbidInfo thng', 'https://apis.data.go.kr/1230000/ScsbidInfoService/getScsbidListSttusThng?serviceKey={key}&numOfRows=1&pageNo=1&type=json&inqryDiv=2&inqryBgnDt=20260520&inqryEndDt=20260520'),
    ('NEW CntrctInfo thng', 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThng?serviceKey={key}&numOfRows=1&pageNo=1&type=json&inqryDiv=2&inqryBgnDt=20260520&inqryEndDt=20260520'),
    ('NEW CntrctInfo cnstwk', 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwk?serviceKey={key}&numOfRows=1&pageNo=1&type=json&inqryDiv=2&inqryBgnDt=20260520&inqryEndDt=20260520'),
    ('NEW CntrctInfo servc', 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServc?serviceKey={key}&numOfRows=1&pageNo=1&type=json&inqryDiv=2&inqryBgnDt=20260520&inqryEndDt=20260520'),
]

for label, url_tpl in tests:
    url = url_tpl.format(key=key)
    try:
        resp = urllib.request.urlopen(url, timeout=15)
        data = resp.read().decode('utf-8')[:200]
        print(f'[{label}] HTTP {resp.status} => {data[:150]}')
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8','replace')[:200]
        print(f'[{label}] HTTP {e.code} {e.reason} => {body}')
    except Exception as e:
        print(f'[{label}] Error: {e}')
" """,
]

for i, cmd in enumerate(commands, 1):
    print(f"\n[{i}] {'='*50}")
    stdin, stdout, stderr = client.exec_command(cmd, timeout=30)
    out = stdout.read().decode('utf-8', 'replace').strip()
    err = stderr.read().decode('utf-8', 'replace').strip()
    if out:
        print(out)
    if err and 'apport' not in err:
        print(f"STDERR: {err}")

client.close()
