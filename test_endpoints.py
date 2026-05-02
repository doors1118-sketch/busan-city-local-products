import requests

urls = [
    'getSmppCertInfoList',
    'getSmppCertList',
    'getCertList',
    'getCertInfo',
    'getCertInfoList',
    'getSmppCert',
    'getCert',
    'smppCertInfoList',
    'smppCertList',
]

base_url = 'https://apis.data.go.kr/B550598/smppCertInfo'
key = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'

for u in urls:
    try:
        r = requests.get(f"{base_url}/{u}?serviceKey={key}")
        print(f"{u}: {r.status_code}")
        if r.status_code == 200:
            print(r.text[:200])
            break
    except Exception as e:
        print(f"{u}: error {e}")
