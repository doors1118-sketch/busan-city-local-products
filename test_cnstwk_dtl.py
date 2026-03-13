import urllib.request
import json
import ssl
import sys

sys.stdout.reconfigure(encoding='utf-8')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'

# 공사계약현황에서 1건 가져와서 계약번호 알아내기 (3월 3일자)
api_url1 = f'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkPPSSrch?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDate=20260303&inqryEndDate=20260303&numOfRows=1&pageNo=1&type=json'
req1 = urllib.request.Request(api_url1, headers={'User-Agent': 'Mozilla/5.0'})

try:
    with urllib.request.urlopen(req1, context=ctx) as res1:
        data1 = json.loads(res1.read().decode('utf-8'))
        item = data1['response']['body']['items'][0]
        unty_no = item.get('untyCntrctNo', '')
        print(f"-> 통합계약번호(untyCntrctNo): {unty_no}")
        print(f"-> 공사명: {item.get('cnstwkNm')}")
        
        # 계약번호는 보통 통합계약번호 앞 11자리 + 계약차수 2자리 (또는 untyCntrctNo 전체 사용)
        cntrct_no = unty_no[:11] if len(unty_no) >= 11 else unty_no
        
        print("\n--- [상세조회 API: getCntrctInfoListCnstwkServcInfo 테스트] ---")
        # 1. inqryDiv=2, cntrctNo
        api_url_dtl = f'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkServcInfo?serviceKey={SERVICE_KEY}&inqryDiv=2&cntrctNo={cntrct_no}&type=json'
        
        req2 = urllib.request.Request(api_url_dtl, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req2, context=ctx) as res2:
            data2 = json.loads(res2.read().decode('utf-8'))
            items2 = data2.get('response', {}).get('body', {}).get('items', [])
            if items2:
                print(f"성공! 상세 정보 필드 목록: {list(items2[0].keys())}")
                print(f"주요 필드 값:")
                for k in ['cntrctNo', 'cnstwkFld', 'cnstwkdRgnAdrs', 'cnstwkSite', 'dminsttRgnNm', 'rgnNm']:
                    if k in items2[0]:
                        print(f"  - {k}: {items2[0][k]}")
            else:
                print(f"검색 결과가 없습니다. (Response: {data2.get('response', {}).get('header', {})})")
                
                # 2. 파라미터를 inqryDiv=1 & inqryBgnDate 로 옵션 바꿔서 호출해보기
                print("\n[inqryDiv=1 & 날짜 로 다시 시도...]")
                api_url_dtl_date = f'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkServcInfo?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDate=20260303&inqryEndDate=20260303&numOfRows=1&pageNo=1&type=json'
                req3 = urllib.request.Request(api_url_dtl_date, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req3, context=ctx) as res3:
                    data3 = json.loads(res3.read().decode('utf-8'))
                    items3 = data3.get('response', {}).get('body', {}).get('items', [])
                    if items3:
                         print(f"성공! 상세 정보 필드 목록 (날짜검색): {list(items3[0].keys())}")
                         print(f"cnstwkFld 현장 정보 존재여부: {'cnstwkFld' in items3[0]}")
                    else:
                        print("검색 결과가 없습니다.")
except Exception as e:
    print(f"Error: {e}")
