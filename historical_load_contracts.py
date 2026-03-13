import urllib.request
import urllib.parse
import json
import ssl
import sqlite3
import datetime
import math
import sys
import time
import os

sys.stdout.reconfigure(encoding='utf-8')

# SSL Context
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'
DB_PATH = 'c:/Users/COMTREE/Desktop/연습/procurement_contracts.db'

# === API Endpoint Mapping ===
API_MAP = {
    '공사': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwk',
    '용역': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServc',
    '물품': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThng'
}

def parse_corp_list(corp_list_str):
    '''
    corpList 파싱 로직 
    "[순번^역할^공동수급구분^업체명^대표자명^국가^지분율^상세상호^기타^사업자등록번호]" 배열에서 
    (사업자등록번호, 지분율(%), 업체명, 역할) 등을 추출
    '''
    results = []
    if not corp_list_str or corp_list_str == 'null': return results
    
    # [ 와 ] 제거 후 콤마(,) 기준으로 분리 (구분자가 캐럿^인 덩어리들)
    # 실제로는 대괄호 안에 여러 개가 들어있을 수 있음
    clean_str = corp_list_str.replace('[', '').replace(']', '')
    
    # API 응답의 캐럿 기반 다중 배열 문자열 형식을 split
    # 주의: 큰따옴표나 내부 콤마 때문에 단순 분리가 어려울 수 있어 안전하게 파싱 (기록 문서 기반)
    # 보통 큰따옴표로 묶여서 ", " 단위로 옴.
    import re
    tokens = re.split(r'\",\s*\"|\",\"|^\"|\"$', clean_str)
    tokens = [t for t in tokens if t.strip()] # 빈 값 제거
    
    for token in tokens:
        parts = token.split('^')
        if len(parts) >= 10:
            nm = parts[3].strip()
            role = parts[1].strip()
            # 지분율은 무조건 소수점으로 추출 시도
            try: share = float(parts[6].strip())
            except: share = 100.0
            
            bizno = parts[-1].strip()
            # 형식 오류 방어: 사업자번호가 빈값이면 업체명을 ID로
            if not bizno: bizno = f'UNKNOWN_{nm}'
            
            results.append({
                'bizno': bizno, 
                'name': nm, 
                'role': role, 
                'share': share
            })
    return results

def load_contracts_for_date(target_date_start, target_date_end, conn):
    cursor = conn.cursor()
    total_inserted = 0
    total_errors = 0
    
    print(f'\\n[START] 계약 데이터 수집 시작 ({target_date_start[:8]} ~ {target_date_end[:8]})')
    
    for category, base_url in API_MAP.items():
        print(f'  -> {category} 카테고리 로드 중...', end='')
        page_no = 1
        num_of_rows = 100 # 안전한 페이징
        category_inserted = 0
        
        while True:
            # 계약일자 기준(inqryBgnDt, inqryEndDt) -- CntrctInfoService 용 파라미터명
            url = f'{base_url}?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt={target_date_start}&inqryEndDt={target_date_end}&numOfRows={num_of_rows}&pageNo={page_no}&type=json'
            
            try:
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, context=ctx, timeout=30) as res:
                    text = res.read().decode('utf-8')
                    data = json.loads(text)
                    items = data.get('response', {}).get('body', {}).get('items', [])
                    
                    if not items:
                        break # 데이터 소진
                    
                    for item in items:
                        # 기본 필드
                        cntrctNo = item.get('cntrctNo', '')
                        if not cntrctNo: cntrctNo = item.get('untyCntrctNo', '')
                        
                        bidNtceNo = item.get('bidNtceNo', '')
                        cntrctNm = item.get('cntrctNm', '')
                        dminsttNm = item.get('dminsttNm', '')
                        dminsttCd = item.get('dminsttCd', '')
                        cntrctDate = item.get('cntrctDate', '')
                        
                        try:
                            totAmt = float(item.get('totCntrctAmt', 0))
                        except:
                            totAmt = 0.0
                            
                        # corpList 파싱 및 분할 적재
                        raw_corp = str(item.get('corpList', ''))
                        corps = parse_corp_list(raw_corp)
                        
                        # 예외 처리: corpList가 텅 비었지만 단독 계약자 필드(cntrctRprsntCorpBzno)가 있는 경우
                        if not corps:
                            single_bizno = item.get('cntrctRprsntCorpBzno', '')
                            single_nm = item.get('cntrctRprsntCorpNm', '')
                            if single_bizno:
                                corps.append({'bizno': single_bizno, 'name': single_nm, 'role': '단독수급', 'share': 100.0})
                                
                        for corp in corps:
                            # 개별 지분 금액 연산
                            krw_share = round(totAmt * (corp['share'] / 100.0))
                            
                            # Insert into DB (IGNORE duplication if run multiple times)
                            cursor.execute('''
                                INSERT OR IGNORE INTO contracts_raw
                                (cntrctNo, bsnsDivNm, bidNtceNo, cntrctNm, dminsttNm, dminsttCd, cntrctDate, totCntrctAmt,
                                corpBizrno, corpNm, corpRole, corpShareRate, krwShareAmt)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (cntrctNo, category, bidNtceNo, cntrctNm, dminsttNm, dminsttCd, cntrctDate, totAmt,
                                  corp['bizno'], corp['name'], corp['role'], corp['share'], krw_share))
                            category_inserted += 1
                    
                    # 페이징 진행
                    total_count = data.get('response', {}).get('body', {}).get('totalCount', 0)
                    if page_no * num_of_rows >= total_count:
                        break # 마지막 페이지
                    page_no += 1
                    time.sleep(0.1) # API 부하 조절
                    
            except Exception as e:
                total_errors += 1
                print(f'\\n    [Error] {category} page {page_no}: {e}')
                break
                
        print(f' {category_inserted} row(s) 분할 적재 완료.')
        total_inserted += category_inserted
        conn.commit()
        
    print(f'[END] 총 {total_inserted} rows 적재 완료 (Errors: {total_errors})\\n')

if __name__ == '__main__':
    print('Connecting to DB...')
    conn = sqlite3.connect(DB_PATH)
    
    # 테스트 구동: 26년 2월 10일 ~ 2월 15일 단기 루프 실행
    start_point = datetime.datetime(2026, 2, 10)
    end_point = datetime.datetime(2026, 2, 15)
    
    curr = start_point
    while curr <= end_point:
        # 매일 일자별로 쪼개어 호출 (데이터 건수가 많아 Input Range 에러 방어)
        date_str_start = curr.strftime('%Y%m%d0000')
        date_str_end = curr.strftime('%Y%m%d2359')
        load_contracts_for_date(date_str_start, date_str_end, conn)
        curr += datetime.timedelta(days=1)
        
    conn.close()
    print('Sample Load Complete!')
