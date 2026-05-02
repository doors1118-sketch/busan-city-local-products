import os
import sqlite3
import datetime
import urllib.request
import json
import ssl
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(encoding='utf-8')
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = os.environ.get('SERVICE_KEY', '')
DB_PATH = 'c:/Users/COMTREE/Desktop/연습/procurement_contracts.db'

API_MAP = {
    '공사': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkPPSSrch',
}

def parse_corp_list(corp_list_str):
    results = []
    if not corp_list_str or corp_list_str == 'null': return results
    clean_str = corp_list_str.replace('[', '').replace(']', '')
    import re
    tokens = re.split(r'\",\s*\"|\",\"|^\"|\"$', clean_str)
    tokens = [t for t in tokens if t.strip()]
    
    for token in tokens:
        parts = token.split('^')
        if len(parts) >= 10:
            nm = parts[3].strip()
            role = parts[1].strip()
            try: share = float(parts[6].strip())
            except: share = 100.0
            bizno = parts[-1].strip()
            if not bizno: bizno = f'UNKNOWN_{nm}'
            results.append({'bizno': bizno, 'name': nm, 'role': role, 'share': share})
    return results

def get_contracts_with_bid_no(target_date):
    date_str_start = f"{target_date}0000"
    date_str_end = f"{target_date}2359"
    results = []
    
    page_no = 1
    num_of_rows = 100
    base_url = API_MAP['공사']
    
    while True:
        url = f'{base_url}?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt={date_str_start}&inqryEndDt={date_str_end}&numOfRows={num_of_rows}&pageNo={page_no}&type=json'
        retry_count = 0
        while retry_count < 5:
            try:
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                res = urllib.request.urlopen(req, context=ctx, timeout=30)
                data = json.loads(res.read().decode('utf-8'))
                items = data.get('response', {}).get('body', {}).get('items', [])
                
                if not items: break
                
                for item in items:
                    cntrctNo = item.get('cntrctNo', '') or item.get('untyCntrctNo', '')
                    bidNtceNo = item.get('ntceNo', '') or item.get('bidNtceNo', '') 
                    if '-' in bidNtceNo:
                        bidNtceNo = bidNtceNo.split('-')[0]
                        
                    cntrctNm = item.get('cntrctNm', '') or item.get('cnstwkNm', '')
                    dminsttNm = item.get('dminsttNm', '')
                    dminsttCd = item.get('dminsttCd', '')
                    cntrctDate = item.get('cntrctDate', '') or item.get('dcsnCntrctDt', '')
                    try: totAmt = float(item.get('totCntrctAmt', item.get('totMny', 0)))
                    except: totAmt = 0.0
                    
                    raw_corp = str(item.get('corpList', ''))
                    corps = parse_corp_list(raw_corp)
                    if not corps:
                        single_bizno = item.get('cntrctRprsntCorpBzno', '')
                        single_nm = item.get('cntrctRprsntCorpNm', '')
                        if single_bizno: corps.append({'bizno': single_bizno, 'name': single_nm, 'role': '단독수급', 'share': 100.0})
                            
                    for corp in corps:
                        krw_share = round(totAmt * (corp['share'] / 100.0))
                        results.append((cntrctNo, '공사', bidNtceNo, cntrctNm, dminsttNm, dminsttCd, cntrctDate, totAmt, corp['bizno'], corp['name'], corp['role'], corp['share'], krw_share))
                
                tc = data.get('response', {}).get('body', {}).get('totalCount', 0)
                if page_no * num_of_rows >= tc: break
                page_no += 1
                time.sleep(0.5)
                break # Success, break inner retry loop
                
            except Exception as e:
                retry_count += 1
                if retry_count >= 5:
                    print(f"Error {target_date} p{page_no} after 5 retries: {e}")
                    break
                time.sleep(2 * retry_count)
            
    return results

def reload_jan_feb_contracts():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # 공사 계약 테이블을 완전히 밀고 다시 적재 (입찰공고 매핑을 위해)
    cursor.execute("DELETE FROM contracts_raw WHERE bsnsDivNm = '공사'")
    conn.commit()
    
    dt_curr = datetime.datetime(2026, 1, 1)
    dt_end = datetime.datetime(2026, 3, 4)
    dates = []
    while dt_curr <= dt_end:
        dates.append(dt_curr.strftime('%Y%m%d'))
        dt_curr += datetime.timedelta(days=1)
        
    print(f"총 {len(dates)}일간의 공사 실적 재수집 시작 (bidNtceNo 매핑 보강)...")
    
    total = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_date = {executor.submit(get_contracts_with_bid_no, d): d for d in dates}
        for future in as_completed(future_to_date):
            res = future.result()
            if res:
                cursor.executemany('''
                    INSERT OR IGNORE INTO contracts_raw
                    (cntrctNo, bsnsDivNm, bidNtceNo, cntrctNm, dminsttNm, dminsttCd, cntrctDate, totCntrctAmt,
                    corpBizrno, corpNm, corpRole, corpShareRate, krwShareAmt)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', res)
                conn.commit()
                total += len(res)
                print(f"적재 완료 누적: {total:,}건")
                
    conn.close()
    print("재수집 완료!")

if __name__ == '__main__':
    reload_jan_feb_contracts()
