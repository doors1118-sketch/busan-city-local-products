import os
import urllib.request
import json
import ssl
import sqlite3
import shutil
import pandas as pd
import datetime
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(encoding='utf-8')

# SSL 및 기본 정보
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


SERVICE_KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'
DB_PATH = 'procurement_contracts.db'
AGENCY_DB_PATH = 'busan_agencies_master.db'
SERVC_SITE_DB_PATH = 'servc_site.db'

def check_api_health():
    """조달청 API 서비스 상태 확인. 정상이면 True, 점검/장애 시 False."""
    test_url = (f"https://apis.data.go.kr/1230000/ao/UsrInfoService02/getDminsttInfo02"
                f"?serviceKey={SERVICE_KEY}&inqryDiv=2"
                f"&inqryBgnDt=202601010000&inqryEndDt=202601010100"
                f"&numOfRows=1&pageNo=1&type=json")
    for attempt in range(3):
        try:
            req = urllib.request.Request(test_url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, context=ctx, timeout=10) as res:
                data = json.loads(res.read().decode('utf-8'))
                code = data.get('response', {}).get('header', {}).get('resultCode')
                if code == '00':
                    return True
                msg = data.get('response', {}).get('header', {}).get('resultMsg', '')
                print(f"   ⚠️ API 응답 비정상 (코드: {code}, 메시지: {msg})")
                return False
        except Exception as e:
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
            else:
                print(f"   ⚠️ API 연결 실패 (3회 시도): {e}")
    return False

APIS = {
    '공사_중앙': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkPPSSrch',
    '공사_자체': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkSrch',
    '용역_중앙': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch',
    '용역_자체': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcSrch',
    '물품_중앙': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThngPPSSrch',
    '물품_자체': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThngSrch',
    '쇼핑몰': 'https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getDlvrReqDtlInfoList'
}

TABLE_MAP = {
    '공사_중앙': 'cnstwk_cntrct',
    '공사_자체': 'cnstwk_cntrct',
    '용역_중앙': 'servc_cntrct',
    '용역_자체': 'servc_cntrct',
    '물품_중앙': 'thng_cntrct',
    '물품_자체': 'thng_cntrct',
    '쇼핑몰': 'shopping_cntrct'
}

def update_agency_master_daily(target_date):
    """ D-1 전국에서 변경된 수요기관 정보만 받아와서 부산 기관인 경우 로컬 SQLite에 추가/갱신 """
    print(f"[수요기관 동기화] {target_date} 전국 기관 변경/신설 내역 스캔 및 부산 필터링 중...")
    bgn_dt = f"{target_date}0000"
    end_dt = f"{target_date}2359"
    api_url = f"https://apis.data.go.kr/1230000/ao/UsrInfoService02/getDminsttInfo02"
    
    def fetch_agency(page_no):
        query = f"?serviceKey={SERVICE_KEY}&inqryDiv=2&inqryBgnDt={bgn_dt}&inqryEndDt={end_dt}&numOfRows=999&pageNo={page_no}&type=json"
        retry = 0
        while retry < 3:
            try:
                rq = urllib.request.Request(api_url + query, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(rq, context=ctx, timeout=20) as res:
                    d = json.loads(res.read().decode('utf-8'))
                    h = d.get('response', {}).get('header', {})
                    if h.get('resultCode') == '00':
                        b = d.get('response', {}).get('body', {})
                        return b.get('items', []), b.get('totalCount', 0)
            except Exception:
                pass
            retry += 1
            time.sleep(1)
        return [], 0
        
    items, total_count = fetch_agency(1)
    all_items = list(items) if items else []
    
    if total_count and int(total_count) > 999:
        total_pages = (int(total_count) // 999) + 1
        for p in range(2, total_pages + 1):
            p_items, _ = fetch_agency(p)
            if p_items:
                all_items.extend(p_items)
            
    busan_agencies = []
    for item in all_items:
        rgn = str(item.get('rgnNm', ''))
        adrs = str(item.get('adrs', ''))
        if '부산' in rgn or '부산광역시' in adrs:
            busan_agencies.append((
                item.get('dminsttCd'),
                item.get('dminsttNm'),
                item.get('bizno'),
                item.get('rgnNm'),
                item.get('adrs'),
                item.get('dltYn'),
                item.get('rgstDt'),
                item.get('chgDt')
            ))
            
    # 제외 기관 (대행 계약이 실적으로 둔갑하는 문제 방지)
    EXCLUDE_AGENCY_CODES = {'1230130'}  # 부산지방조달청
    busan_agencies = [ag for ag in busan_agencies if str(ag[0]).strip() not in EXCLUDE_AGENCY_CODES]
    
    if busan_agencies:
        conn = sqlite3.connect(AGENCY_DB_PATH)
        cursor = conn.cursor()
        
        # 신규 기관 색출 (기존 DB에 없는 코드 확인)
        cursor.execute("SELECT dminsttCd FROM agency_master")
        existing_codes = set(str(row[0]).strip() for row in cursor.fetchall())
        
        new_agencies = []
        for ag in busan_agencies:
            if str(ag[0]).strip() not in existing_codes:
                new_agencies.append(ag)
                
        if new_agencies:
            print(f"\n🚨 [긴급 알림] 기존 마스터 DB에 없던 '신규 부산 기관' {len(new_agencies)}건이 발견되었습니다!")
            print(f"   -> 통계 누락 방지를 위해 분류(대/중/소) 작업이 즉시 필요합니다.")
            
            # CSV로 내보내기
            df_new = pd.DataFrame(new_agencies, columns=['수요기관코드', '수요기관명', '사업자등록번호', '지역명', '상세주소', '삭제여부', '등록일', '변경일'])
            alert_filename = f"[분류요망_신규기관알림]_{target_date}.csv"
            df_new.to_csv(alert_filename, index=False, encoding='utf-8-sig')
            print(f"   -> 알람 파일 자동 생성 완료: {alert_filename}")
            print(f"   -> 조치방법: 파일에 소속을 기입하신 뒤, `update_agency_categories.py`를 실행하세요!\n")
            
        cursor.executemany('''
            INSERT INTO agency_master 
            (dminsttCd, dminsttNm, bizno, rgnNm, adrs, dltYn, rgstDt, chgDt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dminsttCd) DO UPDATE SET
            dminsttNm=excluded.dminsttNm,
            bizno=excluded.bizno,
            rgnNm=excluded.rgnNm,
            adrs=excluded.adrs,
            dltYn=excluded.dltYn,
            chgDt=excluded.chgDt
        ''', busan_agencies)
        conn.commit()
        conn.close()
        print(f"   -> 완료: 부산 관내 신규/수정 수요기관 {len(busan_agencies)}건 DB 자동 등록 및 갱신 성공.")
    else:
        print(f"   -> 완료: 지정한 일자에 부산 관내 수요기관 변동사항 없음.")

COMPANY_DB_PATH = 'busan_companies_master.db'
COMPANY_API_URL = 'https://apis.data.go.kr/1230000/ao/UsrInfoService02/getPrcrmntCorpBasicInfo02'

def update_company_master_daily(target_date):
    """ D-1 전국 조달업체 변동분을 받아와서 부산+본사 업체만 로컬 SQLite에 Upsert """
    print(f"[조달업체 동기화] {target_date} 전국 조달업체 변경/신설 내역 스캔 및 부산+본사 필터링 중...")
    bgn_dt = f"{target_date}0000"
    end_dt = f"{target_date}2359"

    def fetch_company_page(page_no):
        query = f"?serviceKey={SERVICE_KEY}&inqryDiv=2&inqryBgnDt={bgn_dt}&inqryEndDt={end_dt}&numOfRows=999&pageNo={page_no}&type=json"
        retry = 0
        while retry < 3:
            try:
                rq = urllib.request.Request(COMPANY_API_URL + query, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(rq, context=ctx, timeout=20) as res:
                    d = json.loads(res.read().decode('utf-8'))
                    h = d.get('response', {}).get('header', {})
                    if h.get('resultCode') == '00':
                        b = d.get('response', {}).get('body', {})
                        return b.get('items', []), b.get('totalCount', 0)
            except Exception:
                pass
            retry += 1
            time.sleep(1)
        return [], 0

    items, total_count = fetch_company_page(1)
    all_items = list(items) if items else []

    if total_count and int(total_count) > 999:
        total_pages = (int(total_count) // 999) + 1
        for p in range(2, total_pages + 1):
            p_items, _ = fetch_company_page(p)
            if p_items:
                all_items.extend(p_items)

    # 부산 + 본사 필터링
    busan_companies = []
    for item in all_items:
        rgn = str(item.get('rgnNm', ''))
        hdoffce = str(item.get('hdoffceDivNm', ''))
        if '부산' in rgn and hdoffce == '본사':
            bizno = str(item.get('bizno', '')).replace('-', '').strip()
            if not bizno:
                continue
            busan_companies.append((
                bizno,
                item.get('corpNm', ''),
                item.get('ceoNm', ''),
                item.get('rgnNm', ''),
                item.get('adrs', ''),
                item.get('dtlAdrs', ''),
                item.get('hdoffceDivNm', ''),
                item.get('corpBsnsDivNm', ''),
                item.get('mnfctDivNm', ''),
                item.get('opbizDt', ''),
                item.get('rgstDt', ''),
                item.get('chgDt', ''),
                'api'
            ))

    if busan_companies:
        conn = sqlite3.connect(COMPANY_DB_PATH)
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT INTO company_master
            (bizno, corpNm, ceoNm, rgnNm, adrs, dtlAdrs, hdoffceDivNm, corpBsnsDivNm, mnfctDivNm, opbizDt, rgstDt, chgDt, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bizno) DO UPDATE SET
            corpNm=excluded.corpNm,
            ceoNm=excluded.ceoNm,
            rgnNm=excluded.rgnNm,
            adrs=excluded.adrs,
            dtlAdrs=excluded.dtlAdrs,
            hdoffceDivNm=excluded.hdoffceDivNm,
            corpBsnsDivNm=excluded.corpBsnsDivNm,
            mnfctDivNm=excluded.mnfctDivNm,
            opbizDt=excluded.opbizDt,
            chgDt=excluded.chgDt
        ''', busan_companies)
        conn.commit()
        conn.close()
        print(f"   -> 완료: 전국 {len(all_items):,}건 중 부산+본사 {len(busan_companies)}건 DB Upsert 성공.")
    else:
        print(f"   -> 완료: 전국 {len(all_items):,}건 스캔, 부산+본사 변동 없음.")

INDSTRY_API_URL = 'https://apis.data.go.kr/1230000/ao/UsrInfoService02/getPrcrmntCorpIndstrytyInfo02'

def update_company_industry_daily(target_date):
    """ D-1 전국 조달업체 업종정보 변동분을 받아와서 부산 업체만 로컬 SQLite에 Upsert """
    print(f"[업종정보 동기화] {target_date} 전국 업종정보 변경/신설 내역 스캔 중...")
    bgn_dt = f"{target_date}0000"
    end_dt = f"{target_date}2359"

    def fetch_industry_page(page_no):
        query = f"?serviceKey={SERVICE_KEY}&inqryDiv=2&inqryBgnDt={bgn_dt}&inqryEndDt={end_dt}&numOfRows=999&pageNo={page_no}&type=json"
        retry = 0
        while retry < 3:
            try:
                rq = urllib.request.Request(INDSTRY_API_URL + query, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(rq, context=ctx, timeout=20) as res:
                    d = json.loads(res.read().decode('utf-8'))
                    h = d.get('response', {}).get('header', {})
                    if h.get('resultCode') == '00':
                        b = d.get('response', {}).get('body', {})
                        return b.get('items', []), b.get('totalCount', 0)
            except Exception:
                pass
            retry += 1
            time.sleep(1)
        return [], 0

    items, total_count = fetch_industry_page(1)
    all_items = list(items) if items else []

    if total_count and int(total_count) > 999:
        total_pages = (int(total_count) // 999) + 1
        for p in range(2, total_pages + 1):
            p_items, _ = fetch_industry_page(p)
            if p_items:
                all_items.extend(p_items)

    if not all_items:
        print(f"   -> 완료: 해당일 업종정보 변동 없음.")
        return

    # 부산 업체 사업자번호 목록 가져오기
    conn = sqlite3.connect(COMPANY_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT bizno FROM company_master")
    busan_biznos = set(row[0] for row in cursor.fetchall())

    # company_industry 테이블 생성 (없으면)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS company_industry (
            bizno TEXT,
            indstrytyCd TEXT,
            indstrytyNm TEXT,
            rgstDt TEXT,
            vldPrdExprtDt TEXT,
            indstrytyStatsNm TEXT,
            rprsntIndstrytyYn TEXT,
            chgDt TEXT,
            PRIMARY KEY (bizno, indstrytyCd)
        )
    ''')

    # 부산 업체만 필터링하여 upsert
    busan_industries = []
    for item in all_items:
        bizno = str(item.get('bizno', '')).replace('-', '').strip()
        if bizno in busan_biznos:
            busan_industries.append((
                bizno,
                item.get('indstrytyCd', ''),
                item.get('indstrytyNm', ''),
                item.get('rgstDt', ''),
                item.get('vldPrdExprtDt', ''),
                item.get('indstrytyStatsNm', ''),
                item.get('rprsntIndstrytyYn', ''),
                item.get('chgDt', '')
            ))

    if busan_industries:
        cursor.executemany('''
            INSERT OR REPLACE INTO company_industry
            (bizno, indstrytyCd, indstrytyNm, rgstDt, vldPrdExprtDt, indstrytyStatsNm, rprsntIndstrytyYn, chgDt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', busan_industries)
        conn.commit()
        print(f"   -> 완료: 전국 {len(all_items):,}건 중 부산 업체 {len(busan_industries)}건 업종정보 Upsert 성공.")
    else:
        print(f"   -> 완료: 전국 {len(all_items):,}건 스캔, 부산 업체 업종 변동 없음.")

    conn.close()

BID_NOTICES_API_URL = 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwk'

def update_bid_notices_daily(target_date):
    """ D-1 공사 입찰공고 데이터를 수집하여 공사현장이 '부산'인 건만 DB에 적재 """
    print(f"[입찰공고 동기화] {target_date} 공사 입찰공고 스캔 및 부산 현장 필터링 중...")
    bgn_dt = f"{target_date}0000"
    end_dt = f"{target_date}2359"
    
    page_no = 1
    num_of_rows = 999
    busan_notices = []
    total_scanned = 0
    
    while True:
        query = f"?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt={bgn_dt}&inqryEndDt={end_dt}&numOfRows={num_of_rows}&pageNo={page_no}&type=json"
        url = BID_NOTICES_API_URL + query
        
        retry = 0
        success = False
        while retry < 3:
            try:
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, context=ctx, timeout=30) as res:
                    d = json.loads(res.read().decode('utf-8'))
                    h = d.get('response', {}).get('header', {})
                    if h.get('resultCode') == '00':
                        b = d.get('response', {}).get('body', {})
                        items = b.get('items', [])
                        
                        if not items:
                            success = True
                            break
                        
                        for item in items:
                            total_scanned += 1
                            rgn_nm = item.get('cnstrtsiteRgnNm', '')
                            # 공사현장이 부산인 건 필터
                            if rgn_nm and '부산' in str(rgn_nm):
                                bid_no = item.get('bidNtceNo', '')
                                bid_ord = item.get('bidNtceOrd', '00')
                                bid_nm = item.get('bidNtceNm', '')
                                dm_cd = item.get('dminsttCd', '')
                                dm_nm = item.get('dminsttNm', '')
                                dt = item.get('bidNtceDt', '')
                                
                                if bid_no:
                                    busan_notices.append((bid_no, bid_ord, bid_nm, dm_cd, dm_nm, rgn_nm, dt))
                                    
                        total_count = b.get('totalCount', 0)
                        if page_no * num_of_rows >= int(total_count):
                            success = True
                            page_no = -1 # outer loop stop indicator
                        break
            except Exception:
                pass
            retry += 1
            time.sleep(1)
            
        if page_no == -1 or not success:
            break
        page_no += 1
        
    if busan_notices:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT OR IGNORE INTO bid_notices_raw
            (bidNtceNo, bidNtceOrd, bidNtceNm, dminsttCd, dminsttNm, cnstrtsiteRgnNm, bidNtceDt)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', busan_notices)
        conn.commit()
        conn.close()
        print(f"   -> 완료: 전국 {total_scanned:,}건 중 공사현장이 '부산'인 {len(busan_notices)}건 DB 적재 성공.")
    else:
        print(f"   -> 완료: 전국 {total_scanned:,}건 스캔, 공사현장이 '부산'인 공고 없음.")

# 용역 조달요청 현장지역 API
SERVC_REQ_APIS = {
    'tech': {
        'url': 'https://apis.data.go.kr/1230000/ao/PrcrmntReqInfoService/getPrcrmntReqInfoListTechServc',
        'table': 'servc_req_site',
        'site_field': 'cnstrtsiteRgnNm',
        'amt_field': 'totCnstwkScleAmt',
        'method_field': 'cntrctCnclsMthdNm',
        'label': '기술용역',
    },
    'gnrl': {
        'url': 'https://apis.data.go.kr/1230000/ao/PrcrmntReqInfoService/getPrcrmntReqInfoListGnrlServcPPSSrch',
        'table': 'servc_req_site_gnrl',
        'site_field': 'rprsntDlvrPlce',
        'amt_field': 'bdgtAmt',
        'method_field': 'cntrctCnclsStleNm',
        'label': '일반용역',
    },
}

def update_servc_site_daily(target_date):
    """ D-1 용역 조달요청 API에서 현장지역 수집 → servc_site.db 적재 """
    bgn_dt = f"{target_date}0000"
    end_dt = f"{target_date}2359"

    print(f"[용역 현장 동기화] {target_date} 기술/일반 용역 조달요청 수집 중...")

    conn = sqlite3.connect(SERVC_SITE_DB_PATH, timeout=30)

    # 테이블 생성 (없으면)
    conn.execute("""CREATE TABLE IF NOT EXISTS servc_req_site (
        prcrmntReqNo TEXT PRIMARY KEY,
        prcrmntReqNm TEXT,
        cnstrtsiteRgnNm TEXT,
        orderInsttCd TEXT,
        orderInsttNm TEXT,
        rcptDt TEXT,
        totCnstwkScleAmt TEXT,
        cntrctCnclsMthdNm TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS servc_req_site_gnrl (
        prcrmntReqNo TEXT PRIMARY KEY,
        prcrmntReqNm TEXT,
        rprsntDlvrPlce TEXT,
        orderInsttCd TEXT,
        orderInsttNm TEXT,
        rcptDt TEXT,
        bdgtAmt TEXT,
        cntrctCnclsStleNm TEXT
    )""")
    conn.commit()

    for api_key, api_info in SERVC_REQ_APIS.items():
        table = api_info['table']
        site_field = api_info['site_field']
        amt_field = api_info['amt_field']
        method_field = api_info['method_field']
        label = api_info['label']

        existing = set(r[0] for r in conn.execute(f"SELECT prcrmntReqNo FROM {table}").fetchall())
        page = 1
        new_count = 0

        while True:
            url = (f"{api_info['url']}?serviceKey={SERVICE_KEY}"
                   f"&inqryDiv=1&inqryBgnDt={bgn_dt}&inqryEndDt={end_dt}"
                   f"&numOfRows=100&pageNo={page}&type=json")
            retry = 0
            success = False
            while retry < 3:
                try:
                    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                    with urllib.request.urlopen(req, context=ctx, timeout=20) as resp:
                        data = json.loads(resp.read().decode('utf-8'))
                        header = data.get('response', {}).get('header', {})
                        if header.get('resultCode') not in ['00', None]:
                            break
                        body = data.get('response', {}).get('body', {})
                        items = body.get('items', [])
                        if not items:
                            success = True
                            break

                        for item in items:
                            req_no = str(item.get('prcrmntReqNo', '')).strip()
                            if not req_no or req_no in existing:
                                continue
                            conn.execute(f"""INSERT OR IGNORE INTO {table}
                                (prcrmntReqNo, prcrmntReqNm, {site_field}, orderInsttCd, orderInsttNm, rcptDt, {amt_field}, {method_field})
                                VALUES (?,?,?,?,?,?,?,?)""", (
                                req_no,
                                str(item.get('prcrmntReqNm', '')),
                                str(item.get(site_field, '')),
                                str(item.get('orderInsttCd', '')),
                                str(item.get('orderInsttNm', '')),
                                str(item.get('rcptDt', '')),
                                str(item.get(amt_field, '')),
                                str(item.get(method_field, '')),
                            ))
                            existing.add(req_no)
                            new_count += 1

                        total = int(body.get('totalCount', 0))
                        if page * 100 >= total:
                            success = True
                        break
                except Exception:
                    pass
                retry += 1
                time.sleep(1)

            if success or retry >= 3:
                if success:
                    break
                break
            page += 1
            time.sleep(0.3)

        conn.commit()
        print(f"   -> [{label}] {new_count}건 신규 적재")

    conn.close()


def update_servc_site_matching():
    """ servc_site.db의 조달요청 데이터를 servc_cntrct.cnstrtsiteRgnNm에 매칭 """
    print(f"[용역 현장 매칭] servc_site.db → servc_cntrct.cnstrtsiteRgnNm 매칭 중...")

    conn = sqlite3.connect(DB_PATH, timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=120000")
    conn.execute(f"ATTACH DATABASE '{SERVC_SITE_DB_PATH}' AS site")

    # 기술용역 매칭 (cnstrtsiteRgnNm)
    result_tech = conn.execute("""
        UPDATE servc_cntrct SET cnstrtsiteRgnNm = (
            SELECT s.cnstrtsiteRgnNm FROM site.servc_req_site s
            WHERE s.prcrmntReqNo = servc_cntrct.reqNo
            AND s.cnstrtsiteRgnNm IS NOT NULL AND s.cnstrtsiteRgnNm != ''
            LIMIT 1
        )
        WHERE reqNo IS NOT NULL AND reqNo != ''
        AND (cnstrtsiteRgnNm IS NULL OR cnstrtsiteRgnNm = '')
        AND EXISTS (
            SELECT 1 FROM site.servc_req_site s
            WHERE s.prcrmntReqNo = servc_cntrct.reqNo
            AND s.cnstrtsiteRgnNm IS NOT NULL AND s.cnstrtsiteRgnNm != ''
        )
    """)
    tech_count = result_tech.rowcount
    conn.commit()

    # 일반용역 매칭 (rprsntDlvrPlce)
    result_gnrl = conn.execute("""
        UPDATE servc_cntrct SET cnstrtsiteRgnNm = (
            SELECT s.rprsntDlvrPlce FROM site.servc_req_site_gnrl s
            WHERE s.prcrmntReqNo = servc_cntrct.reqNo
            AND s.rprsntDlvrPlce IS NOT NULL AND s.rprsntDlvrPlce != ''
            LIMIT 1
        )
        WHERE reqNo IS NOT NULL AND reqNo != ''
        AND (cnstrtsiteRgnNm IS NULL OR cnstrtsiteRgnNm = '')
        AND EXISTS (
            SELECT 1 FROM site.servc_req_site_gnrl s
            WHERE s.prcrmntReqNo = servc_cntrct.reqNo
            AND s.rprsntDlvrPlce IS NOT NULL AND s.rprsntDlvrPlce != ''
        )
    """)
    gnrl_count = result_gnrl.rowcount
    conn.commit()

    conn.execute("DETACH DATABASE site")
    conn.close()

    print(f"   -> 기술용역 {tech_count}건 + 일반용역 {gnrl_count}건 = {tech_count + gnrl_count}건 매칭")


# ═══════════════ 사전규격 모니터링 수집 ═══════════════
PRESPEC_BASE = 'http://apis.data.go.kr/1230000/ao/HrcspSsstndrdInfoService'
PRESPEC_OPS = {
    '공사': 'getPublicPrcureThngInfoCnstwk',
    '용역': 'getPublicPrcureThngInfoServc',
}
# 보호제도 기준액 — 기관 유형별 차등
PRESPEC_THRESHOLDS = {
    # (부산시, 국가기관)
    '공사_종합': (100e8, 88e8),
    '공사_전문': (10e8, 10e8),
    '용역':      (3.3e8, 2.2e8),
}
PRESPEC_SPECIALIZED_KW = ['전기', '통신', '소방', '기계설비', '정보통신']
BUSAN_CATE = '부산광역시 및 소속기관'  # cate_lrg 값

def sync_prespec(target_date, conn_path=None):
    """[Step 3.7] D-1 사전규격 수집 — 공사+용역, 부산 기관 필터"""
    bgn_dt = f"{target_date}0000"
    end_dt = f"{target_date}2359"
    print(f"[사전규격 수집] {target_date} 공사/용역 사전규격 스캔 중...")

    # agency_master에서 기관명 → (기관코드, 기관유형) 매핑
    conn_ag = sqlite3.connect(AGENCY_DB_PATH)
    ag_map = {}  # {기관명: (dminsttCd, cate_lrg)}
    for r in conn_ag.execute("SELECT dminsttCd, dminsttNm, cate_lrg FROM agency_master").fetchall():
        ag_map[r[1].strip()] = (r[0], r[2] or '')
    conn_ag.close()

    db_path = conn_path or DB_PATH
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("""CREATE TABLE IF NOT EXISTS prespec_monitor (
        bfSpecRgstNo TEXT PRIMARY KEY,
        bsnsDivNm TEXT,
        prdctClsfcNoNm TEXT,
        orderInsttNm TEXT,
        rlDminsttNm TEXT,
        dminsttCd TEXT,
        asignBdgtAmt REAL,
        rcptDt TEXT,
        opninRgstClseDt TEXT,
        bidNtceNoList TEXT,
        rgstDt TEXT,
        chgDt TEXT,
        is_target INTEGER DEFAULT 0,
        target_type TEXT,
        threshold_amt REAL,
        alert_sent INTEGER DEFAULT 0,
        alert_sent_dt TEXT,
        bid_linked INTEGER DEFAULT 0,
        protection_applied TEXT,
        collected_dt TEXT
    )""")
    # alert_history 테이블도 여기서 생성
    conn.execute("""CREATE TABLE IF NOT EXISTS alert_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_dt TEXT,
        alert_type TEXT,
        severity TEXT,
        title TEXT,
        detail TEXT,
        sector TEXT,
        agency TEXT,
        amount REAL,
        ref_no TEXT,
        resolved INTEGER DEFAULT 0,
        resolved_dt TEXT,
        resolved_note TEXT
    )""")
    conn.commit()

    total_new = 0
    for sector, op in PRESPEC_OPS.items():
        page = 1
        sector_items = []
        while True:
            url = (f"{PRESPEC_BASE}/{op}?serviceKey={SERVICE_KEY}"
                   f"&pageNo={page}&numOfRows=999&type=json"
                   f"&inqryDiv=1&inqryBgnDt={bgn_dt}&inqryEndDt={end_dt}")
            retry = 0
            items = []
            total_count = 0
            while retry < 3:
                try:
                    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                    with urllib.request.urlopen(req, context=ctx, timeout=20) as res:
                        data = json.loads(res.read().decode('utf-8'))
                        hdr = data.get('response', {}).get('header', {})
                        if hdr.get('resultCode') == '00':
                            body = data.get('response', {}).get('body', {})
                            items = body.get('items', [])
                            total_count = int(body.get('totalCount', 0))
                    break
                except Exception:
                    retry += 1
                    time.sleep(1)

            if items:
                sector_items.extend(items)
            if not items or page * 999 >= total_count:
                break
            page += 1
            time.sleep(0.3)

        # 부산 기관 필터 + DB 적재
        new_count = 0
        for item in sector_items:
            order_nm = str(item.get('orderInsttNm', '')).strip()
            rl_nm = str(item.get('rlDminsttNm', '')).strip()
            ag_info = ag_map.get(order_nm) or ag_map.get(rl_nm)
            if not ag_info:
                continue
            dm_cd, cate_lrg = ag_info

            reg_no = str(item.get('bfSpecRgstNo', '')).strip()
            if not reg_no:
                continue

            try:
                amt = float(item.get('asignBdgtAmt', 0) or 0)
            except (ValueError, TypeError):
                amt = 0

            # 보호제도 대상 판별 — 기관유형별 기준 차등 적용
            is_busan = (cate_lrg == BUSAN_CATE)
            idx = 0 if is_busan else 1  # (부산, 국가) 튜플 인덱스
            is_target = 0
            target_type = ''
            threshold = 0
            if sector == '공사':
                pname = str(item.get('prdctClsfcNoNm', ''))
                if any(kw in pname for kw in PRESPEC_SPECIALIZED_KW):
                    target_type = '전문'
                    threshold = PRESPEC_THRESHOLDS['공사_전문'][idx]
                else:
                    target_type = '종합'
                    threshold = PRESPEC_THRESHOLDS['공사_종합'][idx]
                if amt > 0 and amt <= threshold:
                    is_target = 1
            else:  # 용역
                target_type = '용역'
                threshold = PRESPEC_THRESHOLDS['용역'][idx]
                if amt > 0 and amt <= threshold:
                    is_target = 1

            now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            conn.execute("""INSERT OR REPLACE INTO prespec_monitor
                (bfSpecRgstNo, bsnsDivNm, prdctClsfcNoNm, orderInsttNm, rlDminsttNm,
                 dminsttCd, asignBdgtAmt, rcptDt, opninRgstClseDt, bidNtceNoList,
                 rgstDt, chgDt, is_target, target_type, threshold_amt, collected_dt)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                reg_no,
                sector,
                str(item.get('prdctClsfcNoNm', '')),
                order_nm,
                rl_nm,
                dm_cd,
                amt,
                str(item.get('rcptDt', '')),
                str(item.get('opninRgstClseDt', '')),
                str(item.get('bidNtceNoList', '')),
                str(item.get('rgstDt', '')),
                str(item.get('chgDt', '')),
                is_target,
                target_type,
                threshold if is_target else None,
                now_str,
            ))
            new_count += 1

        conn.commit()
        total_new += new_count
        print(f"   - [{sector}] 전국 {len(sector_items)}건 → 부산 {new_count}건 적재")

    print(f"   -> 사전규격 총 {total_new}건 수집 완료")
    conn.close()


AWARD_APIS = {
    'busan_award_servc': 'https://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusServcPPSSrch',
    'busan_award_cnstwk': 'https://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusCnstwkPPSSrch',
    'busan_award_thng': 'https://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusThngPPSSrch',
}

def update_busan_awards_daily(target_date):
    """ D-1 부산 지역제한 낙찰정보를 3개 분야(용역/공사/물품) 수집 """
    from urllib.parse import quote
    busan = quote('부산광역시')
    bgn_dt = f"{target_date}0000"
    end_dt = f"{target_date}2359"
    
    print(f"[낙찰정보 동기화] {target_date} 부산 지역제한 낙찰정보 수집 중...")
    
    conn = sqlite3.connect(DB_PATH)
    
    for table_name, base_url in AWARD_APIS.items():
        label = table_name.replace('busan_award_', '')
        page_no = 1
        results = []
        
        while True:
            url = (f'{base_url}?serviceKey={SERVICE_KEY}&inqryDiv=1'
                   f'&inqryBgnDt={bgn_dt}&inqryEndDt={end_dt}'
                   f'&numOfRows=100&pageNo={page_no}&type=json'
                   f'&prtcptLmtRgnNm={busan}')
            try:
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, context=ctx, timeout=30) as res:
                    data = json.loads(res.read().decode('utf-8'))
                    header = data.get('response', {}).get('header', {})
                    if header.get('resultCode') != '00':
                        break
                    items = data.get('response', {}).get('body', {}).get('items', [])
                    if not items:
                        break
                    for item in items:
                        results.append((
                            item.get('bidNtceNo', ''), item.get('bidNtceOrd', '00'),
                            item.get('bidNtceNm', ''), item.get('dminsttCd', ''),
                            item.get('dminsttNm', ''), item.get('bidwinnrBizno', ''),
                            item.get('bidwinnrNm', ''), item.get('bidwinnrAdrs', ''),
                            item.get('sucsfbidAmt', ''), item.get('fnlSucsfDate', ''),
                        ))
                    total = data.get('response', {}).get('body', {}).get('totalCount', 0)
                    if page_no * 100 >= total:
                        break
                    page_no += 1
            except Exception as e:
                print(f"   [Error] {label}: {e}")
                break
        
        if results:
            conn.executemany(f'''
                INSERT OR IGNORE INTO {table_name}
                (bidNtceNo, bidNtceOrd, bidNtceNm, dminsttCd, dminsttNm,
                 bidwinnrBizno, bidwinnrNm, bidwinnrAdrs, sucsfbidAmt, fnlSucsfDate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', results)
            conn.commit()
            print(f"   -> [{label}] {len(results)}건 적재")
        else:
            print(f"   -> [{label}] 해당일 부산 지역제한 낙찰 없음")
    
    conn.close()

# 입찰공고 추정가격 API Endpoints
BID_NOTICE_APIS = {
    '공사': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwk',
    '용역': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServc',
    '물품': 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThng',
}

BID_PRICE_KEEP_FIELDS = [
    'bidNtceNo', 'bidNtceOrd', 'bidNtceNm', 'ntceInsttCd', 'ntceInsttNm',
    'dminsttCd', 'dminsttNm', 'presmptPrce', 'bdgtAmt', 'cntrctCnclsMthdNm',
    'bidNtceDt', 'rgstDt', 'sucsfbidLwltRate', 'sucsfbidMthdNm',
]

def update_bid_notices_price_daily(target_date):
    """ D-1 공사/용역/물품 입찰공고에서 추정가격 등 핵심 필드를 수집 """
    bgn_dt = f"{target_date}0000"
    end_dt = f"{target_date}2359"
    
    print(f"[입찰공고 추정가격 동기화] {target_date} 공사/용역/물품 입찰공고 수집 중...")
    
    conn = sqlite3.connect(DB_PATH)
    
    all_cols = ['bidNtceNo','bidNtceOrd','bidNtceNm','ntceInsttCd','ntceInsttNm',
                'dminsttCd','dminsttNm','presmptPrce','bdgtAmt','cntrctCnclsMthdNm',
                'bidNtceDt','rgstDt','sucsfbidLwltRate','sucsfbidMthdNm',
                'cnstrtsiteRgnNm','prtcptLmtRgnNm','sector','mainCnsttyNm']
    placeholders = ','.join(['?'] * len(all_cols))
    insert_sql = f"INSERT OR IGNORE INTO bid_notices_price ({','.join(all_cols)}) VALUES ({placeholders})"
    
    for sector, api_url in BID_NOTICE_APIS.items():
        extra = 'cnstrtsiteRgnNm' if sector == '공사' else 'prtcptLmtRgnNm'
        fields = BID_PRICE_KEEP_FIELDS + [extra]
        if sector == '공사':
            fields.append('mainCnsttyNm')
        
        page_no = 1
        all_items = []
        
        while True:
            query = (f"?serviceKey={SERVICE_KEY}"
                     f"&inqryDiv=1&inqryBgnDt={bgn_dt}&inqryEndDt={end_dt}"
                     f"&numOfRows=999&pageNo={page_no}&type=json")
            try:
                req = urllib.request.Request(api_url + query, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, context=ctx, timeout=30) as res:
                    data = json.loads(res.read().decode('utf-8'))
                    header = data.get('response', {}).get('header', {})
                    if header.get('resultCode') != '00':
                        break
                    body = data.get('response', {}).get('body', {})
                    items = body.get('items', [])
                    if not items:
                        break
                    for item in items:
                        row = {f: item.get(f, '') for f in fields}
                        row['sector'] = sector
                        row.setdefault('cnstrtsiteRgnNm', '')
                        row.setdefault('prtcptLmtRgnNm', '')
                        row.setdefault('mainCnsttyNm', '')
                        all_items.append(row)
                    total = int(body.get('totalCount', 0))
                    if page_no * 999 >= total:
                        break
                    page_no += 1
            except Exception as e:
                print(f"   [Error] {sector}: {e}")
                break
        
        if all_items:
            rows = [tuple(item.get(c, '') for c in all_cols) for item in all_items]
            conn.executemany(insert_sql, rows)
            conn.commit()
            print(f"   -> [{sector}] {len(all_items)}건 적재")
        else:
            print(f"   -> [{sector}] 해당일 공고 없음")
    
    conn.close()

def fetch_contract_data(api_url, bgn_date, end_date, page_no=1, num_of_rows=999):
    query = f"?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDate={bgn_date}&inqryEndDate={end_date}&numOfRows={num_of_rows}&pageNo={page_no}&type=json"
    url = api_url + query
    retry = 0
    while retry < 3:
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, context=ctx, timeout=30) as res:
                text = res.read().decode('utf-8')
                data = json.loads(text)
                header = data.get('response', {}).get('header', {})
                if header.get('resultCode') == '00':
                    body = data.get('response', {}).get('body', {})
                    return body.get('items', []), body.get('totalCount', 0)
                else:
                    return [], 0
        except Exception:
            time.sleep(1)
            retry += 1
    return [], 0
    
def download_for_category(api_type, date_str):
    api_url = APIS[api_type]
    items, total_count = fetch_contract_data(api_url, date_str, date_str, page_no=1)
    if total_count == 0:
        return api_type, []
        
    all_items = list(items) if items else []
    total = int(total_count)
    if total > 999:
        total_pages = (total // 999) + 1
        with ThreadPoolExecutor(max_workers=3) as p_executor:
            futures = [p_executor.submit(fetch_contract_data, api_url, date_str, date_str, p) for p in range(2, total_pages + 1)]
            for future in as_completed(futures):
                p_items, _ = future.result()
                if p_items:
                    all_items.extend(p_items)
                
    return api_type, all_items

def sync_one_day(target_date):
    """하루치 데이터 수집 (Step 1~3.6). 성공 시 True 반환.
    API 점검 중이면 즉시 False → catch-up 메커니즘으로 추후 자동 보충."""
    print(f"\n{'='*50}")
    print(f" 📅 {target_date} 데이터 수집 시작")
    print(f"{'='*50}\n")

    # [사전 점검] API 서비스 상태 확인
    print("[API 헬스체크] 조달청 API 서비스 상태 확인 중...")
    if not check_api_health():
        print("   🚫 조달청 API 서비스 점검 중 — 금일 수집을 건너뜁니다.")
        print("   → 점검 완료 후 다음 실행 시 자동 보충 수집됩니다.")
        return False
    print("   ✅ API 정상 응답 확인")
    print("\n--------------------------------------------------")

    # 각 Step 개별 실행 — 한 Step 실패가 나머지를 중단시키지 않음
    failed_steps = []

    # [Step 1] 수요기관 마스터 동기화
    try:
        update_agency_master_daily(target_date)
    except Exception as e:
        print(f"   [오류] Step 1 수요기관 동기화 실패: {e}")
        failed_steps.append('Step1_수요기관')
    print("\n--------------------------------------------------")

    # [Step 1.5] 조달업체 마스터 동기화
    try:
        update_company_master_daily(target_date)
    except Exception as e:
        print(f"   [오류] Step 1.5 조달업체 동기화 실패: {e}")
        failed_steps.append('Step1.5_조달업체')
    print("\n--------------------------------------------------")

    # [Step 1.6] 조달업체 업종정보 동기화
    try:
        update_company_industry_daily(target_date)
    except Exception as e:
        print(f"   [오류] Step 1.6 업종정보 동기화 실패: {e}")
        failed_steps.append('Step1.6_업종정보')
    print("\n--------------------------------------------------")

    # [Step 1.7] 공사 입찰공고 동기화
    try:
        update_bid_notices_daily(target_date)
    except Exception as e:
        print(f"   [오류] Step 1.7 입찰공고 동기화 실패: {e}")
        failed_steps.append('Step1.7_입찰공고')
    print("\n--------------------------------------------------")

    # [Step 1.8] 용역 현장 동기화
    try:
        update_servc_site_daily(target_date)
    except Exception as e:
        print(f"   [오류] Step 1.8 용역현장 동기화 실패: {e}")
        failed_steps.append('Step1.8_용역현장')
    print("\n--------------------------------------------------")

    # [Step 1.9] 낙찰정보 브릿지 동기화
    try:
        update_busan_awards_daily(target_date)
    except Exception as e:
        print(f"   [오류] Step 1.9 낙찰정보 동기화 실패: {e}")
        failed_steps.append('Step1.9_낙찰정보')
    print("\n--------------------------------------------------")

    # [Step 2.0] 입찰공고 추정가격 동기화
    try:
        update_bid_notices_price_daily(target_date)
    except Exception as e:
        print(f"   [오류] Step 2.0 추정가격 동기화 실패: {e}")
        failed_steps.append('Step2.0_추정가격')
    print("\n--------------------------------------------------")

    # [Step 2] 전국 4개 조달계약 다운로드 (★ 핵심 — 실패 시 전체 실패)
    print(f"[전국 계약 동기화] {target_date} 계약 정보 수집 중...")
    all_data = {k: [] for k in APIS.keys()}
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(download_for_category, cat, target_date) for cat in APIS.keys()]
            for future in as_completed(futures):
                cat, items = future.result()
                all_data[cat] = items
                print(f"   -> [{cat}] {target_date}: {len(items):,}건 수집")
    except Exception as e:
        print(f"   [오류] Step 2 계약 데이터 수집 실패: {e}")
        print(f"   → 핵심 데이터 수집 실패 — 전체 실패 처리 (추후 자동 보충)")
        return False

    # [Step 3] 부산 수요기관 계약만 DB 저장
    print(f"\n[로컬 DB 저장] {target_date} 부산 필터 후 저장 중...")
    _conn_ag = sqlite3.connect(AGENCY_DB_PATH)
    busan_codes = set(str(r[0]).strip() for r in _conn_ag.execute("SELECT dminsttCd FROM agency_master").fetchall())
    _conn_ag.close()
    try:
        conn = sqlite3.connect(DB_PATH)
        for cat, items in all_data.items():
            if items:
                df = pd.DataFrame(items)
                for col in df.columns:
                    if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                        df[col] = df[col].astype(str)
                n_before = len(df)
                if 'dminsttCd' in df.columns:
                    df['_cd_clean'] = df['dminsttCd'].astype(str).str.strip()
                    df = df[df['_cd_clean'].isin(busan_codes)].drop(columns=['_cd_clean'])
                table_name = TABLE_MAP[cat]
                
                # 중복 데이터 적재 방어 (Upsert 방식 — dcsnCntrctNo + untyCntrctNo 이중 삭제)
                if not df.empty:
                    cur = conn.cursor()
                    if table_name == 'shopping_cntrct':
                        if 'dlvrReqNo' in df.columns and 'prdctSno' in df.columns and 'dlvrReqChgOrd' in df.columns:
                            df = df.drop_duplicates(subset=['dlvrReqNo', 'prdctSno', 'dlvrReqChgOrd'], keep='last')
                            keys = list(zip(df['dlvrReqNo'].astype(str), df['prdctSno'].astype(str), df['dlvrReqChgOrd'].astype(str)))
                            cur.executemany(f"DELETE FROM {table_name} WHERE dlvrReqNo=? AND prdctSno=? AND dlvrReqChgOrd=?", keys)
                    else:
                        if 'dcsnCntrctNo' in df.columns:
                            df = df.drop_duplicates(subset=['dcsnCntrctNo'], keep='last')
                            # 1차: dcsnCntrctNo 기준 삭제
                            keys = [(str(x),) for x in df['dcsnCntrctNo'].tolist() if pd.notna(x) and str(x).strip() != '']
                            if keys:
                                cur.executemany(f"DELETE FROM {table_name} WHERE dcsnCntrctNo=?", keys)
                            # 2차: untyCntrctNo 기준 삭제 (API가 같은 계약을 다른 날짜에 반환하는 경우 방어)
                            if 'untyCntrctNo' in df.columns:
                                ukeys = [(str(x),) for x in df['untyCntrctNo'].tolist() if pd.notna(x) and str(x).strip() != '']
                                if ukeys:
                                    cur.executemany(f"DELETE FROM {table_name} WHERE untyCntrctNo=?", ukeys)
                    conn.commit()

                df.to_sql(table_name, conn, if_exists='append', index=False)
                print(f"   - {cat} (전국 {n_before:,} → 부산 {len(df):,}) -> '{table_name}'")
        # [Step 3.5] 수요기관코드 파싱
        import re
        for tbl in ['cnstwk_cntrct', 'servc_cntrct', 'thng_cntrct']:
            cur = conn.cursor()
            cur.execute(f"SELECT rowid, dminsttList FROM [{tbl}] WHERE dminsttCd IS NULL AND dminsttList IS NOT NULL AND dminsttList != ''")
            rows = cur.fetchall()
            if rows:
                updated = 0
                for rowid, dl in rows:
                    m = re.search(r'\[1\^(\w+)\^([^^]+)\^', str(dl))
                    if m:
                        conn.execute(f"UPDATE [{tbl}] SET dminsttCd=?, dminsttNm_req=? WHERE rowid=?",
                                     (m.group(1), m.group(2), rowid))
                        updated += 1
                conn.commit()
                if updated: print(f"   - {tbl}: {updated}건 파싱")
        # [Step 3.6] 용역 현장 매칭
        update_servc_site_matching()
        # [Step 3.7] 사전규격 수집
        try:
            sync_prespec(target_date, conn_path=DB_PATH)
        except Exception as e:
            print(f"   [경고] 사전규격 수집 실패 (비핵심): {e}")
            failed_steps.append('Step 3.7 사전규격')
        conn.close()
    except Exception as e:
        print(f"   [오류] DB 적재: {e}")
        return False

    # 실패 요약
    if failed_steps:
        print(f"\n   ⚠️ 일부 Step 실패 ({len(failed_steps)}건): {', '.join(failed_steps)}")
        print(f"   → 핵심 계약 데이터는 정상 수집되어 성공 처리합니다.")

    return True

def get_last_sync_date():
    """sync_log 테이블에서 마지막 성공 날짜 반환"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("CREATE TABLE IF NOT EXISTS sync_log (sync_date TEXT PRIMARY KEY, completed_at TEXT)")
        row = conn.execute("SELECT MAX(sync_date) FROM sync_log").fetchone()
        conn.close()
        if row and row[0]:
            return row[0]
    except:
        pass
    return None

def record_sync_success(target_date):
    """수집 성공 기록"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS sync_log (sync_date TEXT PRIMARY KEY, completed_at TEXT)")
    conn.execute("INSERT OR REPLACE INTO sync_log VALUES (?, ?)",
                 (target_date, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    conn.commit()
    conn.close()

def main():
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    
    # 수동 실행: python daily_pipeline_sync.py 20260315
    if len(sys.argv) > 1:
        target_dates = [sys.argv[1]]
        print(f"[수동 실행] 대상 일자: {sys.argv[1]}")
    else:
        # 자동 catch-up: 마지막 성공일+1 ~ 어제까지 빠진 날짜 모두 수집
        last_sync = get_last_sync_date()
        if last_sync:
            last_dt = datetime.datetime.strptime(last_sync, '%Y%m%d')
            yesterday_dt = datetime.datetime.strptime(yesterday, '%Y%m%d')
            target_dates = []
            dt = last_dt + datetime.timedelta(days=1)
            while dt <= yesterday_dt:
                target_dates.append(dt.strftime('%Y%m%d'))
                dt += datetime.timedelta(days=1)
            if not target_dates:
                print(f"[동기화 완료] 마지막 성공: {last_sync}, 새로 수집할 날짜 없음")
                return
            if len(target_dates) > 1:
                print(f"[자동 보충] 마지막 성공: {last_sync} → {len(target_dates)}일치 보충 수집")
        else:
            target_dates = [yesterday]
    
    start_time = time.time()
    date_range = target_dates[0] if len(target_dates) == 1 else f"{target_dates[0]} ~ {target_dates[-1]} ({len(target_dates)}일)"
    print(f"==================================================")
    print(f" 🔄 부산광역시 조달 데이터 자동화 엔진: Daily Sync")
    print(f"    - 수집 대상: {date_range}")
    print(f"==================================================\n")
    
    # 날짜별 데이터 수집
    success_dates = []
    for target_date in target_dates:
        try:
            # sync_one_day 내부 코드를 여기서 인라인 실행
            ok = sync_one_day(target_date)
            if ok:
                record_sync_success(target_date)
                success_dates.append(target_date)
                print(f"   ✅ {target_date} 수집 성공 (sync_log 기록)")
            else:
                print(f"   ❌ {target_date} 수집 실패")
        except Exception as e:
            print(f"   ❌ {target_date} 수집 중 오류: {e}")
    
    if not success_dates:
        print("\n[경고] 수집 성공한 날짜가 없습니다. 캐시 재생성을 건너뜁니다.")
        return
    target_date = success_dates[-1]  # 이후 Step 4/5에서 사용
    # [Step 4] API 캐시 재생성 (build_api_cache.py → api_cache.json)
    print("\n--------------------------------------------------")
    
    # 캐시 백업 (경보 비교용)
    if os.path.exists('api_cache.json'):
        shutil.copy2('api_cache.json', 'api_cache_prev.json')
        print(f"[캐시 백업] api_cache.json → api_cache_prev.json 백업 완료")
    
    print(f"[캐시 재생성] build_api_cache.py 실행 중...")
    try:
        import subprocess
        result = subprocess.run(
            [sys.executable, 'build_api_cache.py'],
            capture_output=True, text=True, encoding='utf-8', timeout=300
        )
        if result.returncode == 0:
            # 마지막 몇 줄만 출력
            lines = result.stdout.strip().split('\n')
            for line in lines[-5:]:
                print(f"   {line}")
            print(f"   -> 캐시 재생성 완료 ✅")
        else:
            print(f"   [오류] build_api_cache.py 실패: {result.stderr[-200:]}")
            # 이전 캐시로 자동 롤백
            if os.path.exists('api_cache_prev.json'):
                shutil.copy2('api_cache_prev.json', 'api_cache.json')
                print(f"   [롤백] api_cache_prev.json → api_cache.json 복원 완료")
    except Exception as e:
        print(f"   [오류] 캐시 재생성 실패: {e}")
        if os.path.exists('api_cache_prev.json'):
            shutil.copy2('api_cache_prev.json', 'api_cache.json')
            print(f"   [롤백] api_cache_prev.json → api_cache.json 복원 완료")

    # [Step 5] 경보 체크 → 크론 (평일 09:00)에서 별도 실행
    # alert_check.py는 crontab `0 9 * * 1-5`로 독립 실행됨

    # [Step 6] DB 백업 (NCP Object Storage, 7일 보관)
    print("\n--------------------------------------------------")
    try:
        from backup_db import backup_and_upload
        backup_and_upload()
    except Exception as e:
        print(f"   [오류] DB 백업 실패: {e}")
        print(f"   → 파이프라인 자체는 정상 완료됩니다.")

    end_time = time.time()
    print("\n==================================================")
    print(f"🎉 Daily Sync 완료! ({len(success_dates)}일 수집: {', '.join(success_dates)})")
    print(f"총 소요시간: {end_time - start_time:.1f}초")
    print("==================================================")

if __name__ == '__main__':
    main()
