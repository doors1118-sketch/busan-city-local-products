import sqlite3
import urllib.request
import urllib.parse
import json
import ssl
import sys
import time
import os

sys.stdout.reconfigure(encoding='utf-8')
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'
DB_PATH = 'c:/Users/COMTREE/Desktop/연습/procurement_contracts.db'
AGENCY_DB_PATH = 'c:/Users/COMTREE/Desktop/연습/busan_agencies_master.db'

# ── 부산 수요기관 마스터 DB 로드 (수요기관코드 + 사업자번호 세트) ──
def _load_busan_agency_master():
    """busan_agencies_master.db에서 수요기관코드(dminsttCd) 및 사업자번호(bizno)를 
    한 번만 메모리에 적재하여, 이후 O(1) 조회로 부산 관내 기관 여부를 판별합니다."""
    conn_ag = sqlite3.connect(AGENCY_DB_PATH)
    cursor = conn_ag.cursor()
    cursor.execute('SELECT dminsttCd, bizno, dminsttNm FROM agency_master')
    rows = cursor.fetchall()
    conn_ag.close()

    codes = set()
    biznos = set()
    names = set()
    for cd, bz, nm in rows:
        if cd: codes.add(str(cd).strip())
        if bz: biznos.add(str(bz).replace('-', '').strip())
        if nm: names.add(str(nm).strip())
    return codes, biznos, names

BUSAN_AGENCY_CODES, BUSAN_AGENCY_BIZNOS, BUSAN_AGENCY_NAMES = _load_busan_agency_master()
print(f"[마스터 DB 로드 완료] 부산 수요기관코드 {len(BUSAN_AGENCY_CODES)}개, 사업자번호 {len(BUSAN_AGENCY_BIZNOS)}개")

def is_busan_agency(agency_nm, agency_cd):
    """수요기관 마스터 DB 절대 기준으로 부산 관내 기관 여부를 판별합니다.
    1순위: 수요기관코드(dminsttCd)로 조회
    2순위: 수요기관명(dminsttNm)으로 조회
    ※ 단순 텍스트 '부산' 포함 여부로 판별하지 않습니다."""
    # 1순위: 수요기관코드가 마스터 DB에 존재하는지 확인
    if agency_cd and str(agency_cd).strip() in BUSAN_AGENCY_CODES:
        return True
    # 2순위: 수요기관명이 마스터 DB에 정확히 존재하는지 확인
    if agency_nm and str(agency_nm).strip() in BUSAN_AGENCY_NAMES:
        return True
    return False

def check_cnstwk_location(bid_ntce_no, conn):
    if not bid_ntce_no: return None
    
    # 길이가 짧으면(예: 11자리 공고번호) 검색 시도 (대리기호 포함 제외)
    if '-' in bid_ntce_no or len(bid_ntce_no) > 15:
        return None
        
    cursor = conn.cursor()
    
    # 1. 1차 관문: 자체 구축된 공사 입찰공고 마스터 DB (bid_master_raw) 먼저 조회 (API 통신 0초 컷)
    cursor.execute('SELECT cnstrtsiteRgnNm FROM bid_master_raw WHERE bidNtceNo = ?', (bid_ntce_no,))
    row = cursor.fetchone()
    if row and row[0]:
        return row[0]
        
    # 2. 2차 관문 (Fallback): 로컬 마스터가 털렸을 경우에만 최후의 수단으로 실시간 API 1회 단건 호출
    url = f'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwk?serviceKey={SERVICE_KEY}&inqryDiv=1&bidNtceNo={bid_ntce_no}&numOfRows=1&pageNo=1&type=json'
    
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=ctx, timeout=5) as res:
            text = res.read().decode('utf-8')
            data = json.loads(text)
            items = data.get('response', {}).get('body', {}).get('items', [])
            if items:
                rgn = items[0].get('cnstrtsiteRgnNm', None)
                if rgn:
                    # 보강 발견된 귀중한 주소를 우리 마스터 DB에 영구 보존 (다음 조회시를 위해)
                    nm = items[0].get('bidNtceNm', '')
                    cursor.execute('''
                        INSERT OR IGNORE INTO bid_master_raw (bidNtceNo, bidNtceNm, cnstrtsiteRgnNm)
                        VALUES (?, ?, ?)
                    ''', (bid_ntce_no, nm, rgn))
                    conn.commit()
                return rgn
    except Exception as e:
        pass
        
    return None

def process_contracts():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 3차 테스트: 공사, 용역, 물품 모두 섞일 수 있도록 무작위(Random) 500건 추출
    cursor.execute('''
        SELECT cntrctNo, bsnsDivNm, bidNtceNo, dminsttNm, dminsttCd, cntrctNm 
        FROM contracts_raw 
        WHERE is_processed = 0
        ORDER BY RANDOM()
        LIMIT 5000
    ''')
    rows = cursor.fetchall()
    
    print(f'개선된 로직으로 5000건 검증 시작...')
    
    update_data = []
    log_data = []
    
    for row in rows:
        cntrct_no, bsns_div, bid_no, agency_nm, agency_cd, cntrct_nm = row
        
        final_rgn = '알수없음'
        is_local = 0
        
        # 1. 공사 분야 (입찰공고 마스터 조인 최우선 판단 -> API Fallback)
        if bsns_div == '공사':
            rgn = None
            if bid_no:
                rgn = check_cnstwk_location(bid_no, conn)
                time.sleep(0.01) # 무분별한 API 부하 방지
                
            if rgn:
                final_rgn = rgn
                if '부산' in rgn:
                    is_local = 1
                else:
                    log_data.append(('CONTRACT', cntrct_no, '공사 API 타지역 적발', rgn))
            else:
                # 공고 매칭 실패(또는 수의계약 등 번호 없음) 시, 수요기관으로 대체 판별
                if is_busan_agency(agency_nm, agency_cd):
                    final_rgn = f'[수요기관대체] {agency_nm}'
                    is_local = 1
                    
                    # 텍스트 마이닝 스캐너 적용 (단양/충주 등 포함 시 허수 처리)
                    blacklist = ['충주', '단양', '서울', '제주', '강원', '경기', '인천', '전남', '경북']
                    if cntrct_nm:
                        for word in blacklist:
                            if word in cntrct_nm:
                                is_local = 0
                                final_rgn = f'[마이닝관외적발] {word} 포함'
                                log_data.append(('CONTRACT', cntrct_no, f'텍스트마이닝 적발 - {word}', cntrct_nm))
                                break
                else:
                    final_rgn = f'[관외대체] {agency_nm}'
                    is_local = 0
                    
        # 2. 물품 / 용역 분야
        else:
            if is_busan_agency(agency_nm, agency_cd):
                final_rgn = f'[수요기관] {agency_nm}'
                is_local = 1
                
                # 마이닝 블랙리스트
                blacklist = ['충주', '단양', '서울', '제주', '강원', '경기', '인천', '전남', '경북']
                if cntrct_nm:
                    for word in blacklist:
                        if word in cntrct_nm:
                            is_local = 0
                            final_rgn = f'[마이닝관외적발] {word} 포함'
                            log_data.append(('CONTRACT', cntrct_no, f'텍스트마이닝 적발 - {word}', cntrct_nm))
                            break
            else:
                final_rgn = f'[타지역기관] {agency_nm}'
                is_local = 0
                
        update_data.append((final_rgn, is_local, 1, cntrct_no))
        
    cursor.executemany('''
        UPDATE contracts_raw 
        SET final_location_rgn = ?, final_is_local = ?, is_processed = ?
        WHERE cntrctNo = ?
    ''', update_data)
    
    cursor.executemany('''
        INSERT INTO filtered_logs (source_type, original_id, filter_reason, target_text)
        VALUES (?, ?, ?, ?)
    ''', log_data)
    
    conn.commit()
    
    cursor.execute('SELECT final_is_local, COUNT(*) FROM contracts_raw WHERE is_processed = 1 AND final_is_local=1')
    local_cnt = cursor.fetchone()[1]
    cursor.execute('SELECT final_is_local, COUNT(*) FROM contracts_raw WHERE is_processed = 1 AND final_is_local=0')
    out_cnt = cursor.fetchone()[1]
    
    print(f'\\n[검증 결과] 부산실적 인정: {local_cnt}건 / 관외유출(또는 타지역): {out_cnt}건')
    if log_data:
        print(f'[마이닝/상세 API 적발 허수 예외데이터]: {len(log_data)}건 (exception_logs 대기열에 저장됨)')
    
    conn.close()

if __name__ == '__main__':
    process_contracts()
