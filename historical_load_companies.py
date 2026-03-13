"""
historical_load_companies.py
===========================
부산광역시 지역업체(조달업체) 마스터 SQLite DB 최초 구축 스크립트

[Phase 1] 기존 CSV (조달업체 등록 내역.csv) → 부산+본사 33,074건 1차 시드 임포트
[Phase 2] API (getPrcrmntCorpBasicInfo02) 1990~현재 연도별 병렬 수집 → 부산+본사 Upsert 오버레이
"""

import urllib.request
import json
import ssl
import sqlite3
import pandas as pd
import datetime
import time
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(encoding='utf-8')

# SSL
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

SERVICE_KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'
BASE_URL = 'https://apis.data.go.kr/1230000/ao/UsrInfoService02/getPrcrmntCorpBasicInfo02'
DB_PATH = 'busan_companies_master.db'
CSV_PATH = '조달업체 등록 내역.csv'


def init_db():
    """SQLite DB 및 테이블 생성"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS company_master (
            bizno          TEXT PRIMARY KEY,
            corpNm         TEXT,
            ceoNm          TEXT,
            rgnNm          TEXT,
            adrs           TEXT,
            dtlAdrs        TEXT,
            hdoffceDivNm   TEXT,
            corpBsnsDivNm  TEXT,
            mnfctDivNm     TEXT,
            opbizDt        TEXT,
            rgstDt         TEXT,
            chgDt          TEXT,
            source         TEXT
        )
    ''')
    conn.commit()
    conn.close()


# ================================================================
# Phase 1: CSV → DB 시드 임포트
# ================================================================
def phase1_csv_import():
    """기존 CSV 파일에서 부산+본사 업체만 추출하여 DB에 1차 임포트"""
    print("=" * 60, flush=True)
    print(" [Phase 1] CSV 시드 임포트: 조달업체 등록 내역.csv → DB", flush=True)
    print("=" * 60, flush=True)
    print("   CSV 파일 읽는 중 (119MB, 잠시 대기)...", flush=True)

    try:
        df = pd.read_csv(CSV_PATH, encoding='utf-16', sep='\t', low_memory=False)
    except Exception:
        df = pd.read_csv(CSV_PATH, encoding='utf-8-sig', sep='\t', low_memory=False)

    print(f"   CSV 전체 로딩 완료: {len(df):,}건", flush=True)

    # 부산 + 본사 필터링
    busan_mask = df['업체소재시도'].astype(str).str.contains('부산', na=False)
    hq_mask = df['본사지사구분'].astype(str).str.strip() == '본사'
    df_busan = df[busan_mask & hq_mask].copy()
    print(f"   부산 + 본사 필터링: {len(df_busan):,}건", flush=True)

    # 사업자번호 정제 (하이픈 제거, 공백 제거)
    df_busan['사업자등록번호'] = df_busan['사업자등록번호'].astype(str).str.replace('-', '').str.strip()

    # DB 삽입용 튜플 생성
    records = []
    for _, row in df_busan.iterrows():
        bizno = str(row.get('사업자등록번호', '')).strip()
        if not bizno or bizno == 'nan':
            continue
        records.append((
            bizno,
            str(row.get('업체명', '')).strip(),
            str(row.get('대표자명', '')).strip(),
            f"{row.get('업체소재시도', '')} {row.get('업체소재시군구', '')}".strip(),
            '',  # adrs: CSV에는 상세주소 없음, API에서 보강
            '',  # dtlAdrs
            str(row.get('본사지사구분', '')).strip(),
            str(row.get('대표업종', '')).strip(),
            str(row.get('제조공급구분', '')).strip(),
            str(row.get('개업일자', '')).strip(),
            str(row.get('나라장터등록일자', '')).strip(),
            '',  # chgDt: CSV에 없음
            'csv'
        ))

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT OR IGNORE INTO company_master
        (bizno, corpNm, ceoNm, rgnNm, adrs, dtlAdrs, hdoffceDivNm, corpBsnsDivNm, mnfctDivNm, opbizDt, rgstDt, chgDt, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', records)
    conn.commit()

    inserted = cursor.execute('SELECT COUNT(*) FROM company_master').fetchone()[0]
    conn.close()
    print(f"   ✅ Phase 1 완료: DB 현재 {inserted:,}건 (CSV 시드)", flush=True)
    return inserted


# ================================================================
# Phase 2: API 오버레이 (1990~현재, 연도별 병렬)
# ================================================================
def fetch_companies_for_year(year):
    """특정 연도의 전국 조달업체 전체를 API로 수집 → 부산+본사만 필터링하여 반환"""
    bgn_dt = f"{year}01010000"
    current_year = datetime.datetime.now().year
    end_dt = f"{year}12312359" if year < current_year else datetime.datetime.now().strftime('%Y%m%d') + '2359'

    # 1차: totalCount 확인
    query = f"?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt={bgn_dt}&inqryEndDt={end_dt}&numOfRows=1&pageNo=1&type=json"
    try:
        req = urllib.request.Request(BASE_URL + query, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=ctx, timeout=20) as res:
            data = json.loads(res.read().decode('utf-8'))
            header = data.get('response', {}).get('header', {})
            if header.get('resultCode') != '00':
                return []
            total_count = int(data.get('response', {}).get('body', {}).get('totalCount', 0))
            if total_count == 0:
                return []
    except Exception as e:
        print(f"   [{year}] 초기 조회 실패: {e}")
        return []

    # 2차: 전체 페이지 수집
    all_items = []
    num_of_rows = 999
    total_pages = (total_count // num_of_rows) + 1

    for page in range(1, total_pages + 1):
        qp = f"?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt={bgn_dt}&inqryEndDt={end_dt}&numOfRows={num_of_rows}&pageNo={page}&type=json"
        retry = 0
        while retry < 3:
            try:
                rq = urllib.request.Request(BASE_URL + qp, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(rq, context=ctx, timeout=30) as r:
                    d = json.loads(r.read().decode('utf-8'))
                    items = d.get('response', {}).get('body', {}).get('items', [])
                    all_items.extend(items)
                    break
            except Exception:
                retry += 1
                time.sleep(1)

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

    print(f"   [{year}] 전국 {total_count:,}건 수집 → 부산+본사 {len(busan_companies):,}건 필터링 완료", flush=True)
    return busan_companies


def phase2_api_overlay():
    """API로 1990~현재까지 연도별 병렬 수집하여 DB를 Upsert로 보강"""
    print("\n" + "=" * 60, flush=True)
    print(" [Phase 2] API 오버레이: 1990~현재 연도별 병렬 수집 → Upsert", flush=True)
    print("=" * 60)

    current_year = datetime.datetime.now().year
    years = list(range(1990, current_year + 1))

    all_records = []
    db_lock = threading.Lock()
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_year = {executor.submit(fetch_companies_for_year, yr): yr for yr in years}
        for future in as_completed(future_to_year):
            yr = future_to_year[future]
            try:
                result = future.result()
                if result:
                    # 즉시 DB에 Upsert (API 데이터가 최신이므로 CSV 레코드를 덮어씀)
                    with db_lock:
                        conn = sqlite3.connect(DB_PATH)
                        cursor = conn.cursor()
                        cursor.executemany('''
                            INSERT OR REPLACE INTO company_master
                            (bizno, corpNm, ceoNm, rgnNm, adrs, dtlAdrs, hdoffceDivNm, corpBsnsDivNm, mnfctDivNm, opbizDt, rgstDt, chgDt, source)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', result)
                        conn.commit()
                        conn.close()
                    all_records.extend(result)
            except Exception as e:
                print(f"   [{yr}] 오류: {e}")

    elapsed = time.time() - start_time

    conn = sqlite3.connect(DB_PATH)
    total_db = conn.execute('SELECT COUNT(*) FROM company_master').fetchone()[0]
    api_count = conn.execute("SELECT COUNT(*) FROM company_master WHERE source='api'").fetchone()[0]
    csv_only = conn.execute("SELECT COUNT(*) FROM company_master WHERE source='csv'").fetchone()[0]
    conn.close()

    print(f"\n   ✅ Phase 2 완료 (소요시간: {elapsed:.1f}초)")
    print(f"   - API로 수집 후 Upsert된 부산+본사 업체: {len(all_records):,}건")
    print(f"   - DB 최종 현황:")
    print(f"     · 전체: {total_db:,}건")
    print(f"     · API가 최신화한 레코드: {api_count:,}건")
    print(f"     · CSV에만 존재(API 미보강): {csv_only:,}건 (1984~1989 등 오래된 업체)")


def main():
    print("=" * 60)
    print(" 🚀 부산광역시 지역업체(조달업체) 마스터 DB 최초 구축")
    print("    CSV 시드 임포트 + API 오버레이 하이브리드 방식")
    print("=" * 60 + "\n")

    init_db()

    # Phase 1: CSV 임포트
    csv_count = phase1_csv_import()

    # Phase 2: API 오버레이
    phase2_api_overlay()

    # 최종 요약
    conn = sqlite3.connect(DB_PATH)
    total = conn.execute('SELECT COUNT(*) FROM company_master').fetchone()[0]
    conn.close()

    print("\n" + "=" * 60)
    print(f" 🎉 구축 완료!")
    print(f"    - DB 파일: {DB_PATH}")
    print(f"    - 최종 부산+본사 업체 수: {total:,}건")
    print(f"    - 이후 daily_pipeline_sync.py로 일일 자동 동기화")
    print("=" * 60)


if __name__ == '__main__':
    main()
