"""
혁신장터 지정 및 특허 관련 정보 API 연동 모듈
- 출처: 조달청 혁신조달플랫폼
- API: https://api.odcloud.kr/api/15154028/v1/uddi:343426a8-607a-404c-8dd1-b4bbc3ffeee6
- 커버: 혁신제품 유형1(우수연구개발), 유형2(혁신시제품)
- 매칭 키: 지정기관명(업체명) — 사업자번호 미제공
- 보충 정보: 여성기업여부, 청년기업여부, 상품식별번호 등
"""
import os
import sys
import sqlite3
import datetime
import hashlib
import logging
import requests
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("InnovationProductAPI")

DB_FILE = os.environ.get("CHATBOT_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chatbot_company.db'))
INNOVATION_SERVICE_KEY = os.environ.get('INNOVATION_SERVICE_KEY') or os.environ.get('SERVICE_KEY')

API_BASE_URL = "https://api.odcloud.kr/api/15154028/v1/uddi:343426a8-607a-404c-8dd1-b4bbc3ffeee6"

# 혁신상품구분명 → 내부 코드 매핑
INNOVATION_TYPE_MAP = {
    "우수연구개발혁신제품": "excellent_rnd_innovation_product",
    "우수연구개발 혁신제품": "excellent_rnd_innovation_product",
    "혁신시제품": "innovation_prototype_product",
    "기타혁신제품": "other_innovation_product",
    "혁신제품": "innovation_product",
}


def hash_string(val: str) -> str:
    if not val:
        return ""
    return hashlib.sha256(val.encode('utf-8')).hexdigest()


def normalize_name(val: str) -> str:
    if not val:
        return ""
    return val.replace(" ", "").replace("(주)", "").replace("㈜", "").replace("주식회사", "").strip().lower()


def normalize_date(dt_str: str) -> str:
    if not dt_str:
        return ""
    dt_str = str(dt_str).strip()
    if len(dt_str) == 10 and dt_str[4] == '-':
        return dt_str
    if len(dt_str) == 8 and dt_str.isdigit():
        return f"{dt_str[:4]}-{dt_str[4:6]}-{dt_str[6:]}"
    if '.' in dt_str:
        return dt_str.replace('.', '-')[:10]
    return dt_str


def fetch_all_pages(service_key: str, per_page: int = 500, max_pages: int = 100):
    """혁신제품 API 전체 페이지 순회"""
    all_data = []
    page = 1

    while page <= max_pages:
        params = {
            'serviceKey': service_key,
            'page': page,
            'perPage': per_page,
            'returnType': 'JSON'
        }
        try:
            logger.info(f"Fetching page {page}...")
            resp = requests.get(API_BASE_URL, params=params, timeout=30)

            if resp.status_code == 401:
                logger.error("API 인증 실패 (401)")
                return None
            if resp.status_code != 200:
                logger.error(f"API HTTP Error: {resp.status_code}")
                return None

            data = resp.json()
            total_count = data.get('totalCount', 0)
            items = data.get('data', [])

            if not items:
                break

            all_data.extend(items)
            logger.info(f"  Page {page}: {len(items)} items (total: {len(all_data)}/{total_count})")

            if len(all_data) >= total_count:
                break

            page += 1
            time.sleep(0.2)

        except Exception as e:
            logger.error(f"API 호출 실패: {e}")
            return None

    logger.info(f"Total fetched: {len(all_data)} items")
    return all_data


def run_import(dry_run=False, probe=False):
    if not INNOVATION_SERVICE_KEY:
        logger.error("INNOVATION_SERVICE_KEY 또는 SERVICE_KEY 환경변수가 설정되지 않았습니다.")
        return False

    logger.info(f"Starting 혁신제품 API Import. dry_run={dry_run}, probe={probe}")

    # 1. API 호출
    if probe:
        items = fetch_all_pages(INNOVATION_SERVICE_KEY, per_page=10, max_pages=1)
    else:
        items = fetch_all_pages(INNOVATION_SERVICE_KEY, per_page=500)

    if items is None:
        logger.error("API 호출 실패.")
        return False

    if not items:
        logger.warning("API 응답 데이터가 없습니다.")
        return True

    # 2. DB 연결
    conn = sqlite3.connect(DB_FILE, timeout=5.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 부산업체 업체명 → internal_id 매핑 (normalized name)
    name_rows = cursor.execute("""
        SELECT company_internal_id, company_name, company_name_normalized
        FROM company_master
        WHERE is_busan_company = 1
    """).fetchall()

    name_map = {}
    for row in name_rows:
        norm = row['company_name_normalized'] or normalize_name(row['company_name'])
        if norm:
            name_map[norm] = row['company_internal_id']
        # 원본 이름도 등록
        orig_norm = normalize_name(row['company_name'])
        if orig_norm and orig_norm not in name_map:
            name_map[orig_norm] = row['company_internal_id']

    now_date = datetime.date.today().isoformat()
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    source_name = "pps_innovation_market_api"

    inserted_count = 0
    skipped_name = 0
    skipped_mapping = 0
    total_count = len(items)

    for idx, item in enumerate(items):
        raw_type = str(item.get('혁신상품구분명', '')).strip()
        product_name = str(item.get('항목명', '')).strip()
        company_name = str(item.get('지정기관명', '')).strip()
        cert_date = normalize_date(str(item.get('지정일자', '')))
        valid_start = normalize_date(str(item.get('유효시작일자', '')))
        valid_end = normalize_date(str(item.get('유효종료일자', '')))
        use_yn = str(item.get('사용여부', '')).strip()
        designation_no = str(item.get('혁신제품지정생성번호', '')).strip()

        if not raw_type or not company_name:
            skipped_name += 1
            continue

        # 업체명 매칭
        company_norm = normalize_name(company_name)
        internal_id = name_map.get(company_norm)

        if not internal_id:
            # 부산업체 매칭 실패 → unmatched 기록
            if not dry_run:
                cursor.execute("""
                    INSERT INTO certified_product_unmatched (
                        raw_certified_product_import_id, source_name, raw_company_name, raw_product_name, reason
                    ) VALUES (0, ?, ?, ?, ?)
                """, (source_name, company_name, product_name, 'company_name_not_matched'))
            skipped_name += 1
            continue

        # 인증구분 매핑
        normalized_type = INNOVATION_TYPE_MAP.get(raw_type)
        if not normalized_type:
            if not dry_run:
                cursor.execute("""
                    INSERT INTO certified_product_unmatched (
                        raw_certified_product_import_id, source_name, raw_company_name, raw_product_name, reason
                    ) VALUES (0, ?, ?, ?, ?)
                """, (source_name, company_name, product_name, f'mapping_missing:{raw_type}'))
            skipped_mapping += 1
            continue

        prod_name_norm = normalize_name(product_name)

        # 인증번호 hash (지정생성번호 활용, 없으면 surrogate)
        if designation_no:
            cert_no_hash = hash_string(designation_no)
        else:
            surrogate = f"{source_name}_{raw_type}_{company_norm}_{prod_name_norm}_{cert_date}"
            cert_no_hash = hash_string(surrogate)

        # validity 계산
        validity = "valid"
        if use_yn == 'N':
            validity = "expired"
        elif valid_end and valid_end < now_date:
            validity = "expired"

        if dry_run:
            inserted_count += 1
            continue

        # UPSERT certified_product
        cursor.execute("""
            INSERT INTO certified_product (
                company_internal_id, certification_type, certification_type_label,
                certification_no_hash, product_name, product_name_normalized,
                certification_date, expiration_date, validity_status,
                source_name, source_refreshed_at, match_method
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(company_internal_id, certification_type, source_name, certification_no_hash, product_name_normalized) DO UPDATE SET
                certification_date=excluded.certification_date,
                expiration_date=excluded.expiration_date,
                validity_status=excluded.validity_status,
                source_refreshed_at=excluded.source_refreshed_at,
                updated_at=CURRENT_TIMESTAMP
        """, (
            internal_id, normalized_type, raw_type, cert_no_hash,
            product_name, prod_name_norm,
            cert_date, valid_end, validity,
            source_name, now_str, 'company_name_match'
        ))
        inserted_count += 1

    if not dry_run:
        cursor.execute("""
            INSERT INTO etl_job_log (
                job_name, source_name, started_at, finished_at, status,
                input_row_count, inserted_count, skipped_count, error_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "import_innovation_product_api", source_name, now_str,
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "success", total_count, inserted_count, skipped_name + skipped_mapping, 0
        ))

        cursor.execute("""
            INSERT INTO source_manifest (source_name, source_type, row_count, source_refreshed_at, status)
            VALUES (?, 'api_full', ?, ?, 'success')
            ON CONFLICT(source_name) DO UPDATE SET
                row_count=excluded.row_count,
                source_refreshed_at=excluded.source_refreshed_at,
                status='success'
        """, (source_name, inserted_count, now_str))

        conn.commit()

    conn.close()

    logger.info(f"Import Finished. API Total: {total_count}, "
                f"Matched & Inserted: {inserted_count}, "
                f"Skipped(name-miss): {skipped_name}, "
                f"Skipped(mapping): {skipped_mapping}")

    if probe:
        print(f"\n=== Probe Result ===")
        print(f"API Total Items: {total_count}")
        print(f"Busan Matched: {inserted_count}")
        print(f"Skipped (name miss): {skipped_name}")
        print(f"Skipped (mapping): {skipped_mapping}")
        if items:
            print(f"\nSample Record:")
            for k, v in items[0].items():
                print(f"  {k}: {v}")

    return True


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="혁신장터 지정정보 API → certified_product 자동 수집")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--probe", action="store_true", help="1페이지(10건)만 호출하여 API 통신 테스트")
    args = parser.parse_args()

    success = run_import(dry_run=args.dry_run, probe=args.probe)
    if not success:
        sys.exit(1)
