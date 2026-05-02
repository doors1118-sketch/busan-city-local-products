"""
기술개발제품 인증현황 API 연동 모듈
- 출처: 중소벤처기업부 공공구매종합정보망(SMPP)
- API: https://api.odcloud.kr/api/3033913/v1/uddi:27bb6889-e56d-4cdc-a222-9f02900c81e7
- 커버: 13종 기술개발제품 인증 전체 (성능인증, NEP, NET, GS, 혁신제품 등)
- 매칭 키: 사업자등록번호
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
logger = logging.getLogger("CertProductAPI")

DB_FILE = os.environ.get("CHATBOT_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chatbot_company.db'))
# API key는 환경변수에서만 로드. 하드코딩 금지.
TECH_PRODUCT_SERVICE_KEY = os.environ.get('TECH_PRODUCT_SERVICE_KEY') or os.environ.get('SERVICE_KEY')

# 최신 Snapshot (2023.11.30) - 사업자등록번호, 인증번호, 인증일자, 만료일자 포함 버전
API_BASE_URL = "https://api.odcloud.kr/api/3033913/v1/uddi:27bb6889-e56d-4cdc-a222-9f02900c81e7"

# 인증구분 → 내부 normalized_certification_type 매핑
CERT_TYPE_MAP = {
    "성능인증": "performance_certification",
    "성능인증제품": "performance_certification",
    "우수조달물품지정": "excellent_procurement_product",
    "우수조달물품": "excellent_procurement_product",
    "NEP": "nep_product",
    "신제품인증(NEP)": "nep_product",
    "GS인증": "gs_certified_product",
    "GS": "gs_certified_product",
    "GS인증(1등급)": "gs_certified_product",
    "NET": "net_certified_product",
    "신기술인증(NET)": "net_certified_product",
    "혁신제품": "innovation_product",
    "우수연구개발혁신제품": "excellent_rnd_innovation_product",
    "혁신시제품": "innovation_prototype_product",
    "기타혁신제품": "other_innovation_product",
    "재난안전제품인증": "disaster_safety_certified_product",
    "녹색기술제품": "green_technology_product",
    "녹색인증제품": "green_technology_product",
    "산업융합 신제품 적합성 인증": "industrial_convergence_new_product",
    "우수조달공동상표": "excellent_procurement_joint_brand",
    # 신규 추가 (API 13종 중 기존 매핑에 없던 것)
    "물산업 우수제품 등 지정": "water_industry_excellent_product",
    "물산업우수제품등지정": "water_industry_excellent_product",
    "물산업우수제품 등 지정": "water_industry_excellent_product",
    "산업융합품목": "industrial_convergence_item",
    "수요처 지정형 기술개발제품": "demand_designated_tech_product",
    "구매조건부신기술개발": "demand_designated_tech_product",
    "민관공동투자기술개발": "demand_designated_tech_product",
    "성과공유기술개발": "demand_designated_tech_product",
    "중소기업융복합기술개발": "demand_designated_tech_product",
    # API 실측 추가분
    "우수산업디자인(GD)": "excellent_industrial_design",
    "GD": "excellent_industrial_design",
}


def hash_string(val: str) -> str:
    if not val:
        return ""
    return hashlib.sha256(val.encode('utf-8')).hexdigest()


def normalize_product_name(val: str) -> str:
    if not val:
        return ""
    return val.replace(" ", "").lower()


def normalize_date(dt_str: str) -> str:
    """YYYYMMDD 또는 YYYY-MM-DD 등 다양한 형식을 YYYY-MM-DD로 정규화"""
    if not dt_str:
        return ""
    dt_str = dt_str.strip()
    # 이미 YYYY-MM-DD 형식
    if len(dt_str) == 10 and dt_str[4] == '-':
        return dt_str
    # YYYYMMDD 형식
    if len(dt_str) == 8 and dt_str.isdigit():
        return f"{dt_str[:4]}-{dt_str[4:6]}-{dt_str[6:]}"
    # YYYY.MM.DD 형식
    if len(dt_str) == 10 and dt_str[4] == '.':
        return dt_str.replace('.', '-')
    return dt_str


def fetch_all_pages(service_key: str, per_page: int = 500, max_pages: int = 200):
    """기술개발제품 API를 전체 페이지 순회하여 모든 데이터를 수집"""
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
            logger.info(f"Fetching page {page} (perPage={per_page})...")
            resp = requests.get(API_BASE_URL, params=params, timeout=30)

            if resp.status_code == 401:
                logger.error("API 인증 실패 (401). 서비스 키를 확인하세요.")
                return None
            if resp.status_code != 200:
                logger.error(f"API HTTP Error: {resp.status_code}")
                return None

            data = resp.json()
            total_count = data.get('totalCount', 0)
            current_count = data.get('currentCount', 0)
            items = data.get('data', [])

            if not items:
                break

            all_data.extend(items)
            logger.info(f"  Page {page}: {current_count} items (total so far: {len(all_data)}/{total_count})")

            if len(all_data) >= total_count:
                break

            page += 1
            time.sleep(0.2)  # Rate limiting

        except requests.RequestException as e:
            logger.error(f"API 요청 실패: {e}")
            return None
        except Exception as e:
            logger.error(f"파싱 실패: {e}")
            return None

    logger.info(f"Total fetched: {len(all_data)} items")
    return all_data


def run_import(dry_run=False, probe=False):
    """
    기술개발제품 인증현황 API를 호출하여 certified_product 테이블에 UPSERT.
    probe=True이면 1페이지(10건)만 호출하여 통신 상태 확인.
    """
    if not TECH_PRODUCT_SERVICE_KEY:
        logger.error("TECH_PRODUCT_SERVICE_KEY 또는 SERVICE_KEY 환경변수가 설정되지 않았습니다.")
        return False

    logger.info(f"Starting 기술개발제품 API Import. dry_run={dry_run}, probe={probe}")

    # 1. API 호출
    if probe:
        items = fetch_all_pages(TECH_PRODUCT_SERVICE_KEY, per_page=10, max_pages=1)
    else:
        items = fetch_all_pages(TECH_PRODUCT_SERVICE_KEY, per_page=500)

    if items is None:
        logger.error("API 호출 실패. 중단.")
        return False

    if not items:
        logger.warning("API 응답 데이터가 없습니다.")
        return True

    # 2. DB 연결 및 매핑 준비
    conn = sqlite3.connect(DB_FILE, timeout=5.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 부산업체 사업자번호 → internal_id 매핑 로드
    bno_rows = cursor.execute("""
        SELECT m.company_internal_id, i.canonical_business_no 
        FROM company_master m 
        JOIN company_identity i ON m.company_internal_id = i.company_internal_id
        WHERE m.is_busan_company = 1
    """).fetchall()
    bno_map = {row['canonical_business_no']: row['company_internal_id'] for row in bno_rows if row['canonical_business_no']}

    now_date = datetime.date.today().isoformat()
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    source_name = "smpp_tech_product_api"

    inserted_count = 0
    skipped_bno = 0
    skipped_mapping = 0
    total_count = len(items)

    for idx, item in enumerate(items):
        # API 필드 추출
        raw_cert_type = str(item.get('인증구분', '')).strip()
        cert_no = str(item.get('인증번호', '')).strip()
        product_name = str(item.get('인증제품명', '')).strip()
        company_name = str(item.get('업체명', '')).strip()
        # 사업자등록번호: API에서 integer로 올 수 있으므로 str 변환 후 하이픈 제거
        b_no_raw = str(item.get('사업자등록번호', '')).strip()
        b_no = b_no_raw.replace('-', '').replace('.0', '').replace(' ', '')
        # 10자리 미만이면 앞에 0 채우기 (API에서 integer로 올 경우 앞자리 0 탈락)
        if b_no and len(b_no) < 10:
            b_no = b_no.zfill(10)
        cert_date = normalize_date(str(item.get('인증일자', '')).strip())
        exp_date = normalize_date(str(item.get('만료일자', '')).strip())

        if not b_no or not raw_cert_type:
            skipped_bno += 1
            continue

        # 부산업체 필터링
        internal_id = bno_map.get(b_no)
        if not internal_id:
            skipped_bno += 1
            continue

        # 인증구분 매핑
        normalized_type = CERT_TYPE_MAP.get(raw_cert_type)
        if not normalized_type:
            # 매핑에 없는 신규 인증구분 → 리뷰 큐에 기록
            if not dry_run:
                cursor.execute("""
                    INSERT INTO certified_product_unmatched (
                        raw_certified_product_import_id, source_name, raw_company_name, raw_business_no_hash, raw_product_name, reason
                    ) VALUES (0, ?, ?, ?, ?, ?)
                """, (source_name, company_name, hash_string(b_no), product_name, f'mapping_missing:{raw_cert_type}'))
            skipped_mapping += 1
            continue

        prod_name_norm = normalize_product_name(product_name)
        cert_no_hash = hash_string(cert_no)

        # 인증번호 없는 경우 surrogate hash 생성
        if not cert_no:
            surrogate = f"{source_name}_{raw_cert_type}_{b_no}_{prod_name_norm}_{cert_date}_{exp_date}"
            cert_no_hash = hash_string(surrogate)

        # validity 계산
        validity = "valid"
        if exp_date and exp_date < now_date:
            validity = "expired"

        if dry_run:
            inserted_count += 1
            continue

        # Raw 로그
        cursor.execute("""
            INSERT INTO raw_certified_product_import (
                source_name, source_row_no, source_collected_at, raw_certification_type,
                raw_certification_no_hash, raw_product_name, raw_company_name,
                raw_business_no_hash, raw_certification_date, raw_expiration_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            source_name, idx + 1, now_str, raw_cert_type,
            cert_no_hash, product_name, company_name,
            hash_string(b_no), cert_date, exp_date
        ))

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
            internal_id, normalized_type, raw_cert_type, cert_no_hash,
            product_name, prod_name_norm,
            cert_date, exp_date, validity,
            source_name, now_str, 'exact_bno'
        ))
        inserted_count += 1

    # ETL 로그 및 Source Manifest
    if not dry_run:
        status = "success"
        cursor.execute("""
            INSERT INTO etl_job_log (
                job_name, source_name, started_at, finished_at, status,
                input_row_count, inserted_count, skipped_count, error_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "import_certified_product_api", source_name, now_str,
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            status, total_count, inserted_count, skipped_bno + skipped_mapping, 0
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
                f"Busan Matched & Inserted: {inserted_count}, "
                f"Skipped(non-Busan/no-BNO): {skipped_bno}, "
                f"Skipped(mapping-missing): {skipped_mapping}")

    if probe:
        print(f"\n=== Probe Result ===")
        print(f"API Total Items (1 page): {total_count}")
        print(f"Busan Matched: {inserted_count}")
        print(f"Skipped (non-Busan): {skipped_bno}")
        print(f"Skipped (mapping): {skipped_mapping}")
        if total_count > 0:
            sample = items[0]
            print(f"\nSample Record:")
            for k, v in sample.items():
                print(f"  {k}: {v}")

    return True


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="기술개발제품 인증현황 API → certified_product 자동 수집")
    parser.add_argument("--dry-run", action="store_true", help="DB 기록 없이 매칭 통계만 확인")
    parser.add_argument("--probe", action="store_true", help="1페이지(10건)만 호출하여 API 통신 테스트")
    args = parser.parse_args()

    success = run_import(dry_run=args.dry_run, probe=args.probe)
    if not success:
        sys.exit(1)
