import os
import sqlite3
import datetime
import hashlib
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("CertifiedProductImport")

DB_FILE = os.environ.get("CHATBOT_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chatbot_company.db'))

def setup_db_schema():
    conn = sqlite3.connect(DB_FILE, timeout=5.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    
    # 1. Raw Staging Table (원문 보존 금지, 민감정보 Hash 처리 전용)
    conn.execute('''
    CREATE TABLE IF NOT EXISTS raw_certified_product_import (
        raw_certified_product_import_id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_name TEXT NOT NULL,
        source_file_name TEXT,
        source_row_no INTEGER,
        source_collected_at DATETIME,
        raw_certification_type TEXT,
        raw_certification_no_hash TEXT,
        raw_product_name TEXT,
        raw_company_name TEXT,
        raw_business_no_hash TEXT,
        raw_representative_name_hash TEXT,
        raw_certification_date TEXT,
        raw_expiration_date TEXT,
        raw_product_code TEXT,
        raw_category_code TEXT,
        raw_payload_db_file TEXT,
        raw_payload_table TEXT,
        raw_payload_key TEXT,
        raw_payload_retention_until DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 2. 정규화 테이블
    conn.execute('''
    CREATE TABLE IF NOT EXISTS certified_product (
        certified_product_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER,
        certification_type TEXT NOT NULL,
        certification_type_label TEXT,
        certification_no_hash TEXT,
        product_name TEXT NOT NULL,
        product_name_normalized TEXT,
        product_code TEXT,
        g2b_category_code TEXT,
        certification_date DATE,
        expiration_date DATE,
        validity_status TEXT NOT NULL DEFAULT 'unknown',
        source_name TEXT NOT NULL,
        source_refreshed_at DATETIME,
        match_method TEXT,
        match_status TEXT NOT NULL DEFAULT 'matched',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(company_internal_id) REFERENCES company_master(company_internal_id)
    )
    ''')
    # Idempotency를 위한 복합 Unique Index
    conn.execute('''
    CREATE UNIQUE INDEX IF NOT EXISTS idx_certified_product_unique
    ON certified_product (
        company_internal_id,
        certification_type,
        source_name,
        certification_no_hash,
        product_name_normalized
    )
    ''')

    # 3. 인증유형 Mapping 테이블
    conn.execute('''
    CREATE TABLE IF NOT EXISTS certified_product_type_map (
        raw_certification_type TEXT PRIMARY KEY,
        normalized_certification_type TEXT NOT NULL,
        certification_group TEXT NOT NULL,
        is_priority_purchase_product BOOLEAN DEFAULT 0,
        is_innovation_product BOOLEAN DEFAULT 0,
        is_excellent_procurement_product BOOLEAN DEFAULT 0,
        is_active BOOLEAN DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 4. 매칭 실패 및 매핑 리뷰 테이블
    conn.execute('''
    CREATE TABLE IF NOT EXISTS certified_product_unmatched (
        unmatched_id INTEGER PRIMARY KEY AUTOINCREMENT,
        raw_certified_product_import_id INTEGER,
        source_name TEXT,
        raw_company_name TEXT,
        raw_business_no_hash TEXT,
        raw_product_name TEXT,
        reason TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.execute('''
    CREATE TABLE IF NOT EXISTS certified_product_conflict_log (
        conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER,
        certification_type TEXT,
        certification_no_hash TEXT,
        conflict_reason TEXT,
        source_1 TEXT,
        source_2 TEXT,
        resolved_action TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 5. ETL 로그 및 Source Manifest
    conn.execute('''
    CREATE TABLE IF NOT EXISTS source_manifest (
        source_id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_name TEXT UNIQUE NOT NULL,
        source_type TEXT,
        source_url_or_file TEXT,
        source_refreshed_at DATETIME,
        row_count INTEGER,
        checksum TEXT,
        status TEXT,
        error_message TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_source_manifest_name ON source_manifest(source_name)')
    
    conn.execute('''
    CREATE TABLE IF NOT EXISTS etl_job_log (
        job_id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_name TEXT,
        source_name TEXT,
        started_at DATETIME,
        finished_at DATETIME,
        status TEXT,
        input_row_count INTEGER,
        inserted_count INTEGER,
        updated_count INTEGER,
        skipped_count INTEGER,
        error_count INTEGER,
        error_message TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    _init_seed_mappings(conn)
    conn.commit()
    conn.close()
    logger.info("Database schema setup complete.")

def _init_seed_mappings(conn):
    """기본 인증 매핑 테이블 Seed 구성"""
    seeds = [
        ("성능인증", "performance_certification", "priority_purchase", 1, 0, 0),
        ("우수조달물품지정", "excellent_procurement_product", "priority_purchase", 1, 0, 1),
        ("우수조달물품", "excellent_procurement_product", "priority_purchase", 1, 0, 1),
        ("NEP", "nep_product", "priority_purchase", 1, 0, 0),
        ("신제품인증(NEP)", "nep_product", "priority_purchase", 1, 0, 0),
        ("GS인증", "gs_certified_product", "priority_purchase", 1, 0, 0),
        ("NET", "net_certified_product", "priority_purchase", 1, 0, 0),
        ("신기술인증(NET)", "net_certified_product", "priority_purchase", 1, 0, 0),
        ("혁신제품", "innovation_product", "innovation", 1, 1, 0),
        ("우수연구개발혁신제품", "excellent_rnd_innovation_product", "innovation", 1, 1, 0),
        ("혁신시제품", "innovation_prototype_product", "innovation", 1, 1, 0),
        ("기타혁신제품", "other_innovation_product", "innovation", 1, 1, 0),
        ("재난안전제품인증", "disaster_safety_certified_product", "priority_purchase", 1, 0, 0),
        ("녹색기술제품", "green_technology_product", "priority_purchase", 1, 0, 0),
        ("산업융합 신제품 적합성 인증", "industrial_convergence_new_product", "priority_purchase", 1, 0, 0),
        ("우수조달공동상표", "excellent_procurement_joint_brand", "priority_purchase", 1, 0, 0)
    ]
    for seed in seeds:
        conn.execute('''
        INSERT OR IGNORE INTO certified_product_type_map (
            raw_certification_type, normalized_certification_type, certification_group,
            is_priority_purchase_product, is_innovation_product, is_excellent_procurement_product
        ) VALUES (?, ?, ?, ?, ?, ?)
        ''', seed)

def fetch_mock_data():
    """테스트/Mock 용 데이터 반환"""
    return [
        {
            "cert_type": "성능인증",
            "cert_no": "PERF-2023-01",
            "product_name": "고성능 CCTV",
            "b_no": "1234567890",
            "comp_name": "테스트업체",
            "rep_name": "홍길동",
            "v_from": "2023-01-01",
            "v_to": "2026-12-31"
        },
        {
            "cert_type": "혁신제품",
            "cert_no": "",  # 인증번호 없음 (surrogate hash 테스트)
            "product_name": "AI 영상분석 솔루션",
            "b_no": "1234567890",
            "comp_name": "테스트업체",
            "rep_name": "홍길동",
            "v_from": "2024-01-01",
            "v_to": "2027-12-31"
        },
        {
            "cert_type": "미확인인증", # 매핑에 없는 인증 (unmatched mapping)
            "cert_no": "UNK-001",
            "product_name": "테스트물품",
            "b_no": "1234567890",
            "comp_name": "테스트업체",
            "rep_name": "홍길동",
            "v_from": "2024-01-01",
            "v_to": "2025-12-31"
        },
        {
            "cert_type": "우수조달물품",
            "cert_no": "EXC-002",
            "product_name": "비매칭업체제품",
            "b_no": "0000000000", # 매칭 안 되는 사업자번호
            "comp_name": "타지역업체",
            "rep_name": "김철수",
            "v_from": "2023-01-01",
            "v_to": "2025-12-31"
        }
    ]

def hash_string(val: str) -> str:
    if not val:
        return ""
    return hashlib.sha256(val.encode('utf-8')).hexdigest()

def normalize_product_name(val: str) -> str:
    if not val:
        return ""
    return val.replace(" ", "").lower()

def run_import(dry_run=False, use_mock=False, use_file=False):
    logger.info(f"Starting Certified Product Import. Dry_run={dry_run}, Use_mock={use_mock}, Use_file={use_file}")
    
    conn = sqlite3.connect(DB_FILE, timeout=5.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    
    start_time = datetime.datetime.now()
    now_date = datetime.date.today().isoformat()
    now_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    
    # 1. Mapping table 로드
    cursor = conn.cursor()
    map_rows = cursor.execute("SELECT raw_certification_type, normalized_certification_type FROM certified_product_type_map").fetchall()
    cert_map = {row['raw_certification_type']: row['normalized_certification_type'] for row in map_rows}
    
    # 2. Canonical BNO 로드 (Phase 5: exact match only)
    bno_rows = cursor.execute("SELECT m.company_internal_id, i.canonical_business_no FROM company_master m JOIN company_identity i ON m.company_internal_id = i.company_internal_id").fetchall()
    bno_map = {row['canonical_business_no']: row['company_internal_id'] for row in bno_rows if row['canonical_business_no']}
    
    data = []
    if use_file:
        source_name = "certified_file_sample"
        try:
            import csv
            with open('certified_sample.csv', 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if i >= 100: break
                    data.append(row)
        except Exception as e:
            logger.error(f"Failed to read certified_sample.csv: {e}")
    elif use_mock:
        source_name = "smpp_certified_mock"
        data = fetch_mock_data()
    else:
        source_name = "smpp_certified_api"
        data = []
    
    inserted_count = 0
    skipped_count = 0
    total_count = len(data)
    
    for idx, item in enumerate(data):
        b_no = item.get('b_no', '')
        b_no_hash = hash_string(b_no)
        cert_no = item.get('cert_no', '')
        rep_name = item.get('rep_name', '')
        raw_cert_type = item.get('cert_type', '')
        prod_name = item.get('product_name', '')
        prod_name_norm = normalize_product_name(prod_name)
        v_from = item.get('v_from', '')
        v_to = item.get('v_to', '')
        
        cert_no_hash = hash_string(cert_no)
        rep_name_hash = hash_string(rep_name)
        
        # surrogate hash if cert_no is missing
        if not cert_no:
            surrogate_str = f"{source_name}_{raw_cert_type}_{b_no}_{prod_name_norm}_{v_from}_{v_to}"
            cert_no_hash = hash_string(surrogate_str)
            
        if dry_run:
            continue
            
        # 3. Raw Table (Hash Only)
        cursor.execute('''
            INSERT INTO raw_certified_product_import (
                source_name, source_row_no, source_collected_at, raw_certification_type,
                raw_certification_no_hash, raw_product_name, raw_company_name,
                raw_business_no_hash, raw_representative_name_hash,
                raw_certification_date, raw_expiration_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            source_name, idx+1, now_str, raw_cert_type,
            cert_no_hash, prod_name, item.get('comp_name', ''),
            b_no_hash, rep_name_hash, v_from, v_to
        ))
        raw_id = cursor.lastrowid
        
        # 4. Matching
        internal_id = bno_map.get(b_no)
        if not internal_id:
            # Unmatched
            cursor.execute('''
                INSERT INTO certified_product_unmatched (
                    raw_certified_product_import_id, source_name, raw_company_name,
                    raw_business_no_hash, raw_product_name, reason
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (raw_id, source_name, item.get('comp_name', ''), b_no_hash, prod_name, 'business_no_not_found'))
            skipped_count += 1
            continue
            
        # 5. Type mapping
        normalized_type = cert_map.get(raw_cert_type)
        if not normalized_type:
            # Type mapping missing - record as mapping_review
            cursor.execute('''
                INSERT INTO certified_product_unmatched (
                    raw_certified_product_import_id, source_name, raw_company_name,
                    raw_business_no_hash, raw_product_name, reason
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (raw_id, source_name, item.get('comp_name', ''), b_no_hash, prod_name, f'mapping_missing:{raw_cert_type}'))
            skipped_count += 1
            continue
            
        # 6. Validity
        validity = "valid"
        if v_to and v_to < now_date:
            validity = "expired"
            
        # 7. Upsert certified_product
        cursor.execute('''
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
        ''', (
            internal_id, normalized_type, raw_cert_type, cert_no_hash, prod_name, prod_name_norm,
            v_from, v_to, validity, source_name, now_str, 'exact_bno'
        ))
        inserted_count += 1

    if not dry_run:
        cursor.execute('''
            INSERT INTO etl_job_log (
                job_name, source_name, started_at, finished_at, status, 
                input_row_count, inserted_count, skipped_count, error_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            "import_certified_product", source_name, now_str,
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "success", total_count, inserted_count, skipped_count, 0
        ))
        
        cursor.execute('''
            INSERT INTO source_manifest (source_name, source_type, row_count, source_refreshed_at, status)
            VALUES (?, 'api', ?, ?, 'success')
            ON CONFLICT(source_name) DO UPDATE SET
                row_count=excluded.row_count,
                source_refreshed_at=excluded.source_refreshed_at,
                status='success'
        ''', (source_name, total_count, now_str))
        
        conn.commit()
    
    conn.close()
    logger.info(f"Import Finished. Processed: {total_count}, Inserted/Updated: {inserted_count}, Skipped/Unmatched: {skipped_count}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--setup", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--use-mock", action="store_true")
    parser.add_argument("--probe", action="store_true", help="Run source sample test probe (mock based)")
    parser.add_argument("--file-probe", action="store_true", help="Run source file test probe (CSV based)")
    args = parser.parse_args()
    
    if args.setup:
        setup_db_schema()
    elif args.file_probe:
        run_import(dry_run=args.dry_run, use_mock=False, use_file=True)
    elif args.probe:
        run_import(dry_run=args.dry_run, use_mock=True, use_file=False)
    else:
        run_import(dry_run=args.dry_run, use_mock=args.use_mock, use_file=False)
