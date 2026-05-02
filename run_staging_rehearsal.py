import os
import sys
import subprocess
import sqlite3
import json
import logging
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("StagingRehearsal")

STAGING_DB = "staging_chatbot_company.db" # Default, updated in main
TEST_DB = "staging_test_chatbot.db"

# [수정 8] 파일명 정합성 점검 (Canonical Names)
REQUIRED_FILES = [
    "api_server.py",
    "migrate_chatbot_db.py",
    "import_policy_company.py",
    "import_certified_product.py",
    "import_mas_product.py",
    "nts_business_status_client.py",
    "test_phase4.py",
    "test_phase5.py",
    "test_phase6c.py",
    "run_staging_rehearsal.py"
]

def check_canonical_files():
    logger.info("==================================================")
    logger.info("0. 패키지 파일명 정합성 점검")
    logger.info("==================================================")
    missing = []
    for f in REQUIRED_FILES:
        if not os.path.exists(f):
            missing.append(f)
    if missing:
        logger.error(f"Missing required canonical files: {missing}")
        sys.exit(1)
    logger.info("All canonical files verified.")

def run_tests():
    logger.info("==================================================")
    logger.info("3. Mock 회귀 테스트 시작")
    logger.info("==================================================")
    
    env = os.environ.copy()
    env["CHATBOT_DB"] = TEST_DB
    
    for test_file in ["test_phase4.py", "test_phase5.py", "test_phase6c.py"]:
        logger.info(f"Running {test_file}...")
        res = subprocess.run([sys.executable, "-m", "pytest", test_file, "-v"], env=env, capture_output=True, text=True)
        if res.returncode != 0:
            logger.error(f"Test {test_file} failed!")
            logger.error(res.stdout[-1000:])
            sys.exit(1)
        else:
            logger.info(f"{test_file} PASSED.")
            
    if os.path.exists(TEST_DB):
        try:
            os.remove(TEST_DB)
        except:
            pass

def prep_staging_db():
    logger.info("==================================================")
    logger.info("1. Staging DB 준비")
    logger.info("==================================================")
    if os.path.exists(STAGING_DB):
        try:
            os.remove(STAGING_DB)
        except Exception as e:
            logger.warning(f"Failed to remove {STAGING_DB}: {e}")
            
    os.environ["CHATBOT_DB"] = STAGING_DB
    import migrate_chatbot_db
    migrate_chatbot_db.DB_FILE = STAGING_DB
    migrate_chatbot_db.migrate()
    logger.info("Staging DB migration completed.")

def inject_minimal_master_data():
    logger.info("==================================================")
    logger.info("2. 최소 master 데이터 주입")
    logger.info("==================================================")
    conn = sqlite3.connect(STAGING_DB)
    # 부산업체, 비부산업체, 정상, 폐업, 정책기업, MAS 등
    companies = [
        (1, '부산정상기업', 1, '1234567890', 'busan_1'),
        (2, '부산폐업기업', 1, '2345678901', 'busan_2'),
        (3, '서울정상기업', 0, '3456789012', 'seoul_1'),
        (4, '부산정책MAS기업', 1, '4567890123', 'busan_3'),
        (5, '부산인증제품기업', 1, '5678901234', 'busan_4')
    ]
    
    for c_id, name, is_busan, b_no, ext_id in companies:
        conn.execute("INSERT INTO company_master (company_internal_id, company_name, is_busan_company) VALUES (?, ?, ?)", (c_id, name, is_busan))
        conn.execute("INSERT INTO company_identity (company_internal_id, canonical_business_no, company_id) VALUES (?, ?, ?)", (c_id, b_no, ext_id))
    
    conn.commit()
    conn.close()
    logger.info(f"Injected {len(companies)} minimal companies.")

def source_smoke_test(mode):
    logger.info("==================================================")
    logger.info(f"4. Source Smoke Test (Mode: {mode})")
    logger.info("==================================================")
    
    # [수정 2] NTS source smoke fail-closed
    logger.info("4-1. NTS API")
    import nts_business_status_client
    try:
        bno_list = ['1234567890']
        res = nts_business_status_client.check_business_status(bno_list)
        logger.info(f"NTS API Response Success: {res.get('success')}")
        
        # NTS 결과 DB 기록
        if mode in ("source-smoke", "full-staging"):
            conn = sqlite3.connect(STAGING_DB)
            conn.execute('''
                INSERT INTO source_manifest (source_name, source_refreshed_at, row_count, status)
                VALUES (?, datetime('now'), ?, ?)
                ON CONFLICT(source_name) DO UPDATE SET row_count=excluded.row_count, status=excluded.status, source_refreshed_at=datetime('now')
            ''', ('nts_status_probe', len(bno_list), 'success' if res.get('success') else 'failed'))
            conn.commit()
            conn.close()

        if mode in ("source-smoke", "full-staging"):
            if not res.get("success"):
                logger.error("NTS API failed in source-smoke mode (Fail-closed)")
                sys.exit(1)
        assert "serviceKey" not in json.dumps(res), "serviceKey leaked in NTS response!"
    except Exception as e:
        logger.error(f"NTS test failed: {e}")
        if mode in ("source-smoke", "full-staging"):
            sys.exit(1)
            
    # [수정 3,4,5] Run Mock or Probe
    logger.info(f"4-2. Running ETL Imports (Mode: {mode})")
    env = os.environ.copy()
    env["CHATBOT_DB"] = STAGING_DB
    
    scripts = [
        ("import_policy_company.py", "SMPP API"),
        ("import_certified_product.py", "Certified Product"),
        ("import_mas_product.py", "MAS Product")
    ]
    
    for script, name in scripts:
        cmd = [sys.executable, script]
        if mode == "mock":
            if script == "import_policy_company.py":
                cmd.append("--use-mock")
            else:
                cmd.append("--probe") # mock 기반 sample_probe
        elif mode in ("source-smoke", "full-staging"):
            if script == "import_policy_company.py":
                cmd.append("--probe")
            else:
                cmd.append("--file-probe") # 실제 파일 기반 sample_probe
            
        logger.info(f"Running {name}: {' '.join(cmd)}")
        res = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if res.returncode != 0:
            logger.error(f"{name} test failed!")
            logger.error(res.stdout)
            logger.error(res.stderr)
            sys.exit(1)
        else:
            logger.info(f"{name} {mode} completed.")

def verify_etl_logs(mode):
    # [수정 5] source_manifest / etl_job_log 검증 강화
    logger.info("==================================================")
    logger.info(f"4-3. ETL/Source Log Verification ({mode})")
    logger.info("==================================================")
    conn = sqlite3.connect(STAGING_DB)
    conn.row_factory = sqlite3.Row
    
    # 1. source_manifest
    manifest_rows = conn.execute("SELECT * FROM source_manifest").fetchall()
    if not manifest_rows:
        logger.error("source_manifest is empty!")
        sys.exit(1)
        
    for r in manifest_rows:
        logger.info(f"Source: {r['source_name']}, Count: {r['row_count']}, Status: {r['status']}")
        if r['row_count'] == 0:
            logger.warning(f"Warning: {r['source_name']} has 0 rows.")
            
    # source_smoke 모드 특화 검증
    if mode in ("source-smoke", "full-staging"):
        source_names = [r['source_name'] for r in manifest_rows]
        if 'nts_status_probe' not in source_names:
            logger.error("Missing NTS probe source_manifest record!")
            sys.exit(1)
        if 'certified_file_sample' not in source_names and 'certified_sample' not in source_names:
             logger.error("Missing certified sample probe result!")
             sys.exit(1)
        if 'mas_file_sample' not in source_names and 'mas_sample' not in source_names:
             logger.error("Missing mas sample probe result!")
             sys.exit(1)
             
        # DB 실제 적재 확인 (MAS)
        c = conn.cursor()
        if c.execute("SELECT COUNT(*) FROM mas_product").fetchone()[0] == 0:
            logger.error("mas_product count is 0")
            sys.exit(1)
        if c.execute("SELECT COUNT(*) FROM mas_contract").fetchone()[0] == 0:
            logger.error("mas_contract count is 0")
            sys.exit(1)
        if c.execute("SELECT COUNT(*) FROM mas_price_condition").fetchone()[0] == 0:
            logger.error("mas_price_condition count is 0")
            sys.exit(1)
        if c.execute("SELECT COUNT(*) FROM mas_product_unmatched").fetchone()[0] == 0:
            logger.info("mas_product_unmatched count is 0 (All sample data matched)")
        
        # Hash-only 확인 (MAS)
        raw_mas = c.execute("SELECT raw_business_no_hash, raw_contract_no_hash FROM raw_mas_product_import LIMIT 1").fetchone()
        if not raw_mas or '-' in raw_mas[0] or len(raw_mas[0]) < 64:
            logger.error(f"raw_mas_product_import is not hash-only! {raw_mas[0]}")
            sys.exit(1)
            
        # DB 실제 적재 확인 (Certified)
        if c.execute("SELECT COUNT(*) FROM certified_product").fetchone()[0] == 0:
            logger.error("certified_product count is 0")
            sys.exit(1)
        
        # Hash-only 확인 (Certified)
        raw_cert = c.execute("SELECT raw_business_no_hash, raw_certification_no_hash FROM raw_certified_product_import LIMIT 1").fetchone()
        if not raw_cert or '-' in raw_cert[0] or len(raw_cert[0]) < 64:
            logger.error(f"raw_certified_product_import is not hash-only! {raw_cert[0]}")
            sys.exit(1)
            
    # 2. etl_job_log
    job_logs = conn.execute("SELECT * FROM etl_job_log ORDER BY job_id DESC LIMIT 5").fetchall()
    if not job_logs:
        logger.error("etl_job_log is empty!")
        sys.exit(1)
        
    for j in job_logs:
        logger.info(f"Job: {j['job_name']}, Source: {j['source_name']}, Status: {j['status']}, Errors: {j['error_count']}")
        if j['status'] not in ('success', 'partial_success'):
            logger.error(f"ETL Job failed: {j['job_name']} status={j['status']}")
            sys.exit(1)
            
    conn.close()

def api_smoke_test():
    logger.info("==================================================")
    logger.info("5. API Smoke Test & 6. 금지어 스캔")
    logger.info("==================================================")
    
    import api_server
    from fastapi.testclient import TestClient
    
    api_server.CHATBOT_DB = STAGING_DB
    client = TestClient(api_server.app)
    
    endpoints = [
        "/api/company/license-list",
        "/api/company/product-list",
        "/api/ranking",
        "/api/chatbot/company/product-search?product_name=컴퓨터",
        "/api/chatbot/company/detail?company_id=busan_1",
        "/api/chatbot/company/policy-search",
        "/api/chatbot/product/certified-search",
        "/api/chatbot/product/priority-purchase-search",
        "/api/chatbot/mas/search",
        "/api/chatbot/mas/product-search?product_name=데스크톱",
        "/api/chatbot/mas/supplier-search",
        "/api/chatbot/mas/list"
    ]
    
    forbidden_words = [
        "canonical_business_no", "raw_business_no", "contract_no", "contract_no_hash", "raw_contract_no",
        "internal_join_key", "serviceKey", "api_key", "token", "route_codes", "check_codes",
        "구매 가능", "MAS 구매 가능", "종합쇼핑몰 구매 가능", "계약 가능", "수의계약 가능"
    ]
    
    for ep in endpoints:
        logger.info(f"GET {ep}")
        resp = client.get(ep)
        logger.info(f"Status: {resp.status_code}")
        
        # [수정 6] API smoke test fail 조건 강화
        if resp.status_code != 200:
            logger.error(f"Response not 200: {resp.text}")
            sys.exit(1)
            
        try:
            data = resp.json()
            if isinstance(data, dict):
                # [수정 2] error 응답 fail 처리 (None, "", False 등은 무시)
                if data.get("error"):
                    logger.error(f"API returned error in JSON: {data['error']}")
                    sys.exit(1)
                if data.get("company_search_status") == "failed":
                    logger.error(f"API returned failed status: {data.get('error', '')}")
                    sys.exit(1)
        except:
            pass
            
        text = resp.text
        # 금지어 스캔
        for word in forbidden_words:
            if word in text:
                logger.error(f"FORBIDDEN WORD FOUND in {ep}: {word}")
                sys.exit(1)
                
    logger.info("All endpoints tested successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["mock", "source-smoke", "full-staging"], required=True)
    args = parser.parse_args()
    
    mode = args.mode
    
    # [수정 1] mode별 staging DB 분리
    if mode == "source-smoke":
        STAGING_DB = "staging_chatbot_company_source_smoke.db"
    elif mode == "full-staging":
        STAGING_DB = "staging_chatbot_company_full.db"
    else:
        STAGING_DB = "staging_chatbot_company_mock.db"
        
    logger.info(f"Starting run_staging_rehearsal in mode: {mode}, DB: {STAGING_DB}")
    
    check_canonical_files()
    
    # In full-staging, maybe run everything. 
    # In mock or source-smoke, tests can be run too.
    run_tests()
    
    prep_staging_db()
    inject_minimal_master_data()
    
    source_smoke_test(mode)
    verify_etl_logs(mode)
    
    api_smoke_test()
    
    logger.info("==================================================")
    logger.info(f"7. PASS 기준 만족: Staging Rehearsal ({mode}) Completed.")
    logger.info("==================================================")
