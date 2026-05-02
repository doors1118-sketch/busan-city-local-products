import pytest
import sqlite3
import datetime
from fastapi.testclient import TestClient
from api_server import app, _get_chatbot_db
from import_policy_company import run_import

def _assert_no_forbidden_words(resp_text: str):
    forbidden = [
        "사업자등록번호", "canonical_business_no", "raw_business_no", "raw_certification_no",
        "certification_no", "cert_no", "internal_join_key", "raw_payload", "raw_json",
        "route_codes", "check_codes", "수의계약 가능", "계약 가능", "우선구매 가능"
    ]
    for word in forbidden:
        assert word not in resp_text, f"Forbidden word found: {word}"

@pytest.fixture
def temp_chatbot_db(tmp_path, monkeypatch):
    """Phase 4 테스트를 위한 임시 데이터베이스 환경 구축"""
    db_path = tmp_path / "temp_chatbot.db"
    
    conn = sqlite3.connect(str(db_path))
    
    # 기본 마스터 테이블
    conn.execute('CREATE TABLE company_master (company_internal_id INTEGER PRIMARY KEY, company_name TEXT, location_sido TEXT, location_detail TEXT, source_refreshed_at DATETIME, is_busan_company INTEGER)')
    conn.execute('CREATE TABLE company_identity (company_internal_id INTEGER, canonical_business_no TEXT, company_id TEXT)')
    conn.execute('CREATE TABLE company_license (company_internal_id INTEGER, license_name TEXT, license_name_normalized TEXT)')
    conn.execute('CREATE TABLE company_product (company_internal_id INTEGER, product_name TEXT, product_name_normalized TEXT, g2b_category_code TEXT)')
    conn.execute('CREATE TABLE company_manufacturer_status (company_internal_id INTEGER, manufacturer_type TEXT, product_name TEXT)')
    conn.execute('CREATE TABLE company_business_status (company_internal_id INTEGER, business_status TEXT, business_status_freshness TEXT, checked_at DATETIME, business_status_source TEXT)')
    
    # Phase 4 테이블
    conn.execute('''
    CREATE TABLE IF NOT EXISTS raw_policy_company_import (
        raw_policy_import_id INTEGER PRIMARY KEY AUTOINCREMENT,
        policy_source_type TEXT NOT NULL,
        source_file_name TEXT,
        raw_company_name TEXT,
        raw_business_no_hash TEXT,
        raw_certification_no_hash TEXT,
        raw_valid_from TEXT,
        raw_valid_to TEXT,
        imported_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.execute('''
    CREATE TABLE IF NOT EXISTS policy_company_certification (
        policy_cert_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER NOT NULL,
        policy_type TEXT NOT NULL,
        policy_subtype TEXT NOT NULL,
        certification_no_hash TEXT,
        certification_valid_from DATE,
        certification_valid_to DATE,
        validity_status TEXT NOT NULL DEFAULT 'unknown',
        issuer TEXT,
        source_name TEXT,
        source_refreshed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_policy_company_cert_unique ON policy_company_certification(company_internal_id, policy_subtype, source_name, certification_no_hash)')
    conn.execute('''
    CREATE TABLE policy_company_unmatched (
        unmatched_id INTEGER PRIMARY KEY AUTOINCREMENT,
        raw_policy_import_id INTEGER,
        policy_source_type TEXT,
        raw_company_name TEXT,
        raw_business_no_hash TEXT,
        reason TEXT
    )''')
    conn.execute('''
    CREATE TABLE IF NOT EXISTS source_manifest (
        source_id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_name TEXT UNIQUE NOT NULL,
        source_type TEXT,
        row_count INTEGER DEFAULT 0,
        source_refreshed_at DATETIME,
        status TEXT,
        last_error_message TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_source_manifest_name ON source_manifest(source_name)')
    
    conn.execute('''
    CREATE TABLE IF NOT EXISTS etl_job_log (
        log_id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_name TEXT NOT NULL,
        source_name TEXT,
        started_at DATETIME,
        finished_at DATETIME,
        status TEXT,
        input_row_count INTEGER,
        inserted_count INTEGER,
        skipped_count INTEGER,
        error_count INTEGER,
        error_message TEXT
    )
    ''')# View
    conn.execute('DROP VIEW IF EXISTS chatbot_company_candidate_view')
    conn.execute('''
    CREATE VIEW chatbot_company_candidate_view AS
    SELECT 
        i.company_id,
        m.company_name,
        m.location_sido AS location,
        m.location_detail AS detail_address,
        m.is_busan_company,
        1 AS is_headquarters,
        (SELECT GROUP_CONCAT(license_name, '|') FROM company_license cl WHERE cl.company_internal_id = m.company_internal_id) AS license_or_business_type,
        (SELECT GROUP_CONCAT(product_name, '|') FROM company_product cp WHERE cp.company_internal_id = m.company_internal_id) AS main_products,
        '["local_procurement_company"]' AS candidate_types,
        'local_procurement_company' AS primary_candidate_type,
        
        (SELECT GROUP_CONCAT(policy_subtype || ':' || validity_status, '|') FROM policy_company_certification pcc WHERE pcc.company_internal_id = m.company_internal_id) AS policy_subtypes_raw,
        
        'unknown' AS manufacturer_type,
        'unknown' AS business_status,
        'not_checked' AS business_status_freshness,
        '후보' AS display_status,
        0 AS contract_possible_auto_promoted,
        
        '["company_master"]' AS source_refs,
        m.source_refreshed_at
    FROM company_master m
    JOIN company_identity i ON m.company_internal_id = i.company_internal_id
    WHERE m.is_busan_company = 1
    ''')

    # Dummy Data
    conn.execute("INSERT INTO company_master (company_internal_id, company_name, is_busan_company) VALUES (1, '테스트업체', 1)")
    conn.execute("INSERT INTO company_identity (company_internal_id, canonical_business_no, company_id) VALUES (1, '1234567890', 'TEST_HASH_1')")
    
    conn.commit()
    conn.close()
    
    import api_server
    import import_policy_company
    monkeypatch.setattr(api_server, "CHATBOT_DB", str(db_path))
    monkeypatch.setattr(import_policy_company, "DB_FILE", str(db_path))
    
    # Mock NTS Client for detail search
    def mock_check_business_status(*args, **kwargs):
        return {"success": True, "results": {"1234567890": {"business_status": "active", "tax_type": "", "closed_at": "", "api_result_code": "01"}}}
    monkeypatch.setattr("nts_business_status_client.check_business_status", mock_check_business_status)

    yield str(db_path)

@pytest.fixture
def client():
    return TestClient(app)

def test_phase4_import_etl(temp_chatbot_db):
    """1~8: ETL Import 로직 확인 (Exact match, Unmatched, Valid/Expired 분리, Multiple 누적)"""
    run_import(dry_run=False, use_mock=True)
    
    conn = sqlite3.connect(temp_chatbot_db)
    conn.row_factory = sqlite3.Row
    
    # 1. 2건 Insert 확인 (여성, 사회적)
    certs = conn.execute("SELECT * FROM policy_company_certification ORDER BY policy_subtype").fetchall()
    assert len(certs) == 2
    
    # 2. 여성기업(valid)
    assert certs[1]['policy_subtype'] == 'women_company'
    assert certs[1]['validity_status'] == 'valid'
    
    # 3. 사회적기업(expired) - 2024년까지이므로 만료됨
    assert certs[0]['policy_subtype'] == 'social_enterprise'
    assert certs[0]['validity_status'] == 'expired'
    
    # 4. Unmatched is no longer applicable in API pull model
    
    # 5. etl_job_log
    log = conn.execute("SELECT * FROM etl_job_log WHERE job_name='import_policy_company'").fetchone()
    assert log['inserted_count'] == 2
    
    conn.close()

def test_phase4_policy_search_api(client, temp_chatbot_db):
    """9: /policy-search 응답 및 valid 필터 확인"""
    run_import(dry_run=False, use_mock=True)
    
    # Default: valid_only (여성기업 1건만 나와야 함)
    resp = client.get("/api/chatbot/company/policy-search")
    assert resp.status_code == 200
    _assert_no_forbidden_words(resp.text)
    data = resp.json()
    assert len(data["candidates"]) == 1
    c = data["candidates"][0]
    
    # Valid 인증은 policy_subtypes에 포함됨, Expired는 포함 안 됨
    assert "women_company" in c["policy_subtypes"]
    assert "social_enterprise" not in c["policy_subtypes"]
    assert c["policy_validity_summary"]["women_company"] == "valid"
    
    # Multiple Policy (candidate_types에 policy_company 추가 여부)
    assert "policy_company" in c["candidate_types"]
    assert c["primary_candidate_type"] == "policy_company"
    
    # 민감정보 미포함 확인
    assert "canonical_business_no" not in c
    assert "raw_business_no" not in c
    assert "route_codes" not in c
    assert c["contract_possible_auto_promoted"] == False

def test_phase4_policy_list_api(client, temp_chatbot_db):
    """10: /policy-list 통계 집계 응답 확인"""
    run_import(dry_run=False, use_mock=True)
    
    resp = client.get("/api/chatbot/company/policy-list")
    assert resp.status_code == 200
    data = resp.json()
    
    # women_company, social_enterprise 통계 확인
    assert len(data["candidates"]) == 2
    for c in data["candidates"]:
        if c["policy_subtype"] == "women_company":
            assert c["valid_count"] == 1
            assert c["expired_count"] == 0
        elif c["policy_subtype"] == "social_enterprise":
            assert c["valid_count"] == 0
            assert c["expired_count"] == 1

def test_phase4_license_search_api(client, temp_chatbot_db):
    """11, 16: 기존 API에 policy_subtypes 정상 연동 및 법적 결론 문구 노출 금지"""
    # 라이선스 데이터 추가
    conn = sqlite3.connect(temp_chatbot_db)
    conn.execute("INSERT INTO company_license (company_internal_id, license_name) VALUES (1, '정보통신공사업')")
    conn.commit()
    conn.close()
    
    run_import(dry_run=False, use_mock=True)
    
    resp = client.get("/api/chatbot/company/license-search?license_name=정보통신")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["candidates"]) == 1
    c = data["candidates"][0]
    
    # 속성 병합 여부
    assert "local_procurement_company" in c["candidate_types"]
    assert "policy_company" in c["candidate_types"]
    assert "women_company" in c["policy_subtypes"]
    assert c["primary_candidate_type"] == "local_procurement_company" # 원래 타입 유지
    
    # 금지된 법적 결론 필드 없음
    json_str = resp.text
    assert "수의계약 가능" not in json_str
    assert "계약 가능" not in json_str
    assert "우선구매 가능" not in json_str

def test_phase4_api_validation(client, temp_chatbot_db):
    """기타 Validation 엣지 케이스 테스트"""
    # 1. Invalid status_filter
    resp = client.get("/api/chatbot/company/policy-search?status_filter=invalid_value")
    assert resp.status_code == 422
    
    # 2. Invalid validity_filter
    resp2 = client.get("/api/chatbot/company/policy-search?validity_filter=invalid_value")
    assert resp2.status_code == 422
    
    # 3. Invalid policy_subtype
    resp3 = client.get("/api/chatbot/company/policy-search?policy_subtype=invalid_value")
    assert resp3.status_code == 422
