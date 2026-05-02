import pytest
import sqlite3
import datetime
from api_server import app
from fastapi.testclient import TestClient
import import_certified_product

@pytest.fixture
def temp_chatbot_db(tmp_path, monkeypatch):
    db_path = tmp_path / "temp_chatbot.db"
    
    # 기본 스키마 생성 (import_certified_product 등에서 필요한 최소한의 테이블)
    conn = sqlite3.connect(str(db_path))
    
    # 마스터/아이덴티티
    conn.execute('CREATE TABLE company_master (company_internal_id INTEGER PRIMARY KEY, company_name TEXT, location_sido TEXT, location_detail TEXT, source_refreshed_at DATETIME, is_busan_company INTEGER)')
    conn.execute('CREATE TABLE company_identity (company_internal_id INTEGER, canonical_business_no TEXT, company_id TEXT)')
    
    # dummy master 데이터 삽입 (1번 정상, 2번 미매칭 테스트용 제외)
    conn.execute("INSERT INTO company_master (company_internal_id, company_name, is_busan_company) VALUES (1, '테스트업체', 1)")
    conn.execute("INSERT INTO company_identity (company_internal_id, canonical_business_no, company_id) VALUES (1, '1234567890', 'TEST_HASH_1')")
    
    # 더미 뷰 생성
    conn.execute('''
    CREATE VIEW chatbot_company_candidate_view AS
    SELECT 
        i.company_id,
        m.company_name,
        '부산광역시' AS location,
        '상세주소' AS detail_address,
        1 AS is_busan_company,
        1 AS is_headquarters,
        NULL AS license_or_business_type,
        NULL AS main_products,
        '["local_procurement_company"]' AS candidate_types,
        'local_procurement_company' AS primary_candidate_type,
        NULL AS policy_subtypes_raw,
        
        -- Phase 5: 인증제품 연동
        (SELECT GROUP_CONCAT(cp.certification_type || ':' || 
                 IFNULL(map.is_priority_purchase_product, 0) || ':' || 
                 IFNULL(map.is_innovation_product, 0) || ':' || 
                 IFNULL(map.is_excellent_procurement_product, 0) || ':' || 
                 cp.validity_status, '|') 
         FROM certified_product cp 
         LEFT JOIN certified_product_type_map map ON cp.certification_type = map.normalized_certification_type 
         WHERE cp.company_internal_id = m.company_internal_id) AS certified_product_types_raw,
        
        (SELECT GROUP_CONCAT(
            cp.certification_type || '^^' || 
            cp.product_name || '^^' || 
            cp.validity_status || '^^' || 
            IFNULL(cp.expiration_date, '') || '^^' || 
            cp.source_name, '|||') 
         FROM certified_product cp WHERE cp.company_internal_id = m.company_internal_id) AS certified_product_summary_raw,
         
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
    
    conn.execute('''
    CREATE TABLE IF NOT EXISTS company_business_status (
        company_internal_id INTEGER PRIMARY KEY,
        business_status TEXT,
        business_status_freshness TEXT,
        tax_type TEXT,
        closed_at TEXT,
        api_result_code TEXT,
        checked_at DATETIME,
        business_status_source TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    conn.commit()
    conn.close()
    
    # DB 위치 변경
    monkeypatch.setattr(import_certified_product, "DB_FILE", str(db_path))
    import api_server
    monkeypatch.setattr(api_server, "CHATBOT_DB", str(db_path))
    
    # 스키마 셋업
    import_certified_product.setup_db_schema()
    
    yield str(db_path)

def test_phase5_hash_only_raw_table(temp_chatbot_db):
    """raw 테이블에 민감정보 원문이 들어가지 않는지 검증 (Hash-only)"""
    conn = sqlite3.connect(temp_chatbot_db)
    cursor = conn.execute("PRAGMA table_info(raw_certified_product_import)")
    columns = {row[1] for row in cursor.fetchall()}
    conn.close()
    
    assert "raw_business_no_hash" in columns
    assert "raw_certification_no_hash" in columns
    assert "raw_representative_name_hash" in columns
    
    assert "raw_business_no" not in columns
    assert "raw_certification_no" not in columns
    assert "raw_representative_name" not in columns

def test_phase5_import_mock_data(temp_chatbot_db):
    """mock 데이터를 import하여 정규화, surrogate hash, unmatched가 올바르게 작동하는지 확인"""
    import_certified_product.run_import(dry_run=False, use_mock=True)
    
    conn = sqlite3.connect(temp_chatbot_db)
    conn.row_factory = sqlite3.Row
    
    # 1. 정규화 확인 (성능인증, 혁신제품 -> 2건)
    certs = conn.execute("SELECT * FROM certified_product ORDER BY certification_type").fetchall()
    assert len(certs) == 2
    
    types = [c['certification_type'] for c in certs]
    assert "innovation_product" in types
    assert "performance_certification" in types
    
    # Surrogate Hash 확인 (인증번호 없는 혁신제품)
    innov = [c for c in certs if c['certification_type'] == 'innovation_product'][0]
    assert innov['certification_no_hash'] != ""
    assert len(innov['certification_no_hash']) == 64 # SHA-256 length
    
    # 2. Unmatched 확인 (매핑 안됨 1건, 사업자번호 없음 1건)
    unmatched = conn.execute("SELECT reason, raw_product_name FROM certified_product_unmatched ORDER BY reason").fetchall()
    assert len(unmatched) == 2
    reasons = [u['reason'] for u in unmatched]
    
    assert "business_no_not_found" in reasons
    # mapping_missing:미확인인증
    assert any(r.startswith("mapping_missing") for r in reasons)
    
    conn.close()

def test_phase5_idempotency(temp_chatbot_db):
    """동일 데이터를 2번 import 했을 때 row_count가 불변하는지 확인"""
    import_certified_product.run_import(dry_run=False, use_mock=True)
    
    conn = sqlite3.connect(temp_chatbot_db)
    count1 = conn.execute("SELECT COUNT(*) FROM certified_product").fetchone()[0]
    
    # 2번째 import
    import_certified_product.run_import(dry_run=False, use_mock=True)
    
    count2 = conn.execute("SELECT COUNT(*) FROM certified_product").fetchone()[0]
    conn.close()
    
    assert count1 == count2
    assert count1 == 2

@pytest.fixture
def client(temp_chatbot_db):
    return TestClient(app)

def test_phase5_certified_search_api(temp_chatbot_db, client):
    """/api/chatbot/product/certified-search 통합 검색 테스트"""
    import_certified_product.run_import(dry_run=False, use_mock=True)
    resp = client.get("/api/chatbot/product/certified-search")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["candidates"]) > 0
    
    # expired 필터 검증 (기본값이 valid_only)
    innov = [c for c in data["candidates"] if "innovation_product" in c.get("certified_product_types", [])]
    assert len(innov) > 0
    assert "priority_purchase_product" in innov[0]["candidate_types"] or "innovation_product" in innov[0]["candidate_types"]
    
    # 50 초과 제한 에러 검증
    resp51 = client.get("/api/chatbot/product/certified-search?limit=51")
    assert resp51.status_code == 422

def test_phase5_priority_purchase_search_api(temp_chatbot_db, client):
    """우선구매대상(기술개발제품) 검색 테스트"""
    import_certified_product.run_import(dry_run=False, use_mock=True)
    resp = client.get("/api/chatbot/product/priority-purchase-search")
    assert resp.status_code == 200
    data = resp.json()
    
    # 성능인증은 priority_purchase_product 매핑됨
    assert any("priority_purchase_product" in c["candidate_types"] for c in data["candidates"])

def test_phase5_innovation_search_api(temp_chatbot_db, client):
    """혁신제품 전용 검색 테스트"""
    import_certified_product.run_import(dry_run=False, use_mock=True)
    resp = client.get("/api/chatbot/product/innovation-search")
    assert resp.status_code == 200
    data = resp.json()
    
    # 혁신제품 매핑 검증
    assert any("innovation_product" in c["candidate_types"] for c in data["candidates"])

def test_phase5_forbidden_words_scan(temp_chatbot_db, client):
    """응답 전체 금지어 스캔 (계약 가능, 수의계약 등)"""
    import_certified_product.run_import(dry_run=False, use_mock=True)
    resp = client.get("/api/chatbot/product/certified-search")
    assert resp.status_code == 200
    
    import json
    resp_text = json.dumps(resp.json(), ensure_ascii=False)
    
    forbidden_words = ["수의계약 가능", "계약 가능", "우선구매 가능", "route_codes", "check_codes"]
    for word in forbidden_words:
        assert word not in resp_text, f"금지어 '{word}' 노출됨"

    assert "1234567890" not in resp_text, "사업자등록번호 원문 노출됨"
    assert "PERF-2023-01" not in resp_text, "인증번호 원문 노출됨"

