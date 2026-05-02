import pytest
import os
import json
import sqlite3
from fastapi.testclient import TestClient
import api_server

@pytest.fixture
def temp_chatbot_db(tmp_path, monkeypatch):
    """임시 SQLite DB를 만들고 api_server의 CHATBOT_DB 경로를 패치 (완벽 격리 테스트 용도)"""
    db_path = tmp_path / "temp_chatbot.db"
    
    conn = sqlite3.connect(str(db_path))
    conn.execute('CREATE TABLE company_master (company_internal_id INTEGER, company_name TEXT, location_sido TEXT, location_detail TEXT, source_refreshed_at DATETIME, is_busan_company INTEGER)')
    conn.execute('CREATE TABLE company_identity (company_internal_id INTEGER, canonical_business_no TEXT, company_id TEXT, identity_source TEXT, identity_status TEXT)')
    
    conn.execute('CREATE TABLE company_license (company_internal_id INTEGER, license_name TEXT, license_name_normalized TEXT)')
    conn.execute('CREATE TABLE company_product (company_internal_id INTEGER, product_name TEXT, product_name_normalized TEXT, g2b_category_code TEXT)')
    conn.execute('CREATE TABLE g2b_product_category (category_code TEXT, category_name TEXT)')
    conn.execute('CREATE TABLE company_manufacturer_status (company_internal_id INTEGER, manufacturer_type TEXT, product_name TEXT)')
    conn.execute('''
    CREATE TABLE IF NOT EXISTS company_business_status (
        company_internal_id INTEGER PRIMARY KEY,
        business_status TEXT NOT NULL DEFAULT 'unknown',
        business_status_freshness TEXT NOT NULL DEFAULT 'not_checked',
        tax_type TEXT,
        closed_at TEXT,
        checked_at DATETIME,
        business_status_source TEXT,
        api_result_code TEXT,
        retry_count INTEGER DEFAULT 0,
        last_error_message TEXT,
        last_attempt_at DATETIME,
        checked_by TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.execute('''
    CREATE TABLE IF NOT EXISTS business_status_refresh_queue (
        queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER NOT NULL,
        priority INTEGER NOT NULL DEFAULT 100,
        reason TEXT NOT NULL,
        requested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        attempt_count INTEGER DEFAULT 0,
        last_attempt_at DATETIME,
        status TEXT DEFAULT 'pending',
        error_message TEXT
    )
    ''')
    
    # 픽스처 셋업 시 더미 로우 삽입 (is_busan_company=1)
    conn.execute("INSERT INTO company_master (company_internal_id, company_name, location_sido, location_detail, source_refreshed_at, is_busan_company) VALUES (1, '테스트업체', '부산광역시', '해운대구', '2026-05-02', 1)")
    conn.execute("INSERT INTO company_identity (company_internal_id, canonical_business_no, company_id, identity_source, identity_status) VALUES (1, '1234567890', 'TEST_HASH_ID', 'g2b', 'verified')")
    
    # 비부산 업체 삽입 (is_busan_company=0)
    conn.execute("INSERT INTO company_master (company_internal_id, company_name, location_sido, location_detail, source_refreshed_at, is_busan_company) VALUES (2, '타지역업체', '서울특별시', '강남구', '2026-05-02', 0)")
    conn.execute("INSERT INTO company_identity (company_internal_id, canonical_business_no, company_id, identity_source, identity_status) VALUES (2, '0987654321', 'OTHER_HASH_ID', 'g2b', 'verified')")
    
    # Phase 2 데이터 삽입
    conn.execute("INSERT INTO company_license (company_internal_id, license_name, license_name_normalized) VALUES (1, '정보통신공사업', '정보통신공사업')")
    conn.execute("INSERT INTO company_product (company_internal_id, product_name, product_name_normalized, g2b_category_code) VALUES (1, '컴퓨터', '컴퓨터', '43211501')")
    conn.execute("INSERT INTO g2b_product_category (category_code, category_name) VALUES ('43211501', '데스크톱컴퓨터')")
    conn.execute("INSERT INTO company_manufacturer_status (company_internal_id, manufacturer_type, product_name) VALUES (1, 'manufacturer', '컴퓨터')")

    # 비부산 업체 Phase 2 데이터
    conn.execute("INSERT INTO company_license (company_internal_id, license_name, license_name_normalized) VALUES (2, '정보통신공사업', '정보통신공사업')")
    
    # Phase 2 뷰 생성
    conn.execute('''
    CREATE VIEW IF NOT EXISTS chatbot_company_candidate_view AS
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
        '[]' AS policy_subtypes,
        IFNULL((SELECT manufacturer_type FROM company_manufacturer_status cms WHERE cms.company_internal_id = m.company_internal_id LIMIT 1), 'unknown') AS manufacturer_type,
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
    
    conn.commit()
    conn.close()

    # api_server의 전역 변수를 임시 DB 경로로 패치
    monkeypatch.setattr(api_server, "CHATBOT_DB", str(db_path))
    
    # Mock nts_business_status_client
    def mock_check_business_status(b_nos):
        res = {"success": True, "is_quota_exceeded": False, "results": {}}
        for b in b_nos:
            if b == "1234567890":
                res["results"][b] = {"business_status": "active", "tax_type": "일반과세자", "closed_at": "", "api_result_code": "01"}
            elif b == "0987654321":
                res["results"][b] = {"business_status": "closed", "tax_type": "폐업자", "closed_at": "2023-01-01", "api_result_code": "03"}
            elif b == "9999999999":
                res["success"] = False # 강제 실패 모의
                res["error_message"] = "Unknown Business No"
            else:
                res["results"][b] = {"business_status": "unknown", "tax_type": "", "closed_at": "", "api_result_code": ""}
        return res
        
    monkeypatch.setattr("nts_business_status_client.check_business_status", mock_check_business_status)
    monkeypatch.setenv("NTS_BUSINESS_STATUS_SERVICE_KEY", "MOCK_KEY")
    
    yield str(db_path)

@pytest.fixture
def client():
    return TestClient(api_server.app)

FORBIDDEN_KEYWORDS = [
    "사업자등록번호", "businessNo", "business_no", "biz_no", "bizNo",
    "법인등록번호", "internal_join_key", "raw_json", "token", "serviceKey",
    "api_key", "secret", "HMAC secret", "email", "휴대전화"
]

def scan_forbidden(text):
    for key in FORBIDDEN_KEYWORDS:
        assert key not in text, f"금지어 '{key}' 발견됨!"

def test_chatbot_invalid_company_id(client, temp_chatbot_db):
    """유효하지 않은 company_id 요청 시 표준 실패 응답 확인"""
    resp = client.get("/api/chatbot/company/detail?company_id=INVALID123")
    assert resp.status_code == 200
    data = resp.json()
    assert data["company_search_status"] == "failed"
    assert data["error"] == "유효하지 않거나 만료된 업체 식별자입니다."
    assert "Exception" not in data.get("error", "")

def test_chatbot_valid_company_id_unauthorized(client, temp_chatbot_db):
    """유효한 company_id 지만 미인증 시 응답 통제 및 전체 JSON 금지어 스캔"""
    resp = client.get("/api/chatbot/company/detail?company_id=TEST_HASH_ID")
    assert resp.status_code == 200
    data = resp.json()
    print("DEBUG DATA:", data)
    assert data["company_search_status"] == "success"
    
    candidate = data["candidates"][0]
    assert "route_codes" not in candidate
    assert "check_codes" not in candidate
    assert "candidate_types" in candidate
    assert "policy_subtypes" in candidate
    assert candidate["representative_name"] is None
    assert candidate["corporate_phone"] is None
    scan_forbidden(resp.text)

def test_chatbot_valid_company_id_authorized(client, temp_chatbot_db):
    resp = client.get("/api/chatbot/company/detail?company_id=TEST_HASH_ID", headers={"X-Internal-Auth": "INTERNAL_VALID_TOKEN"})
    assert resp.status_code == 200
    candidate = resp.json()["candidates"][0]
    assert candidate["representative_name"] is None
    assert candidate["corporate_phone"] is None

def test_phase2_busan_company_filter(client, temp_chatbot_db):
    """비부산 업체가 검색 결과에서 제외되는지 확인"""
    resp = client.get("/api/chatbot/company/license-search?license_name=정보통신")
    assert resp.status_code == 200
    data = resp.json()
    # 2개 업체 중 1개만 반환되어야 함 (비부산 제외)
    assert len(data["candidates"]) == 1
    assert data["candidates"][0]["company_id"] == "TEST_HASH_ID"
    assert data["candidates"][0]["is_busan_company"] == 1

def test_phase2_limit_offset_validation(client, temp_chatbot_db):
    """limit=-1, offset=-1 요청이 422 에러가 되는지 확인"""
    resp = client.get("/api/chatbot/company/license-search?license_name=정보통신&limit=-1")
    assert resp.status_code == 422
    
    resp2 = client.get("/api/chatbot/company/license-search?license_name=정보통신&offset=-1")
    assert resp2.status_code == 422

def test_phase2_license_search(client, temp_chatbot_db):
    resp = client.get("/api/chatbot/company/license-search?license_name=정보통신")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["candidates"]) == 1
    assert "meta" in data
    assert "candidate_counts_by_type" in data["meta"]
    scan_forbidden(resp.text)

def test_phase2_product_search(client, temp_chatbot_db):
    resp = client.get("/api/chatbot/company/product-search?product_name=컴퓨터")
    assert resp.status_code == 200
    scan_forbidden(resp.text)

def test_phase2_category_search(client, temp_chatbot_db):
    resp = client.get("/api/chatbot/company/category-search?category_name=데스크톱")
    assert resp.status_code == 200
    scan_forbidden(resp.text)

def test_phase2_manufacturers(client, temp_chatbot_db):
    resp = client.get("/api/chatbot/company/manufacturers")
    assert resp.status_code == 200
    scan_forbidden(resp.text)

def test_phase2_list_endpoints_aggregation(client, temp_chatbot_db):
    """list API가 후보 전체가 아니라 목록명+count를 반환하는지 확인"""
    resp = client.get("/api/chatbot/company/license-list?limit=50")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["candidates"]) == 1
    candidate = data["candidates"][0]
    assert "license_name" in candidate
    assert "candidate_count" in candidate
    assert candidate["candidate_count"] == 1
    # 민감 정보 필드가 아예 없는지 확인
    assert "business_no" not in candidate
    assert "company_name" not in candidate
    scan_forbidden(resp.text)
    
    resp2 = client.get("/api/chatbot/company/product-list?limit=50")
    assert resp2.status_code == 200
    assert "product_name" in resp2.json()["candidates"][0]
    scan_forbidden(resp2.text)
    
    resp3 = client.get("/api/chatbot/company/category-list?limit=50")
    assert resp3.status_code == 200
    assert "category_code" in resp3.json()["candidates"][0]
    scan_forbidden(resp3.text)

def test_phase3_business_status_cache(client, temp_chatbot_db):
    """Phase 3-B/3-C On-Demand 영업상태 캐싱 로직 및 Fallback 확인"""
    # 최초 조회: NTS API(Mock) 호출 및 active 반환 확인
    resp1 = client.get("/api/chatbot/company/detail?company_id=TEST_HASH_ID")
    assert resp1.status_code == 200
    data1 = resp1.json()
    assert data1["candidates"][0]["business_status"] == "active"
    assert data1["candidates"][0]["business_status_freshness"] == "fresh"

    # 알 수 없는 사업자번호(Mock API에서 unknown 응답) 테스트
    import sqlite3
    conn = sqlite3.connect(temp_chatbot_db)
    conn.execute("INSERT INTO company_master (company_internal_id, company_name, location_sido, is_busan_company) VALUES (99, '미확인업체', '부산광역시', 1)")
    conn.execute("INSERT INTO company_identity (company_internal_id, canonical_business_no, company_id) VALUES (99, '9999999999', 'UNKNOWN_HASH')")
    conn.commit()
    conn.close()
    
    resp_unk = client.get("/api/chatbot/company/detail?company_id=UNKNOWN_HASH")
    assert resp_unk.status_code == 200
    data_unk = resp_unk.json()
    assert data_unk["candidates"][0]["business_status"] == "unknown"
    assert data_unk["candidates"][0]["business_status_freshness"] == "api_failed"

def test_phase3_status_filter(client, temp_chatbot_db):
    """검색 API의 status_filter 기능 확인"""
    # 강제로 상태 주입
    import sqlite3
    conn = sqlite3.connect(temp_chatbot_db)
    # TEST_HASH_ID (internal=1) -> closed
    conn.execute("INSERT INTO company_business_status (company_internal_id, business_status, business_status_freshness) VALUES (1, 'closed', 'fresh')")
    conn.commit()
    conn.close()

    # 1. exclude_closed (기본값) -> 검색 결과에 안나와야 함
    resp1 = client.get("/api/chatbot/company/license-search?license_name=정보통신")
    assert len(resp1.json()["candidates"]) == 0
    
    # 2. all -> 나와야 함
    resp2 = client.get("/api/chatbot/company/license-search?license_name=정보통신&status_filter=all")
    assert len(resp2.json()["candidates"]) == 1
    assert resp2.json()["candidates"][0]["business_status"] == "closed"

    # 3. active_only -> 안나와야 함
    resp3 = client.get("/api/chatbot/company/license-search?license_name=정보통신&status_filter=active_only")
    assert len(resp3.json()["candidates"]) == 0

    # 4. needs_check -> 안나와야 함 (fresh 상태이므로)
    resp4 = client.get("/api/chatbot/company/license-search?license_name=정보통신&status_filter=needs_check")
    assert len(resp4.json()["candidates"]) == 0

    # 5. Invalid status_filter -> 422 (Literal Validation)
    resp5 = client.get("/api/chatbot/company/license-search?license_name=정보통신&status_filter=invalid_value")
    assert resp5.status_code == 422

def test_phase3_ttl_cache_logic(client, temp_chatbot_db, monkeypatch):
    """TTL 7일 이내/초과에 따른 API 호출 여부 검증"""
    import sqlite3
    import datetime
    
    # Mocking check_business_status to track calls
    call_count = {"count": 0}
    def mock_check_business_status(b_nos):
        call_count["count"] += 1
        return {"success": True, "is_quota_exceeded": False, "results": {b_nos[0]: {"business_status": "active", "tax_type": "", "closed_at": "", "api_result_code": "01"}}}
        
    monkeypatch.setattr("nts_business_status_client.check_business_status", mock_check_business_status)
    monkeypatch.setenv("NTS_BUSINESS_STATUS_SERVICE_KEY", "MOCK_KEY")
    
    conn = sqlite3.connect(temp_chatbot_db)
    # 강제로 3일 전 체크된 데이터 주입 (TTL 이내) -> Adapter 미호출 확인
    three_days_ago = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("INSERT INTO company_business_status (company_internal_id, business_status, checked_at) VALUES (1, 'active', ?)", (three_days_ago,))
    conn.commit()
    conn.close()
    
    resp1 = client.get("/api/chatbot/company/detail?company_id=TEST_HASH_ID")
    assert resp1.status_code == 200
    assert call_count["count"] == 0 # Adapter 호출 안 됨

    # 강제로 8일 전 체크된 데이터로 변경 (TTL 초과) -> Adapter 호출 확인
    conn = sqlite3.connect(temp_chatbot_db)
    eight_days_ago = (datetime.datetime.now() - datetime.timedelta(days=8)).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("UPDATE company_business_status SET checked_at = ? WHERE company_internal_id = 1", (eight_days_ago,))
    conn.commit()
    conn.close()

    resp2 = client.get("/api/chatbot/company/detail?company_id=TEST_HASH_ID")
    assert resp2.status_code == 200
    assert call_count["count"] == 1 # Adapter 호출됨

def test_phase3_batch_dry_run(temp_chatbot_db, monkeypatch):
    """Baseline 및 Incremental 배치의 Dry-run 및 Quota Exceeded 처리 검증"""
    import sqlite3
    from refresh_business_status_baseline import run_baseline_refresh
    from refresh_business_status_incremental import run_incremental_refresh
    
    monkeypatch.setattr("refresh_business_status_baseline.CHATBOT_DB", temp_chatbot_db)
    monkeypatch.setattr("refresh_business_status_incremental.CHATBOT_DB", temp_chatbot_db)
    
    # 1. Baseline Dry-run
    run_baseline_refresh(dry_run=True)
    
    # 2. Incremental Refresh Queue에 Quota Exceeded 유발
    conn = sqlite3.connect(temp_chatbot_db)
    conn.execute("INSERT INTO business_status_refresh_queue (company_internal_id, reason, status) VALUES (1, 'test', 'pending')")
    conn.commit()
    conn.close()
    
    # Mock NTS to return quota exceeded
    def mock_quota_exceeded(b_nos):
        return {"success": False, "is_quota_exceeded": True, "error_message": "Quota Exceeded"}
        
    monkeypatch.setattr("refresh_business_status_incremental.check_business_status", mock_quota_exceeded)
    
    run_incremental_refresh(dry_run=False)
    
    # Queue 상태 확인: Quota Exceeded 시에는 영구 failed가 아니라 pending으로 남아야 함
    conn = sqlite3.connect(temp_chatbot_db)
    row = conn.execute("SELECT status, error_message FROM business_status_refresh_queue WHERE company_internal_id = 1").fetchone()
    conn.close()
    
    assert row is not None
    assert row[0] == "pending"
    assert "Quota" in row[1]

