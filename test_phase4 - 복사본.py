import pytest
import sqlite3
import json
from fastapi.testclient import TestClient
from api_server import app
import import_policy_company

def _assert_no_forbidden_words(resp_text: str):
    forbidden = [
        "사업자등록번호", "canonical_business_no", "raw_business_no", "raw_certification_no",
        "certification_no", "cert_no", "internal_join_key", "raw_payload", "raw_json",
        "route_codes", "check_codes", "수의계약 가능", "계약 가능", "우선구매 가능"
    ]
    for word in forbidden:
        assert word not in resp_text, f"Forbidden word found: {word}"

def _assert_no_raw_exception(resp_json: dict):
    """error 필드에 traceback/Exception 클래스명이 없는지 확인"""
    err = resp_json.get("error", "")
    if not err:
        return
    # 허용되는 고정 문구
    allowed = ["업체 목록 조회 실패", "정책기업 조회 실패", "유효하지 않거나 만료된 업체 식별자입니다."]
    assert err in allowed, f"Raw exception leaked: {err}"

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
    conn.execute('CREATE TABLE company_business_status (company_internal_id INTEGER PRIMARY KEY, business_status TEXT DEFAULT "unknown", business_status_freshness TEXT DEFAULT "not_checked", tax_type TEXT, closed_at TEXT, checked_at DATETIME, business_status_source TEXT, api_result_code TEXT)')

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
    ''')

    # View
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
        NULL AS certified_product_types_raw,
        NULL AS certified_product_summary_raw,

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
    monkeypatch.setattr(api_server, "CHATBOT_DB", str(db_path))
    monkeypatch.setattr(import_policy_company, "DB_FILE", str(db_path))

    # Mock NTS Client - inject module into sys.modules since it doesn't exist locally
    import sys
    import types
    mock_nts = types.ModuleType("nts_business_status_client")
    def mock_check_business_status(b_nos):
        results = {}
        for b in b_nos:
            results[b] = {"business_status": "active", "tax_type": "", "closed_at": "", "api_result_code": "01"}
        return {"success": True, "is_quota_exceeded": False, "results": results}
    mock_nts.check_business_status = mock_check_business_status
    sys.modules["nts_business_status_client"] = mock_nts

    yield str(db_path)

@pytest.fixture
def client():
    return TestClient(app)

# ── ETL Import 테스트 ──

def test_phase4_import_etl(temp_chatbot_db):
    """ETL Import 로직: Exact match, Valid/Expired 분리"""
    import_policy_company.run_import(dry_run=False, use_mock=True)

    conn = sqlite3.connect(temp_chatbot_db)
    conn.row_factory = sqlite3.Row

    certs = conn.execute("SELECT * FROM policy_company_certification ORDER BY policy_subtype").fetchall()
    assert len(certs) == 2

    # 여성기업(valid)
    assert certs[1]['policy_subtype'] == 'women_company'
    assert certs[1]['validity_status'] == 'valid'

    # 사회적기업(expired) - 2024년까지이므로 만료
    assert certs[0]['policy_subtype'] == 'social_enterprise'
    assert certs[0]['validity_status'] == 'expired'

    # etl_job_log
    log = conn.execute("SELECT * FROM etl_job_log WHERE job_name='import_policy_company'").fetchone()
    assert log['inserted_count'] == 2

    conn.close()

def test_phase4_etl_idempotency(temp_chatbot_db):
    """같은 import를 2회 실행해도 row count가 증가하지 않는다"""
    import_policy_company.run_import(dry_run=False, use_mock=True)

    conn = sqlite3.connect(temp_chatbot_db)
    count1 = conn.execute("SELECT COUNT(*) FROM policy_company_certification").fetchone()[0]
    conn.close()

    # 2회차 실행
    import_policy_company.run_import(dry_run=False, use_mock=True)

    conn = sqlite3.connect(temp_chatbot_db)
    count2 = conn.execute("SELECT COUNT(*) FROM policy_company_certification").fetchone()[0]
    conn.close()

    assert count1 == count2, f"Idempotency violation: {count1} -> {count2}"

# ── API 응답 테스트 ──

def test_phase4_policy_search_api(client, temp_chatbot_db):
    """policy-search 응답: valid 필터, 금지어, expired 분리"""
    import_policy_company.run_import(dry_run=False, use_mock=True)

    resp = client.get("/api/chatbot/company/policy-search")
    assert resp.status_code == 200
    _assert_no_forbidden_words(resp.text)
    _assert_no_raw_exception(resp.json())
    data = resp.json()
    assert len(data["candidates"]) == 1
    c = data["candidates"][0]

    # Valid만 policy_subtypes에 포함, expired는 미포함
    assert "women_company" in c["policy_subtypes"]
    assert "social_enterprise" not in c["policy_subtypes"]
    assert c["policy_validity_summary"]["women_company"] == "valid"
    assert c["policy_validity_summary"]["social_enterprise"] == "expired"

    # candidate_types에 policy_company 추가
    assert "policy_company" in c["candidate_types"]
    assert c["primary_candidate_type"] == "policy_company"

    # 민감정보 미포함
    assert "canonical_business_no" not in c
    assert c["contract_possible_auto_promoted"] == False

def test_phase4_policy_list_api(client, temp_chatbot_db):
    """policy-list 통계 집계 응답"""
    import_policy_company.run_import(dry_run=False, use_mock=True)

    resp = client.get("/api/chatbot/company/policy-list")
    assert resp.status_code == 200
    _assert_no_raw_exception(resp.json())
    data = resp.json()

    assert len(data["candidates"]) == 2
    for c in data["candidates"]:
        if c["policy_subtype"] == "women_company":
            assert c["valid_count"] == 1
            assert c["expired_count"] == 0
        elif c["policy_subtype"] == "social_enterprise":
            assert c["valid_count"] == 0
            assert c["expired_count"] == 1

def test_phase4_license_search_api(client, temp_chatbot_db):
    """기존 API에 policy_subtypes 연동 + 금지어 검사"""
    conn = sqlite3.connect(temp_chatbot_db)
    conn.execute("INSERT INTO company_license (company_internal_id, license_name) VALUES (1, '정보통신공사업')")
    conn.commit()
    conn.close()

    import_policy_company.run_import(dry_run=False, use_mock=True)

    resp = client.get("/api/chatbot/company/license-search?license_name=정보통신")
    assert resp.status_code == 200
    _assert_no_forbidden_words(resp.text)
    _assert_no_raw_exception(resp.json())
    data = resp.json()
    assert len(data["candidates"]) == 1
    c = data["candidates"][0]

    assert "local_procurement_company" in c["candidate_types"]
    assert "policy_company" in c["candidate_types"]
    assert "women_company" in c["policy_subtypes"]

    json_str = resp.text
    assert "수의계약 가능" not in json_str
    assert "계약 가능" not in json_str
    assert "우선구매 가능" not in json_str

# ── Validation 테스트 ──

def test_phase4_api_validation(client, temp_chatbot_db):
    """422 Validation: invalid status_filter, validity_filter, policy_subtype"""
    resp = client.get("/api/chatbot/company/policy-search?status_filter=invalid_value")
    assert resp.status_code == 422

    resp2 = client.get("/api/chatbot/company/policy-search?validity_filter=invalid_value")
    assert resp2.status_code == 422

    resp3 = client.get("/api/chatbot/company/policy-search?policy_subtype=invalid_value")
    assert resp3.status_code == 422

def test_phase4_limit_regression(client, temp_chatbot_db):
    """limit > 50 요청은 422로 거부"""
    resp = client.get("/api/chatbot/company/license-list?limit=51")
    assert resp.status_code == 422

    resp2 = client.get("/api/chatbot/company/license-list?limit=5000")
    assert resp2.status_code == 422

    # limit=50은 정상
    resp3 = client.get("/api/chatbot/company/license-list?limit=50")
    assert resp3.status_code == 200

def test_phase4_validity_filter_all_no_expired_in_subtypes(client, temp_chatbot_db):
    """validity_filter=all이어도 expired가 policy_subtypes에 들어가지 않는다"""
    import_policy_company.run_import(dry_run=False, use_mock=True)

    resp = client.get("/api/chatbot/company/policy-search?validity_filter=all")
    assert resp.status_code == 200
    data = resp.json()
    for c in data["candidates"]:
        # policy_subtypes에는 valid만
        for st in c.get("policy_subtypes", []):
            summary = c.get("policy_validity_summary", {})
            if st in summary:
                assert summary[st] == "valid", f"Non-valid subtype in policy_subtypes: {st}={summary[st]}"

# ── raw 스키마 테스트 ──

def test_phase4_raw_table_hash_only(temp_chatbot_db):
    """raw 테이블에 원문 사업자번호/인증번호 컬럼이 없고 _hash 컬럼만 존재"""
    conn = sqlite3.connect(temp_chatbot_db)
    cursor = conn.execute("PRAGMA table_info(raw_policy_company_import)")
    columns = {row[1] for row in cursor.fetchall()}
    conn.close()

    assert "raw_business_no_hash" in columns
    assert "raw_certification_no_hash" in columns
    # 원문 컬럼은 존재하면 안 됨
    assert "raw_business_no" not in columns
    assert "raw_certification_no" not in columns

# ── debug endpoint 없음 테스트 ──

def test_phase4_no_debug_db_status(client):
    """더미 /api/debug/db-status endpoint가 존재하지 않는다"""
    resp = client.get("/api/debug/db-status")
    assert resp.status_code == 404

# ── 보안 테스트 ──

def test_phase4_no_hardcoded_service_key():
    """import_policy_company.py 소스코드에 serviceKey가 하드코딩되어 있지 않다"""
    import inspect
    source = inspect.getsource(import_policy_company)
    # 기존 노출된 키 패턴
    assert "c551b235" not in source, "폐기 대상 하드코딩 키가 소스에 남아 있음"
    # 일반적인 하드코딩 패턴 (64자 hex)
    import re
    hex_keys = re.findall(r"SMPP_SERVICE_KEY\s*=\s*['\"][0-9a-fA-F]{32,}['\"]", source)
    assert len(hex_keys) == 0, f"하드코딩된 serviceKey 발견: {hex_keys}"

def test_phase4_missing_env_key_safe_failure(monkeypatch):
    """환경변수에 SMPP key가 없을 때 fetch_smpp_certs가 빈 리스트를 반환한다"""
    monkeypatch.delenv("SMPP_CERT_INFO_SERVICE_KEY", raising=False)
    monkeypatch.delenv("SMPP_SERVICE_KEY", raising=False)
    # 모듈 레벨 변수를 강제로 None으로
    monkeypatch.setattr(import_policy_company, "SMPP_SERVICE_KEY", None)

    result = import_policy_company.fetch_smpp_certs("1234567890", use_mock=False)
    assert result == [], f"Key 없을 때 빈 결과가 아님: {result}"

def test_phase4_mock_mode_without_env_key(temp_chatbot_db, monkeypatch):
    """mock 모드에서는 환경변수 없이도 정상 동작한다"""
    monkeypatch.delenv("SMPP_CERT_INFO_SERVICE_KEY", raising=False)
    monkeypatch.delenv("SMPP_SERVICE_KEY", raising=False)
    monkeypatch.setattr(import_policy_company, "SMPP_SERVICE_KEY", None)

    # mock 모드는 API를 호출하지 않으므로 key 없이도 성공해야 함
    import_policy_company.run_import(dry_run=False, use_mock=True)

    conn = sqlite3.connect(temp_chatbot_db)
    count = conn.execute("SELECT COUNT(*) FROM policy_company_certification").fetchone()[0]
    conn.close()
    assert count > 0, "Mock 모드에서 인증서가 적재되지 않음"

def test_phase4_service_key_not_in_log_or_response(temp_chatbot_db, monkeypatch, caplog):
    """serviceKey 문자열이 로그에 노출되지 않는다"""
    fake_key = "FAKE_TEST_" + "KEY_DO_NOT_LEAK_12345"
    monkeypatch.setattr(import_policy_company, "SMPP_SERVICE_KEY", fake_key)

    import_policy_company.run_import(dry_run=True, use_mock=False)

    for record in caplog.records:
        assert fake_key not in record.getMessage(), f"serviceKey가 로그에 노출됨: {record.getMessage()}"
