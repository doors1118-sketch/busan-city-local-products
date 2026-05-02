import pytest
from fastapi.testclient import TestClient
from api_server import app
import sqlite3
import os
import json
import import_mas_product
import migrate_chatbot_db

client = TestClient(app)

DB_FILE = migrate_chatbot_db.DB_FILE

@pytest.fixture(scope="module", autouse=True)
def setup_test_db():
    # 1. 스키마 초기화
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    migrate_chatbot_db.migrate()
    
    # 2. 기초 데이터 (Company Master & Identity)
    conn = sqlite3.connect(DB_FILE)
    conn.execute('''
        INSERT INTO company_master (company_internal_id, company_name, is_busan_company)
        VALUES (1, '테스트업체', 1)
    ''')
    conn.execute('''
        INSERT INTO company_identity (company_internal_id, canonical_business_no, company_id)
        VALUES (1, '1234567890', 'test_company_id_1')
    ''')
    conn.execute('''
        INSERT INTO company_master (company_internal_id, company_name, is_busan_company)
        VALUES (2, '타지역업체', 0)
    ''')
    conn.execute('''
        INSERT INTO company_identity (company_internal_id, canonical_business_no, company_id)
        VALUES (2, '9999999999', 'test_company_id_2')
    ''')
    
    # company/product-search 용
    conn.execute('''
        INSERT INTO company_product (company_internal_id, product_name, product_name_normalized)
        VALUES (1, '데스크톱컴퓨터', '데스크톱컴퓨터')
    ''')
    
    # 3. 다른 Phase 속성 병합 테스트용 데이터 추가
    # Policy
    conn.execute('''
        INSERT INTO policy_company_certification (company_internal_id, policy_type, policy_subtype, validity_status, source_name)
        VALUES (1, '여성기업', 'women_company', 'valid', 'smba')
    ''')
    # Certified Product
    conn.execute('''
        INSERT INTO certified_product (company_internal_id, certification_type, product_name, validity_status, source_name)
        VALUES (1, 'performance_certification', '고성능 CCTV', 'valid', 'smpp')
    ''')
    
    conn.commit()
    conn.close()

    # 4. Import 실행 (통합 경로 정상 테스트)
    import_mas_product.run_import(use_mock=True)
    
    yield
    try:
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)
    except Exception:
        pass

def test_phase6c_integration_path():
    # migration -> import 경로가 정상인지 확인
    conn = sqlite3.connect(DB_FILE)
    # raw 적재 확인
    raw_count = conn.execute("SELECT COUNT(*) FROM raw_mas_product_import").fetchone()[0]
    assert raw_count == 3
    
    # mas_product 적재 확인 (2건 매칭, 1건 실패)
    mas_count = conn.execute("SELECT COUNT(*) FROM mas_product").fetchone()[0]
    assert mas_count == 2
    
    # unmatched 확인
    unmatched = conn.execute("SELECT COUNT(*) FROM mas_product_unmatched").fetchone()[0]
    assert unmatched == 1
    
    # etl_job_log 및 source_manifest 확인
    assert conn.execute("SELECT COUNT(*) FROM source_manifest WHERE source_name='g2b_mas_mock'").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM etl_job_log WHERE job_name='import_mas_product'").fetchone()[0] == 1
    
    conn.close()

def test_phase6c_raw_hash_only():
    conn = sqlite3.connect(DB_FILE)
    row = conn.execute("SELECT raw_business_no_hash, raw_contract_no_hash FROM raw_mas_product_import LIMIT 1").fetchone()
    # 사업자등록번호 원문(1234567890)이나 계약번호 원문이 들어가선 안됨
    assert row[0] != "1234567890"
    assert row[1] != "MAS-2023-01"
    assert len(row[0]) == 64 # SHA-256 length
    assert len(row[1]) == 64
    
    # 컬럼 존재 여부 (원문 필드가 없어야 함)
    cols = [col[1] for col in conn.execute("PRAGMA table_info(raw_mas_product_import)").fetchall()]
    assert "raw_business_no" not in cols
    assert "raw_contract_no" not in cols
    
    conn.close()

def test_phase6c_contract_no_not_stored():
    conn = sqlite3.connect(DB_FILE)
    # mas_product, mas_contract에도 원문이 없어야 함
    prod_cols = [col[1] for col in conn.execute("PRAGMA table_info(mas_product)").fetchall()]
    assert "contract_no" not in prod_cols
    assert "contract_no_hash" in prod_cols
    
    cont_cols = [col[1] for col in conn.execute("PRAGMA table_info(mas_contract)").fetchall()]
    assert "contract_no" not in cont_cols
    assert "contract_no_hash" in cont_cols
    conn.close()

def test_phase6c_mas_search_api():
    resp = client.get("/api/chatbot/mas/search?product_name=데스크")
    assert resp.status_code == 200
    data = resp.json()
    assert data["company_search_status"] == "success"
    assert len(data["candidates"]) == 1
    
    c = data["candidates"][0]
    # active 계약만 shopping_mall_supplier 승격
    assert "shopping_mall_supplier" in c["candidate_types"]
    assert "mas_registered" in c["shopping_mall_flags"]
    assert c["mas_product_summary"][0]["product_name"] == "데스크톱컴퓨터"
    assert c["mas_product_summary"][0]["contract_status"] == "active"
    assert "route_codes" not in c
    assert "check_codes" not in c

def test_phase6c_expired_exclusion_in_default():
    # CCTV는 expired 상태임
    resp = client.get("/api/chatbot/mas/search?product_name=CCTV")
    # 기본은 active_only 이므로 검색결과에 나와서는 안됨 (company 자체가 제외될수도 있고, summary에 없을수도 있음. 여기선 product_name 필터가 active에 안걸림)
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["candidates"]) == 0
    
    # include_unknown으로 검색하면 나와야 함
    resp2 = client.get("/api/chatbot/mas/search?product_name=CCTV&contract_status_filter=all")
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert len(data2["candidates"]) == 1
    c = data2["candidates"][0]
    
    # expired 계약은 기본 flags/types에 영향을 주지 않음 (뷰에서 막혀있음) -> Wait, 데스크톱이 active니까 flag는 있음.
    # 하지만 summary 안에는 expired CCTV가 보여야 함
    summaries = c["mas_product_summary"]
    has_expired = any(s["product_name"] == "영상감시장치" and s["contract_status"] == "expired" for s in summaries)
    assert has_expired

def test_phase6c_filter_validation():
    # invalid filter
    resp = client.get("/api/chatbot/mas/search?contract_status_filter=invalid_value")
    assert resp.status_code == 422
    
    # limit=51
    resp2 = client.get("/api/chatbot/mas/search?limit=51")
    assert resp2.status_code == 422

def test_phase6c_existing_api_merge():
    # 기존 product-search에서도 mas_product_summary가 병합되어야 함
    resp = client.get("/api/chatbot/company/product-search?product_name=데스크")
    assert resp.status_code == 200
    data = resp.json()
    c = data["candidates"][0]
    
    assert "mas_product_summary" in c
    assert c["mas_product_summary"][0]["product_name"] == "데스크톱컴퓨터"
    assert "shopping_mall_supplier" in c["candidate_types"]
    
    # policy_subtypes + certified_product_types + shopping_mall_flags 동시 병합 확인
    assert "women_company" in c["policy_subtypes"]
    assert "performance_certification" in c["certified_product_types"]
    assert "mas_registered" in c["shopping_mall_flags"]
    assert "priority_purchase_product" in c["candidate_types"]
    assert "policy_company" in c["candidate_types"]

def test_phase6c_forbidden_words():
    # "종합쇼핑몰 구매 가능", "MAS 구매 가능", "계약 가능" 문구 미포함
    resp = client.get("/api/chatbot/mas/search?company_keyword=테스트")
    assert resp.status_code == 200
    text = resp.text
    assert "구매 가능" not in text
    assert "MAS 구매 가능" not in text
    assert "계약 가능" not in text

def test_phase6c_mas_list_api():
    resp = client.get("/api/chatbot/mas/list")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["candidates"]) > 0
    c = data["candidates"][0]
    assert "active_contract_count" in c
    assert "expired_contract_count" in c
    assert "supplier_count" in c

def test_phase6c_mas_supplier_search():
    resp = client.get("/api/chatbot/mas/supplier-search?company_keyword=테스트")
    assert resp.status_code == 200
    assert len(resp.json()["candidates"]) == 1

def test_phase6c_idempotency():
    # 현재 카운트 기록
    conn = sqlite3.connect(DB_FILE)
    mas_prod_count = conn.execute("SELECT COUNT(*) FROM mas_product").fetchone()[0]
    mas_cont_count = conn.execute("SELECT COUNT(*) FROM mas_contract").fetchone()[0]
    mas_price_count = conn.execute("SELECT COUNT(*) FROM mas_price_condition").fetchone()[0]
    conn.close()

    # 2회차 실행
    import_mas_product.run_import(use_mock=True)

    # 멱등성 검증 (카운트 불변 확인)
    conn = sqlite3.connect(DB_FILE)
    new_mas_prod_count = conn.execute("SELECT COUNT(*) FROM mas_product").fetchone()[0]
    new_mas_cont_count = conn.execute("SELECT COUNT(*) FROM mas_contract").fetchone()[0]
    new_mas_price_count = conn.execute("SELECT COUNT(*) FROM mas_price_condition").fetchone()[0]
    
    assert mas_prod_count == new_mas_prod_count
    assert mas_cont_count == new_mas_cont_count
    assert mas_price_count == new_mas_price_count

    # Orphan row 검증
    orphan_count = conn.execute('''
        SELECT COUNT(*) FROM mas_price_condition pc
        LEFT JOIN mas_product mp ON pc.mas_product_id = mp.mas_product_id
        WHERE mp.mas_product_id IS NULL
    ''').fetchone()[0]
    assert orphan_count == 0
    conn.close()
