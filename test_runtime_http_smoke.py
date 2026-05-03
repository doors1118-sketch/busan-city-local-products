import sqlite3
import requests
import time
import json
import concurrent.futures
import numpy as np
import urllib.parse
import sys

BASE_URL = "http://127.0.0.1:8001/api/chatbot"

FORBIDDEN_WORDS = [
    "사업자등록번호", "canonical_business_no", "raw_business_no",
    "contract_no", "contract_no_hash", "raw_contract_no",
    "internal_join_key", "serviceKey", "api_key", "token",
    "route_codes", "check_codes",
    "구매 가능", "계약 가능", "수의계약 가능",
    "MAS 구매 가능", "종합쇼핑몰 구매 가능", "2단계경쟁 불필요",
    "바로 구매 가능"
]

ALLOWED_SM_FLAGS = {
    "shopping_mall_registered", "mas_registered",
    "third_party_unit_price_registered", "general_unit_price_registered",
    "excellent_procurement_registered"
}
ALLOWED_SM_CONTRACT_TYPES = {
    "mas", "third_party_unit_price", "general_unit_price",
    "excellent_procurement", "unknown"
}

def scan_forbidden_words(text):
    found = []
    # exception logic for contract_possible_auto_promoted
    text_to_scan = text.replace('"contract_possible_auto_promoted":false', '').replace('"contract_possible_auto_promoted": false', '')
    for word in FORBIDDEN_WORDS:
        if word in text_to_scan:
            found.append(word)
    return found

def get_samples():
    conn = sqlite3.connect("staging_chatbot_company.db")
    cur = conn.cursor()
    
    # Sampling
    cur.execute("SELECT product_name FROM company_product WHERE product_name IS NOT NULL AND product_name != '' LIMIT 1 OFFSET 100")
    prod = cur.fetchone()
    product_name = prod[0] if prod else "테이블"
    
    cur.execute("SELECT license_name FROM company_license WHERE license_name IS NOT NULL AND license_name != '' LIMIT 1 OFFSET 100")
    lic = cur.fetchone()
    license_name = lic[0] if lic else "소프트웨어"
    
    cur.execute("SELECT policy_subtype FROM policy_company_certification WHERE policy_subtype IS NOT NULL AND policy_subtype != '' LIMIT 1")
    pol = cur.fetchone()
    policy_subtype = pol[0] if pol else "women_company"
    
    cur.execute("SELECT company_id FROM company_identity LIMIT 1 OFFSET 100")
    comp = cur.fetchone()
    company_id = comp[0] if comp else "TEST_COMP_ID"
    
    conn.close()
    return product_name, license_name, policy_subtype, company_id

def test_endpoint(url, must_be_non_empty=False):
    print(f"\n[TEST] {url}")
    try:
        resp = requests.get(url, timeout=5)
        text = resp.text
        status_code = resp.status_code
        
        if status_code != 200:
            print(f"  ❌ FAIL: HTTP {status_code}")
            return False

        try:
            data = resp.json()
        except json.JSONDecodeError:
            print(f"  ❌ FAIL: Invalid JSON")
            return False

        # Scan forbidden words
        forbidden = scan_forbidden_words(text)
        if forbidden:
            print(f"  ❌ FAIL: Forbidden words found: {forbidden}")
            return False
            
        if "/health" in url:
            db = data.get("db", {})
            if data.get("production_deployment") != "HOLD":
                print("  ❌ FAIL: production_deployment is not HOLD in health")
                return False
            if not db.get("chatbot_db_connected"):
                print("  ❌ FAIL: chatbot_db_connected is not true")
                return False
            if db.get("company_master_count", 0) <= 0 or db.get("company_license_count", 0) <= 0 or db.get("company_product_count", 0) <= 0:
                print("  ❌ FAIL: health db counts are 0")
                return False
            # Phase 6-G: shopping_mall_product_count 확인
            if "shopping_mall_product_count" not in db:
                print("  ❌ FAIL: shopping_mall_product_count missing from health")
                return False
            if "active_shopping_mall_product_count" not in db:
                print("  ❌ FAIL: active_shopping_mall_product_count missing from health")
                return False
            if db.get("shopping_mall_product_count", 0) <= 0:
                print("  ❌ FAIL: shopping_mall_product_count is 0")
                return False
            print("  ✅ health checks passed")
            return True
            
        if "/version" in url:
            if data.get("production_deployment") != "HOLD":
                print("  ❌ FAIL: production_deployment is not HOLD in version")
                return False
            features = data.get("features", [])
            req_features = ["company_search", "policy_company", "certified_product", "mas_product",
                           "shopping_mall_product", "shopping_mall_contract_type"]
            if not all(f in features for f in req_features):
                missing = [f for f in req_features if f not in features]
                print(f"  ❌ FAIL: missing features in version: {missing}")
                return False
            print("  ✅ version checks passed")
            return True

        if data.get("company_search_status") == "failed":
            print(f"  ❌ FAIL: company_search_status is failed")
            return False
            
        candidates = data.get("candidates", [])
        print(f"  ✅ Candidates count: {len(candidates)}")
        
        if must_be_non_empty and len(candidates) == 0:
            print(f"  ❌ FAIL: candidates is empty but must be non-empty")
            return False
            
        # Check attributes
        for c in candidates:
            if "company_id" in c:
                if c.get("display_status") != "후보":
                    print(f"  ❌ FAIL: display_status is not 후보")
                    return False
                if c.get("contract_possible_auto_promoted", None) is True:
                    print(f"  ❌ FAIL: contract_possible_auto_promoted is true")
                    return False
                
                c_types = c.get("candidate_types", [])
                for t in c_types:
                    if t in c.get("procurement_attributes", []):
                        print(f"  ❌ FAIL: procurement_attribute {t} found in candidate_types")
                        return False
                    if t in c.get("general_certifications", []):
                        print(f"  ❌ FAIL: general_certification {t} found in candidate_types")
                        return False
                    
        return True
    except Exception as e:
        print(f"  ❌ FAIL: Exception {e}")
        return False

def test_shopping_mall_endpoints():
    """Phase 6-G 종합쇼핑몰 전용 엔드포인트 검증"""
    print("\n=== Phase 6-G Shopping Mall Endpoint Tests ===")
    all_ok = True
    
    # 1. /shopping-mall/search
    print("\n[TEST] /shopping-mall/search (기본)")
    try:
        resp = requests.get(f"{BASE_URL}/shopping-mall/search", timeout=5)
        if resp.status_code != 200:
            print(f"  ❌ FAIL: HTTP {resp.status_code}")
            all_ok = False
        else:
            data = resp.json()
            candidates = data.get("candidates", [])
            print(f"  ✅ Candidates: {len(candidates)}")
            
            # 금지어 스캔
            forbidden = scan_forbidden_words(resp.text)
            if forbidden:
                print(f"  ❌ FAIL: Forbidden words in search: {forbidden}")
                all_ok = False
            
            # shopping_mall_flags whitelist 확인
            for c in candidates:
                sm_flags = c.get("shopping_mall_flags", [])
                for f in sm_flags:
                    if f not in ALLOWED_SM_FLAGS:
                        print(f"  ❌ FAIL: shopping_mall_flags contains non-whitelisted: {f}")
                        all_ok = False
                
                # shopping_mall_product_summary contract_type whitelist 확인
                for sm in c.get("shopping_mall_product_summary", []):
                    ct = sm.get("shopping_mall_contract_type")
                    if ct and ct not in ALLOWED_SM_CONTRACT_TYPES:
                        print(f"  ❌ FAIL: shopping_mall_contract_type not allowed: {ct}")
                        all_ok = False
                
                # contract_no / contract_no_hash 미노출 확인
                c_text = json.dumps(c, ensure_ascii=False)
                if '"contract_no"' in c_text.replace('"contract_no_hash"', ''):
                    # contract_no_hash도 허용 안됨
                    pass
                if "contract_no_hash" in c_text:
                    print(f"  ❌ FAIL: contract_no_hash exposed in candidate")
                    all_ok = False
            
            # shopping_mall_product_summary non-empty 확인 (전체 중 1건 이상)
            has_sm_summary = any(
                len(c.get("shopping_mall_product_summary", [])) > 0 
                for c in candidates
            )
            if candidates and not has_sm_summary:
                print(f"  ⚠️ WARNING: no candidates have shopping_mall_product_summary")
            
            # shopping_mall_registered flag 존재 확인
            has_sm_registered = any(
                "shopping_mall_registered" in c.get("shopping_mall_flags", [])
                for c in candidates
            )
            if candidates and not has_sm_registered:
                print(f"  ⚠️ WARNING: no candidates have shopping_mall_registered flag")
    except Exception as e:
        print(f"  ❌ FAIL: Exception {e}")
        all_ok = False
    
    # 2. /shopping-mall/product-search
    print("\n[TEST] /shopping-mall/product-search")
    try:
        resp = requests.get(f"{BASE_URL}/shopping-mall/product-search?product_name=%EC%BB%B4%ED%93%A8%ED%84%B0", timeout=5)
        if resp.status_code != 200:
            print(f"  ❌ FAIL: HTTP {resp.status_code}")
            all_ok = False
        else:
            data = resp.json()
            print(f"  ✅ Candidates: {len(data.get('candidates', []))}")
            forbidden = scan_forbidden_words(resp.text)
            if forbidden:
                print(f"  ❌ FAIL: Forbidden words: {forbidden}")
                all_ok = False
    except Exception as e:
        print(f"  ❌ FAIL: Exception {e}")
        all_ok = False
    
    # 3. /shopping-mall/supplier-search
    print("\n[TEST] /shopping-mall/supplier-search")
    try:
        resp = requests.get(f"{BASE_URL}/shopping-mall/supplier-search", timeout=5)
        if resp.status_code != 200:
            print(f"  ❌ FAIL: HTTP {resp.status_code}")
            all_ok = False
        else:
            data = resp.json()
            print(f"  ✅ Candidates: {len(data.get('candidates', []))}")
    except Exception as e:
        print(f"  ❌ FAIL: Exception {e}")
        all_ok = False
    
    # 4. /shopping-mall/list
    print("\n[TEST] /shopping-mall/list")
    try:
        resp = requests.get(f"{BASE_URL}/shopping-mall/list", timeout=5)
        if resp.status_code != 200:
            print(f"  ❌ FAIL: HTTP {resp.status_code}")
            all_ok = False
        else:
            data = resp.json()
            candidates = data.get("candidates", [])
            print(f"  ✅ List items: {len(candidates)}")
            
            # contract_type whitelist 확인
            for item in candidates:
                ct = item.get("shopping_mall_contract_type")
                if ct and ct not in ALLOWED_SM_CONTRACT_TYPES:
                    print(f"  ❌ FAIL: list contains non-allowed contract_type: {ct}")
                    all_ok = False
            
            forbidden = scan_forbidden_words(resp.text)
            if forbidden:
                print(f"  ❌ FAIL: Forbidden words in list: {forbidden}")
                all_ok = False
    except Exception as e:
        print(f"  ❌ FAIL: Exception {e}")
        all_ok = False
    
    # 5. contract_type_filter 테스트 (mas 필터)
    print("\n[TEST] /shopping-mall/search?contract_type_filter=mas")
    try:
        resp = requests.get(f"{BASE_URL}/shopping-mall/search?contract_type_filter=mas", timeout=5)
        if resp.status_code != 200:
            print(f"  ❌ FAIL: HTTP {resp.status_code}")
            all_ok = False
        else:
            data = resp.json()
            print(f"  ✅ MAS-filtered candidates: {len(data.get('candidates', []))}")
    except Exception as e:
        print(f"  ❌ FAIL: Exception {e}")
        all_ok = False
    
    # 6. contract_type_filter 테스트 (third_party_unit_price 필터)
    print("\n[TEST] /shopping-mall/search?contract_type_filter=third_party_unit_price")
    try:
        resp = requests.get(f"{BASE_URL}/shopping-mall/search?contract_type_filter=third_party_unit_price", timeout=5)
        if resp.status_code != 200:
            print(f"  ❌ FAIL: HTTP {resp.status_code}")
            all_ok = False
        else:
            data = resp.json()
            print(f"  ✅ Third-party filtered candidates: {len(data.get('candidates', []))}")
    except Exception as e:
        print(f"  ❌ FAIL: Exception {e}")
        all_ok = False
    
    # 7. contract_type_filter 테스트 (excellent_procurement 필터)
    print("\n[TEST] /shopping-mall/search?contract_type_filter=excellent_procurement")
    try:
        resp = requests.get(f"{BASE_URL}/shopping-mall/search?contract_type_filter=excellent_procurement", timeout=5)
        if resp.status_code != 200:
            print(f"  ❌ FAIL: HTTP {resp.status_code}")
            all_ok = False
        else:
            data = resp.json()
            print(f"  ✅ Excellent procurement filtered candidates: {len(data.get('candidates', []))}")
    except Exception as e:
        print(f"  ❌ FAIL: Exception {e}")
        all_ok = False
    
    return all_ok

def test_performance(url, count=10):
    times = []
    errors = 0
    for _ in range(count):
        start = time.time()
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code != 200:
                errors += 1
        except:
            errors += 1
        times.append(time.time() - start)
        time.sleep(0.1)
    
    avg = np.mean(times)
    p95 = np.percentile(times, 95)
    print(f"  [Perf] {url} -> Avg: {avg:.3f}s, p95: {p95:.3f}s, Errors: {errors}")
    return p95 < 3.0 and errors == 0

def fetch_url(url):
    try:
        resp = requests.get(url, timeout=5)
        return resp.status_code
    except Exception as e:
        return str(e)

def test_concurrency(url, concurrency=5):
    print(f"  [Concurrency] Testing 5 concurrent requests to {url}...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(fetch_url, url) for _ in range(concurrency)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
    
    failed = [r for r in results if r != 200]
    if failed:
        print(f"  ❌ FAIL: Concurrent requests had failures/errors: {failed}")
        return False
    else:
        print("  ✅ All concurrent requests returned HTTP 200")
        return True

def main():
    product_name, license_name, policy_subtype, company_id = get_samples()
    print(f"=== Extracted Samples ===")
    print(f"product_name: {product_name}")
    print(f"license_name: {license_name}")
    print(f"policy_subtype: {policy_subtype}")
    print(f"company_id: {company_id}\n")

    endpoints = [
        ("/health", False),
        ("/version", False),
        ("/company/product-list", False),
        ("/company/license-list", False),
        (f"/company/product-search?product_name={urllib.parse.quote(product_name)}", True),
        (f"/company/license-search?license_name={urllib.parse.quote(license_name)}", True),
        (f"/company/policy-search?policy_subtype={urllib.parse.quote(policy_subtype)}", True),
        ("/company/manufacturers", True),
        ("/mas/search", False),
        (f"/mas/product-search?product_name={urllib.parse.quote(product_name)}", False),
        ("/mas/list", False),
        ("/product/certified-search", False),
        ("/product/certified-list", False),
    ]

    all_passed = True
    for path, must_be_non_empty in endpoints:
        url = BASE_URL + path
        if not test_endpoint(url, must_be_non_empty):
            all_passed = False

    print("\n[TEST] /company/detail (Fail-closed Check without NTS Key)")
    detail_url = BASE_URL + f"/company/detail?company_id={company_id}"
    if not test_endpoint(detail_url, False):
        all_passed = False

    # Phase 6-G: shopping-mall endpoints
    if not test_shopping_mall_endpoints():
        all_passed = False

    print("\n=== Performance Tests ===")
    perf_endpoints = [
        f"/company/product-search?product_name={urllib.parse.quote(product_name)}",
        f"/company/license-search?license_name={urllib.parse.quote(license_name)}",
        f"/company/policy-search?policy_subtype={urllib.parse.quote(policy_subtype)}",
        "/mas/search",
        "/shopping-mall/search"
    ]
    for path in perf_endpoints:
        url = BASE_URL + path
        if not test_performance(url):
            all_passed = False
            
    print("\n=== Concurrency Tests ===")
    for path in perf_endpoints:
        url = BASE_URL + path
        if not test_concurrency(url, 5):
            all_passed = False

    if all_passed:
        print("\n🎉 ALL TESTS PASSED.")
        sys.exit(0)
    else:
        print("\n💥 SOME TESTS FAILED.")
        sys.exit(1)

if __name__ == "__main__":
    main()
