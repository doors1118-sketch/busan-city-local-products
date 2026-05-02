import os
import sys
import sqlite3
from fastapi.testclient import TestClient

# Must import from api_server
try:
    os.environ["CHATBOT_DB"] = "staging_chatbot_company.db"
    from api_server import app
except ImportError:
    print("Cannot import api_server. app not found.")
    sys.exit(1)

client = TestClient(app)

DB_FILE = "staging_chatbot_company.db"

def get_random_sample(query):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(query)
    res = cur.fetchone()
    conn.close()
    return res[0] if res else None

import urllib.parse

def test_endpoints():
    print("Starting API Smoke Tests on Staging DB...")
    
    # 1. License Search
    sample_license = get_random_sample("SELECT license_name FROM company_license LIMIT 1 OFFSET 100")
    if not sample_license:
        print("FAIL: No license found in DB to test.")
        sys.exit(1)
        
    encoded_license = urllib.parse.quote(sample_license)
    print(f"Testing /api/chatbot/company/license-search?license_name={sample_license}")
    resp = client.get(f"/api/chatbot/company/license-search?license_name={encoded_license}")
    if resp.status_code != 200 or not resp.json().get('candidates'):
        print(f"FAIL: license-search failed or empty for {sample_license}")
        print(resp.json() if resp.status_code == 200 else resp.text)
        sys.exit(1)
    print("PASS: license-search returned valid results.")
        
    # 2. Product Search
    sample_product = get_random_sample("SELECT product_name FROM company_product LIMIT 1 OFFSET 100")
    if not sample_product:
        print("FAIL: No product found in DB to test.")
        sys.exit(1)
        
    encoded_product = urllib.parse.quote(sample_product)
    print(f"Testing /api/chatbot/company/product-search?product_name={sample_product}")
    resp = client.get(f"/api/chatbot/company/product-search?product_name={encoded_product}")
    if resp.status_code != 200 or not resp.json().get('candidates'):
        print(f"FAIL: product-search failed or empty for {sample_product}")
        print(resp.json() if resp.status_code == 200 else resp.text)
        sys.exit(1)
    print("PASS: product-search returned valid results.")
        
    # 3. Policy Search
    sample_policy = get_random_sample("SELECT policy_subtype FROM policy_company_certification LIMIT 1 OFFSET 10")
    if not sample_policy:
        print("FAIL: No policy found in DB to test.")
        sys.exit(1)
        
    print(f"Testing /api/chatbot/company/policy-search?policy_subtype={sample_policy}")
    resp = client.get(f"/api/chatbot/company/policy-search?policy_subtype={sample_policy}")
    if resp.status_code != 200 or not resp.json().get('candidates'):
        print(f"FAIL: policy-search failed or empty for {sample_policy}")
        sys.exit(1)
    print("PASS: policy-search returned valid results.")

    # 4. Manufacturers Search
    print(f"Testing /api/chatbot/company/manufacturers?is_manufacturer=true")
    resp = client.get(f"/api/chatbot/company/manufacturers?is_manufacturer=true")
    if resp.status_code != 200 or not resp.json().get('candidates'):
        print(f"FAIL: manufacturers failed or empty")
        sys.exit(1)
    print("PASS: manufacturers returned valid results.")
    
    # 5. SME Competition Product Flag check
    print("Testing sme_competition_product flag structure...")
    resp = client.get("/api/chatbot/company/product-search?product_name=" + urllib.parse.quote("컴퓨터서버"))
    if resp.status_code == 200 and resp.json().get('candidates'):
        cand = resp.json()['candidates'][0]
        if 'sme_competition_product' not in cand:
            print("FAIL: sme_competition_product flag missing in response")
            sys.exit(1)
        print("PASS: sme_competition_product flag is present.")
    
    print("\nAll Smoke Tests Passed!")

if __name__ == "__main__":
    test_endpoints()
