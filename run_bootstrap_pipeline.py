import os
import sys
import subprocess
import sqlite3

os.environ["CHATBOT_DB"] = "staging_chatbot_company.db"
DB_FILE = "staging_chatbot_company.db"

def init_db():
    print(f"==========================================")
    print(f"Initializing Clean Database...")
    print(f"==========================================")
    if not os.environ.get("COMPANY_ID_HMAC_SECRET"):
        print("ERROR: COMPANY_ID_HMAC_SECRET is missing. Cannot proceed.")
        sys.exit(1)
        
    if os.path.exists(DB_FILE):
        print(f"Removing old {DB_FILE} to ensure clean state.")
        os.remove(DB_FILE)
    
    run_script("migrate_chatbot_db.py")

def run_script(script_name, *args):
    print(f"\n==========================================")
    print(f"Running {script_name}...")
    print(f"==========================================")
    cmd = [sys.executable, script_name] + list(args)
    res = subprocess.run(cmd)
    if res.returncode != 0:
        print(f"Error: {script_name} failed.")
        sys.exit(1)

def print_summary():
    if not os.path.exists(DB_FILE):
        return
        
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    
    print("\n==========================================")
    print("Database Bootstrap Summary")
    print("==========================================")
    
    tables = [
        "company_master",
        "company_identity",
        "company_license",
        "company_product",
        "company_manufacturer_status",
        "policy_company_certification",
        "certified_product",
        "mas_product",
        "mas_contract",
        "mas_price_condition",
        "ref_sme_competition_product",
        "company_procurement_attribute",
        "product_general_certification",
        "procurement_label_mapping_review",
        "procurement_label_map",
        "etl_job_log"
    ]
    
    for t in tables:
        try:
            cnt = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"{t}: {cnt:,} rows")
        except:
            print(f"{t}: Error reading table")
            
    print("\n[Source Manifest]")
    try:
        rows = cur.execute("SELECT source_name, row_count, status FROM source_manifest").fetchall()
        for r in rows:
            print(f" - {r[0]}: {r[1]} rows ({r[2]})")
    except:
        pass
        
    conn.close()

if __name__ == "__main__":
    # 0. Secret Check and Clean DB
    init_db()
    
    # 1. Bootstrap Master (Master, Identity, License, Product)
    run_script("bootstrap_master_data.py")
    
    # 2. Bootstrap from Excel (Policy, Manufacturer)
    run_script("bootstrap_from_excel.py")
    
    # 3. Bootstrap NTS (Simulated for 500 limits, using validity mock if key missing to not break here)
    # nts is done via nts_business_status_client.py, we can write a short runner or rely on actual data later.
    
    print_summary()
    print("\nBootstrap pipeline completed. PRODUCTION_DEPLOYMENT=HOLD")
