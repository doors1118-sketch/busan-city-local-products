import sqlite3

db_path = 'staging_chatbot_company.db'
conn = sqlite3.connect(db_path)

tables = [
    'company_master', 'company_identity', 'company_license', 'company_product', 
    'company_manufacturer_status', 'policy_company_certification', 'mas_product', 
    'mas_contract', 'mas_price_condition', 'ref_sme_competition_product', 
    'company_procurement_attribute', 'product_general_certification', 
    'procurement_label_map', 'source_manifest', 'etl_job_log'
]

print("=== Staging DB Row Counts ===")
for t in tables:
    try:
        count = conn.execute(f"SELECT COUNT(1) FROM {t}").fetchone()[0]
        print(f"{t}: {count}")
    except sqlite3.OperationalError as e:
        print(f"{t}: ERROR - {e}")
        
try:
    cols = conn.execute("PRAGMA table_info(chatbot_company_candidate_view)").fetchall()
    col_names = [c[1] for c in cols]
    print("\nchatbot_company_candidate_view columns:", len(col_names))
    if 'procurement_attributes_raw' in col_names and 'general_certifications_raw' in col_names:
        print("Required raw columns are present.")
    else:
        print("WARNING: Missing required raw columns!")
        print("Current columns:", col_names)
except Exception as e:
    print("chatbot_company_candidate_view ERROR:", e)

conn.close()
