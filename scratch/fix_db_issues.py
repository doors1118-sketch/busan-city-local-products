import sqlite3

db = 'staging_chatbot_company.db'
conn = sqlite3.connect(db)

# Issue 2: Remove test rows
deleted = conn.execute("DELETE FROM shopping_mall_product WHERE source_name='test' OR product_name='Test'").rowcount
print(f"Deleted test rows from shopping_mall_product: {deleted}")

# Issue 1: Insert shopping_mall_excel_bootstrap into source_manifest
# Count actual shopping_mall_product rows from mas_excel_bootstrap source
sm_count = conn.execute("SELECT COUNT(*) FROM shopping_mall_product WHERE source_name='mas_excel_bootstrap'").fetchone()[0]
sm_active = conn.execute("SELECT COUNT(*) FROM shopping_mall_product WHERE source_name='mas_excel_bootstrap' AND contract_status='active'").fetchone()[0]
mas_active = conn.execute("SELECT COUNT(*) FROM mas_product WHERE source_name='mas_excel_bootstrap' AND contract_status='active'").fetchone()[0]

# Insert source_manifest for shopping_mall_excel_bootstrap
conn.execute("""
    INSERT INTO source_manifest (source_name, source_type, source_refreshed_at, row_count, status, error_message)
    VALUES ('shopping_mall_excel_bootstrap', 'excel_bootstrap', datetime('now'), ?, 'success', ?)
    ON CONFLICT(source_name) DO UPDATE SET 
        row_count=excluded.row_count, 
        source_refreshed_at=excluded.source_refreshed_at, 
        status=excluded.status, 
        error_message=excluded.error_message
""", (sm_count, f"sm_active={sm_active}, mas_active={mas_active}"))

# Insert etl_job_log for shopping_mall_excel_bootstrap
conn.execute("""
    INSERT INTO etl_job_log (job_name, source_name, started_at, finished_at, status, input_row_count, inserted_count, skipped_count, error_count, error_message)
    VALUES ('bootstrap_shopping_mall_excel', 'shopping_mall_excel_bootstrap', datetime('now'), datetime('now'), 'success', ?, ?, 0, 0, ?)
""", (sm_count, sm_count, f"sm_active={sm_active}, mas_active={mas_active}"))

conn.commit()

# Verify
print(f"\n=== Verification ===")
sm_total = conn.execute('SELECT COUNT(*) FROM shopping_mall_product').fetchone()[0]
sm_act = conn.execute("SELECT COUNT(*) FROM shopping_mall_product WHERE contract_status='active'").fetchone()[0]
print(f"shopping_mall_product total: {sm_total}")
print(f"shopping_mall_product active: {sm_act}")

r = conn.execute("SELECT * FROM source_manifest WHERE source_name='shopping_mall_excel_bootstrap'").fetchone()
print(f"\nsource_manifest row: {r}")

r2 = conn.execute("SELECT * FROM etl_job_log WHERE source_name='shopping_mall_excel_bootstrap'").fetchone()
print(f"etl_job_log row: {r2}")

test_rows = conn.execute("SELECT COUNT(*) FROM shopping_mall_product WHERE source_name='test' OR product_name='Test'").fetchone()[0]
print(f"\nRemaining test rows: {test_rows}")

conn.close()
