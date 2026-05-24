import sqlite3

db = 'staging_chatbot_company.db'
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row

print("=" * 80)
print("source_manifest dump")
print("=" * 80)
rows = conn.execute("SELECT * FROM source_manifest ORDER BY source_name").fetchall()
for r in rows:
    d = dict(r)
    print(f"  {d['source_name']}: status={d['status']}, rows={d['row_count']}, refreshed={d['source_refreshed_at']}, error={d.get('error_message','')}")

print()
print("=" * 80)
print("etl_job_log dump")
print("=" * 80)
rows = conn.execute("SELECT * FROM etl_job_log ORDER BY job_name").fetchall()
for r in rows:
    d = dict(r)
    print(f"  [{d['job_name']}] source={d['source_name']}, status={d['status']}, input={d['input_row_count']}, inserted={d['inserted_count']}, msg={d.get('error_message','')}")

print()
print("=" * 80)
print("shopping_mall_product summary")
print("=" * 80)
total = conn.execute("SELECT COUNT(*) FROM shopping_mall_product").fetchone()[0]
active = conn.execute("SELECT COUNT(*) FROM shopping_mall_product WHERE contract_status='active'").fetchone()[0]
test_rows = conn.execute("SELECT COUNT(*) FROM shopping_mall_product WHERE source_name='test' OR product_name='Test'").fetchone()[0]
types = conn.execute("SELECT shopping_mall_contract_type, COUNT(*) as cnt FROM shopping_mall_product GROUP BY shopping_mall_contract_type ORDER BY cnt DESC").fetchall()

print(f"  Total: {total}")
print(f"  Active: {active}")
print(f"  Test rows: {test_rows}")
print(f"  Types:")
for t in types:
    print(f"    {t['shopping_mall_contract_type']}: {t['cnt']}")

print()
print("=" * 80)
print("source_manifest: shopping_mall_excel_bootstrap")
print("=" * 80)
r = conn.execute("SELECT * FROM source_manifest WHERE source_name='shopping_mall_excel_bootstrap'").fetchone()
if r:
    for k in r.keys():
        print(f"  {k}: {r[k]}")
else:
    print("  NOT FOUND!")

conn.close()
