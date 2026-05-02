import sqlite3
conn = sqlite3.connect('staging_chatbot_company.db')

print("=== Unmatched labels from MAS ===")
rows = conn.execute("""
    SELECT raw_product_name, COUNT(*) 
    FROM certified_product_unmatched 
    WHERE source_name='mas_excel_bootstrap' 
    GROUP BY raw_product_name 
    ORDER BY COUNT(*) DESC
""").fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]}")

print(f"\nTotal unmatched: {sum(r[1] for r in rows)}")

print("\n=== Current certified_product certification_types ===")
rows2 = conn.execute("""
    SELECT certification_type, COUNT(*) 
    FROM certified_product 
    GROUP BY certification_type 
    ORDER BY COUNT(*) DESC
""").fetchall()
for r in rows2:
    print(f"  {r[0]}: {r[1]}")

print(f"\n=== manual_review check ===")
mr = conn.execute("SELECT COUNT(*) FROM certified_product WHERE certification_type='manual_review'").fetchone()[0]
print(f"  manual_review in certified_product: {mr}")

conn.close()
