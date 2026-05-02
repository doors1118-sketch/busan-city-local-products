import sqlite3
conn = sqlite3.connect('staging_chatbot_company.db')

# 속성이 있는 업체의 뷰 데이터 확인
print("=== View data for companies WITH procurement attributes ===")
rows = conn.execute("""
    SELECT company_id, company_name, procurement_attributes_raw, general_certifications_raw
    FROM chatbot_company_candidate_view
    WHERE procurement_attributes_raw IS NOT NULL
    LIMIT 5
""").fetchall()
for r in rows:
    print(f"  {r[1]}: attrs=[{r[2]}], certs=[{r[3]}]")

print(f"\n=== Companies with non-null procurement_attributes_raw ===")
cnt = conn.execute("SELECT COUNT(*) FROM chatbot_company_candidate_view WHERE procurement_attributes_raw IS NOT NULL").fetchone()[0]
print(f"  Count: {cnt}")

print(f"\n=== Companies with non-null general_certifications_raw ===")
cnt2 = conn.execute("SELECT COUNT(*) FROM chatbot_company_candidate_view WHERE general_certifications_raw IS NOT NULL").fetchone()[0]
print(f"  Count: {cnt2}")

conn.close()
