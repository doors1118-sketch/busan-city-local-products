import sqlite3
c = sqlite3.connect('staging_chatbot_company.db')
r = c.execute("SELECT sql FROM sqlite_master WHERE name='shopping_mall_product'").fetchone()
print(r[0] if r else 'TABLE NOT FOUND')
print('---')
print('Total rows:', c.execute('SELECT COUNT(*) FROM shopping_mall_product').fetchone()[0])
print('Active rows:', c.execute("SELECT COUNT(*) FROM shopping_mall_product WHERE contract_status='active'").fetchone()[0])
print('Types:', c.execute('SELECT shopping_mall_contract_type, COUNT(*) FROM shopping_mall_product GROUP BY shopping_mall_contract_type').fetchall())

# Check view columns
vr = c.execute("SELECT sql FROM sqlite_master WHERE name='chatbot_company_candidate_view'").fetchone()
if vr:
    print('\n--- VIEW SQL ---')
    print(vr[0][:2000])
c.close()
