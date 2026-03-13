import sqlite3
import pandas as pd
conn = sqlite3.connect('procurement_contracts.db')
df = pd.read_sql("SELECT untyCntrctNo, corpList FROM cnstwk_cntrct WHERE cntrctDate = '2026-03-03' LIMIT 5", conn)
for i, row in df.iterrows():
    val = row['corpList']
    if pd.notna(val) and val != 'nan':
        print(f"Contract {row['untyCntrctNo']}")
        print(f"Type: {type(val)} | Length: {len(val)}")
        print(f"Content_prefix: {repr(val[:150])}")
        print("-" * 30)
conn.close()
