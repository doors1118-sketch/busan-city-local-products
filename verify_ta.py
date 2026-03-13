import sqlite3
import pandas as pd
conn = sqlite3.connect('procurement_contracts.db')
df = pd.read_sql("SELECT untyCntrctNo, cntrctRefNo, cntrctNm FROM thng_cntrct WHERE cntrctRefNo LIKE '%TA%' LIMIT 5", conn)
print("TA items in thng_cntrct:", len(df))
print(df)
conn.close()
