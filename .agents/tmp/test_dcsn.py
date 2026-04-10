import sqlite3, pandas as pd
conn = sqlite3.connect('procurement_contracts.db')
name_part = '서구 의료관광특구 대형병원 일원 하수관로 신설공사'
df = pd.read_sql(f"SELECT cnstwkNm, dcsnCntrctNo, cntrctDate FROM cnstwk_cntrct WHERE cnstwkNm LIKE '%{name_part}%'", conn)
print(df.to_string())
conn.close()
