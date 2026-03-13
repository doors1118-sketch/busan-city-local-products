import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

# 마지막으로 추가한 291개는 기존 4364개 이후이므로, rowid 기준으로 추출
conn = sqlite3.connect('busan_agencies_master.db')
df = pd.read_sql("""
    SELECT dminsttCd as 기관코드, dminsttNm as 기관명, 
           cate_lrg as 대분류, cate_mid as 중분류, cate_sml as 소분류
    FROM agency_master 
    ORDER BY rowid DESC 
    LIMIT 291
""", conn)
conn.close()

output_path = r'C:\Users\COMTREE\Desktop\연습\추가기관_291개.xlsx'
df.to_excel(output_path, index=False)
print(f"✅ 저장 완료: {output_path} ({len(df)}건)")
