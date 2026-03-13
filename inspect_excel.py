import pandas as pd
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

EXCEL_PATH = r'C:\Users\COMTREE\Desktop\연습\부산광역시 종합쇼핑몰 납품요구 물품 내역{20년 1월이후자료(조회속도향상)}.xlsx'

# 1. 먼저 처음 20줄을 raw 텍스트로 확인
print("🔍 처음 20줄 (raw값):")
df_raw = pd.read_excel(EXCEL_PATH, header=None, nrows=20)
for i, row in df_raw.iterrows():
    vals = [str(v)[:20] for v in row.values if str(v) != 'nan']
    if vals:
        print(f"  행{i:2d}: {vals[:6]}")
    else:
        print(f"  행{i:2d}: (빈줄)")
