import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

excel_path = r'C:\Users\COMTREE\Desktop\연습\남구물품 계약 상세내역.xlsx'

df_raw = pd.read_excel(excel_path, engine='openpyxl')
header_idx = df_raw[df_raw.apply(lambda row: row.astype(str).str.contains('계약번호', na=False).any(), axis=1)].index

if len(header_idx) > 0:
    header_row = header_idx[0]
    df_excel = pd.read_excel(excel_path, engine='openpyxl', header=header_row+1)
else:
    df_excel = pd.read_excel(excel_path, engine='openpyxl')

df_excel.columns = df_excel.columns.str.replace('\n', '').str.strip()

print("한국형 저상압축진개차량 (R26TA01399658) 엑셀 내역:")
cols = [c for c in ['계약번호', '계약명', '품명', '계약일자', '계약금액'] if c in df_excel.columns]
filtered = df_excel[df_excel['계약번호'].astype(str).str.contains('R26TA01399658', na=False)][cols]
print(filtered)
