import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

excel_path = r'C:\Users\COMTREE\Desktop\연습\수영구 물품.xlsx'

# 엑셀 파일 로드
df_raw = pd.read_excel(excel_path, engine='openpyxl')
header_idx = df_raw[df_raw.apply(lambda row: row.astype(str).str.contains('계약번호', na=False).any(), axis=1)].index

if len(header_idx) > 0:
    header_row = header_idx[0]
    df_excel = pd.read_excel(excel_path, engine='openpyxl', header=header_row+1)
else:
    df_excel = pd.read_excel(excel_path, engine='openpyxl')
    
df_excel.columns = df_excel.columns.str.replace('\n', '').str.strip()

# '망미 청소년탐구' 가 포함된 행 찾기
target_rows = df_excel[df_excel['계약명'].astype(str).str.contains('망미 청소년탐구', na=False)]

print(f"엑셀에서 '망미 청소년탐구' 검색 결과: {len(target_rows)}건")
for _, row in target_rows.iterrows():
    print(f"계약번호: {row['계약번호']}")
    print(f"계약명: {row['계약명']}")
    print(f"계약금액: {row['계약금액']}")
    print("-" * 30)

# 이 계약의 금액이 594,604,550원 합계에 포함되었는지 확인하기 위해, 
# 계약번호를 키로 해서 금액을 추출해본다.
excel_data_max = {}
for _, row in df_excel.iterrows():
    no = str(row['계약번호']).strip()
    amt = float(str(row.get('계약금액', 0)).replace(',', ''))
    if no not in excel_data_max:
        excel_data_max[no] = amt
    else:
        excel_data_max[no] = max(excel_data_max[no], amt)

print(f"엑셀 고유 계약번호 수: {len(excel_data_max)}")
print(f"엑셀 계약금액 합계 (Max 기준): {sum(excel_data_max.values()):,.0f}원")
