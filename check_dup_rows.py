import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')
df = pd.read_sql("SELECT untyCntrctNo, cntrctRefNo, totCntrctAmt, corpList FROM servc_cntrct WHERE cntrctDate >= '2026-01-01'", conn)

# 1. 100% 초과 계약 번호 추출
over_100_unty_nos = set()
for _, row in df.iterrows():
    c_list = str(row['corpList'])
    if c_list and c_list not in ('nan', 'None'):
        corps = c_list.split('[')[1:]
        shares = []
        for c in corps:
            parts = c.split(']')[0].split('^')
            if len(parts) >= 10:
                share_str = str(parts[6]).strip()
                try: shares.append(float(share_str) if share_str else 100.0)
                except: shares.append(100.0)
        
        if sum(shares) > 100.1:
            over_100_unty_nos.add(row['untyCntrctNo'])

print(f"100% 초과 판정된 고유 계약번호 수: {len(over_100_unty_nos)}건")

# 2. 해당 계약번호들이 DB 상에서 여러 줄(중복 행)로 존재하는지 검사
df_over = df[df['untyCntrctNo'].isin(over_100_unty_nos)]
print(f"해당 계약번호들의 총 DB 행(Row) 수: {len(df_over)}행")

# 3. 중복도 검사
counts = df_over['untyCntrctNo'].value_counts()
multiple_rows_cnt = (counts > 1).sum()
single_row_cnt = (counts == 1).sum()

print(f"- 1건의 계약이 여러 줄(중복 행)로 등록된 경우: {multiple_rows_cnt}건")
print(f"- 1건의 계약이 1줄(단일 행)로 정상 등록된 경우: {single_row_cnt}건")

if multiple_rows_cnt > 0:
    print("\n[중복 행 예시]")
    dup_nos = counts[counts > 1].head(3).index
    for d_no in dup_nos:
        rows = df_over[df_over['untyCntrctNo'] == d_no]
        print(f" ▶ {d_no}: DB상에 {len(rows)}줄 존재함.")

conn.close()
