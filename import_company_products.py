# 조달업체 등록 내역.csv → busan_companies_master.db 임포트
# 대표세부품명번호, 대표세부품명, 대표업종 컬럼 추가 및 채우기

import pandas as pd
import sqlite3
import sys
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'busan_companies_master.db'

# 1. CSV 로드 (부산+본사만)
print("=== Step 1: CSV 로드 ===")
df = pd.read_csv('조달업체 등록 내역.csv', sep='\t', encoding='utf-16', dtype=str)
busan = df[(df['업체소재시도'].str.contains('부산', na=False)) & (df['본사지사구분'] == '본사')].copy()
busan['사업자등록번호'] = busan['사업자등록번호'].str.replace('-', '').str.strip()
print(f"CSV 부산+본사: {len(busan):,}건")
print(f"대표세부품명 있는 건: {busan['대표세부품명'].notna().sum():,}건")

# 2. DB에 컬럼 추가
print("\n=== Step 2: DB 컬럼 추가 ===")
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# 기존 컬럼 확인
cursor.execute("PRAGMA table_info(company_master)")
existing_cols = [row[1] for row in cursor.fetchall()]
print(f"기존 컬럼: {existing_cols}")

new_cols = {
    'rprsntDtlPrdnmNo': 'TEXT',    # 대표세부품명번호
    'rprsntDtlPrdnm': 'TEXT',       # 대표세부품명
    'rprsntIndstrytyNm': 'TEXT',    # 대표업종
}

for col, dtype in new_cols.items():
    if col not in existing_cols:
        cursor.execute(f"ALTER TABLE company_master ADD COLUMN {col} {dtype}")
        print(f"  + {col} 컬럼 추가")
    else:
        print(f"  - {col} 이미 존재")

conn.commit()

# 3. CSV 데이터로 업데이트
print("\n=== Step 3: CSV → DB 업데이트 ===")
updated = 0
not_in_db = 0

for _, row in busan.iterrows():
    bizno = str(row['사업자등록번호']).strip()
    prdnm_no = row.get('대표세부품명번호', '')
    prdnm = row.get('대표세부품명', '')
    indstry = row.get('대표업종', '')
    
    # NaN 처리
    if pd.isna(prdnm_no): prdnm_no = ''
    if pd.isna(prdnm): prdnm = ''
    if pd.isna(indstry): indstry = ''
    
    if not prdnm and not indstry:
        continue
    
    cursor.execute("""
        UPDATE company_master 
        SET rprsntDtlPrdnmNo=?, rprsntDtlPrdnm=?, rprsntIndstrytyNm=?
        WHERE bizno=?
    """, (prdnm_no, prdnm, indstry, bizno))
    
    if cursor.rowcount > 0:
        updated += 1
    else:
        not_in_db += 1

conn.commit()

# 4. 결과 확인
print(f"\n=== 결과 ===")
print(f"업데이트 성공: {updated:,}건")
print(f"DB에 없는 업체 (CSV에만 존재): {not_in_db:,}건")

cursor.execute("SELECT COUNT(*) FROM company_master WHERE rprsntDtlPrdnm IS NOT NULL AND rprsntDtlPrdnm != ''")
filled = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(*) FROM company_master")
total = cursor.fetchone()[0]
print(f"\nDB 전체: {total:,}건")
print(f"대표세부품명 채워진 건: {filled:,}건 ({filled/total*100:.1f}%)")

# 세부품명 Top 20
print(f"\n=== 대표세부품명 Top 20 ===")
cursor.execute("""
    SELECT rprsntDtlPrdnm, COUNT(*) cnt 
    FROM company_master 
    WHERE rprsntDtlPrdnm IS NOT NULL AND rprsntDtlPrdnm != ''
    GROUP BY rprsntDtlPrdnm 
    ORDER BY cnt DESC 
    LIMIT 20
""")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1]}")

conn.close()
print("\n✅ 완료!")
