import pandas as pd
import sqlite3
import sys
sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'busan_companies_master.db'

# 1. 엑셀 로드
print("=== Step 1: 엑셀 로드 ===")
df = pd.read_excel('조달업체 면허 업종 등록 내역.xlsx', header=0, dtype=str)
df.columns = df.iloc[0]
df = df.iloc[1:].reset_index(drop=True)
print(f"전체: {len(df):,}행, {df['업체사업자등록번호'].nunique():,}개사")

# 2. DB 연결 및 테이블 확인
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# company_industry 테이블 생성 (없으면)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS company_industry (
        bizno TEXT,
        indstrytyCd TEXT,
        indstrytyNm TEXT,
        rgstDt TEXT,
        vldPrdExprtDt TEXT,
        indstrytyStatsNm TEXT,
        rprsntIndstrytyYn TEXT,
        chgDt TEXT,
        PRIMARY KEY (bizno, indstrytyCd)
    )
''')

# 기존 DB 업체 목록
cursor.execute("SELECT bizno FROM company_master")
db_biznos = set(row[0] for row in cursor.fetchall())

# 3. 임포트
print("\n=== Step 2: company_industry에 임포트 ===")
inserted = 0
skipped = 0

# 업종코드가 없으므로 면허업종명 자체를 코드로 사용
for _, row in df.iterrows():
    bizno = str(row['업체사업자등록번호']).replace('-', '').strip()
    if bizno not in db_biznos:
        skipped += 1
        continue
    
    indstry_nm = str(row.get('면허업종', '')).strip()
    if not indstry_nm or indstry_nm == 'nan':
        continue
    
    rprsnt = 'Y' if str(row.get('대표면허여부', '')) == 'Y' else 'N'
    rgst_dt = str(row.get('입력일자', '')).strip()
    chg_dt = str(row.get('변경일자', '')).strip()
    
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO company_industry
            (bizno, indstrytyCd, indstrytyNm, rgstDt, vldPrdExprtDt, indstrytyStatsNm, rprsntIndstrytyYn, chgDt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (bizno, indstry_nm, indstry_nm, rgst_dt, '', '', rprsnt, chg_dt))
        inserted += 1
    except Exception as e:
        pass

conn.commit()

# 4. 결과 확인
print(f"임포트 성공: {inserted:,}건")
print(f"DB에 없는 업체 (스킵): {skipped}건")

cursor.execute("SELECT COUNT(*) FROM company_industry")
total = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(DISTINCT bizno) FROM company_industry")
corps = cursor.fetchone()[0]
print(f"\n=== company_industry 현황 ===")
print(f"총 행수: {total:,}")
print(f"업체 수: {corps:,}")

print(f"\n업종 Top 20:")
cursor.execute("""
    SELECT indstrytyNm, COUNT(*) cnt 
    FROM company_industry 
    GROUP BY indstrytyNm 
    ORDER BY cnt DESC 
    LIMIT 20
""")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1]}")

conn.close()
print("\n✅ 완료!")
