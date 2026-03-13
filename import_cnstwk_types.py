"""
계약 엑셀 → cnstwk_cntrct 직접 종합/전문 매칭
==============================================
- 엑셀 입찰공고번호 → DB ntceNo 매칭 (99.5% 커버리지)
- 중분류공공조달분류 → cnstwkTypeLrg (종합건설/개별법령/시설물유지관리공사)
- 공공조달분류 → cnstwkTypeDtl (토목공사/건축공사/전기공사 등)
"""
import pandas as pd, sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')

DB = 'procurement_contracts.db'
conn = sqlite3.connect(DB, timeout=30)
conn.execute("ATTACH DATABASE 'busan_agencies_master.db' AS am")

# 1. 컬럼 추가
for col in ['cnstwkTypeLrg', 'cnstwkTypeDtl']:
    try:
        conn.execute(f"ALTER TABLE cnstwk_cntrct ADD COLUMN {col} TEXT DEFAULT ''")
        print(f"  ✅ cnstwk_cntrct.{col} 컬럼 추가")
    except:
        print(f"  ℹ️ cnstwk_cntrct.{col} 이미 있음")
conn.commit()

# 2. 계약 엑셀 로드
print(f"\n{'='*60}")
print("계약 엑셀 로드")
print("=" * 60)
df = pd.read_excel(r'공사 공동수급 계약 내역(전문종합구분).xlsx')
real_headers = [str(df.iloc[0, i]) if pd.notna(df.iloc[0, i]) else f'col{i}' for i in range(len(df.columns))]
df.columns = real_headers
df = df.iloc[1:].reset_index(drop=True)
print(f"총 {len(df):,}건")

# 3. 공고번호별 분류 (중복 제거 - 같은 공고에 여러 업체)
ntce_map = {}
for _, row in df.iterrows():
    ntce_no = str(row.get('입찰공고번호', '')).strip()
    if not ntce_no or ntce_no == 'nan':
        continue
    if ntce_no not in ntce_map:
        mid = str(row.get('중분류공공조달분류', '')).strip() if pd.notna(row.get('중분류공공조달분류')) else ''
        detail = str(row.get('공공조달분류', '')).strip() if pd.notna(row.get('공공조달분류')) else ''
        ntce_map[ntce_no] = (mid, detail)

print(f"고유 공고번호 매핑: {len(ntce_map):,}건")

# 4. cnstwk_cntrct UPDATE
print(f"\n{'='*60}")
print("cnstwk_cntrct UPDATE (ntceNo 매칭)")
print("=" * 60)

updated = 0
for ntce_no, (mid, detail) in ntce_map.items():
    if mid or detail:
        cur = conn.execute(
            "UPDATE cnstwk_cntrct SET cnstwkTypeLrg=?, cnstwkTypeDtl=? WHERE ntceNo=?",
            (mid, detail, ntce_no))
        updated += cur.rowcount

conn.commit()
print(f"  업데이트된 계약 행: {updated:,}건")

# 5. 부산 계약 커버리지 확인
print(f"\n{'='*60}")
print("부산 공사 계약 종합/전문 커버리지")
print("=" * 60)

busan_total = conn.execute("""
    SELECT COUNT(DISTINCT c.untyCntrctNo) 
    FROM cnstwk_cntrct c
    JOIN am.agency_master a ON c.dminsttCd = a.dminsttCd
""").fetchone()[0]

busan_with_type = conn.execute("""
    SELECT COUNT(DISTINCT c.untyCntrctNo) 
    FROM cnstwk_cntrct c
    JOIN am.agency_master a ON c.dminsttCd = a.dminsttCd
    WHERE c.cnstwkTypeLrg != '' AND c.cnstwkTypeLrg IS NOT NULL
""").fetchone()[0]

print(f"  전체: {busan_total:,}건")
print(f"  종합/전문 구분됨: {busan_with_type:,}건 ({busan_with_type/busan_total*100:.1f}%)")
print(f"  미구분: {busan_total-busan_with_type:,}건 ({(busan_total-busan_with_type)/busan_total*100:.1f}%)")

# 분포
print(f"\n=== cnstwkTypeLrg (중분류) 분포 ===")
rows = conn.execute("""
    SELECT c.cnstwkTypeLrg, COUNT(DISTINCT c.untyCntrctNo) cnt
    FROM cnstwk_cntrct c
    JOIN am.agency_master a ON c.dminsttCd = a.dminsttCd
    WHERE c.cnstwkTypeLrg != ''
    GROUP BY c.cnstwkTypeLrg ORDER BY cnt DESC
""").fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]:,}건")

print(f"\n=== cnstwkTypeDtl (세부 공공조달분류) 분포 ===")
rows2 = conn.execute("""
    SELECT c.cnstwkTypeDtl, COUNT(DISTINCT c.untyCntrctNo) cnt
    FROM cnstwk_cntrct c
    JOIN am.agency_master a ON c.dminsttCd = a.dminsttCd
    WHERE c.cnstwkTypeDtl != ''
    GROUP BY c.cnstwkTypeDtl ORDER BY cnt DESC
""").fetchall()
for r in rows2:
    print(f"  {r[0]}: {r[1]:,}건")

# 미구분 건의 계약방법 확인
print(f"\n=== 미구분 건의 계약방법 ===")
rows3 = conn.execute("""
    SELECT c.cntrctCnclsMthdNm, COUNT(DISTINCT c.untyCntrctNo) cnt
    FROM cnstwk_cntrct c
    JOIN am.agency_master a ON c.dminsttCd = a.dminsttCd
    WHERE c.cnstwkTypeLrg = '' OR c.cnstwkTypeLrg IS NULL
    GROUP BY c.cntrctCnclsMthdNm ORDER BY cnt DESC
""").fetchall()
for r in rows3:
    print(f"  {r[0]}: {r[1]:,}건")

conn.close()
print(f"\n🎉 완료!")
