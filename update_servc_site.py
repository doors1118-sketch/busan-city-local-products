# -*- coding: utf-8 -*-
"""
update_servc_site.py — 용역 현장지역 데이터 갱신
=================================================
1. servc_cntrct.cnstrtsiteRgnNm 기존값 전부 초기화
2. servc_site.db의 조달요청 API 데이터를 reqNo로 매칭하여 채우기
   - 기술용역: cnstrtsiteRgnNm
   - 일반용역: rprsntDlvrPlce
"""
import sqlite3, sys, os
sys.stdout.reconfigure(encoding='utf-8')
os.chdir(os.path.dirname(os.path.abspath(__file__)))

DB_MAIN = 'procurement_contracts.db'
DB_SITE = 'servc_site.db'

conn = sqlite3.connect(DB_MAIN, timeout=120)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA busy_timeout=120000")

# ============================================================
# Step 1: 기존 cnstrtsiteRgnNm 초기화
# ============================================================
print("=" * 70)
print("  Step 1: 기존 cnstrtsiteRgnNm 초기화")
print("=" * 70)

before_count = conn.execute(
    "SELECT COUNT(*) FROM servc_cntrct WHERE cnstrtsiteRgnNm IS NOT NULL AND cnstrtsiteRgnNm != ''"
).fetchone()[0]
print(f"  기존 값 있는 건: {before_count:,}건")

conn.execute("UPDATE servc_cntrct SET cnstrtsiteRgnNm = NULL")
conn.commit()
print(f"  → 전부 NULL로 초기화 완료")

# ============================================================
# Step 2: servc_site.db ATTACH → reqNo 매칭
# ============================================================
print(f"\n{'=' * 70}")
print("  Step 2: 조달요청 API 데이터 매칭 (reqNo)")
print("=" * 70)

conn.execute(f"ATTACH DATABASE '{DB_SITE}' AS site")

# 2-1. 기술용역 매칭 (servc_req_site.cnstrtsiteRgnNm)
print("\n  [기술용역] 매칭 중...")
result_tech = conn.execute("""
    UPDATE servc_cntrct SET cnstrtsiteRgnNm = (
        SELECT s.cnstrtsiteRgnNm FROM site.servc_req_site s
        WHERE s.prcrmntReqNo = servc_cntrct.reqNo
        AND s.cnstrtsiteRgnNm IS NOT NULL AND s.cnstrtsiteRgnNm != ''
        LIMIT 1
    )
    WHERE reqNo IS NOT NULL AND reqNo != ''
    AND EXISTS (
        SELECT 1 FROM site.servc_req_site s
        WHERE s.prcrmntReqNo = servc_cntrct.reqNo
        AND s.cnstrtsiteRgnNm IS NOT NULL AND s.cnstrtsiteRgnNm != ''
    )
""")
tech_count = result_tech.rowcount
conn.commit()
print(f"    매칭 완료: {tech_count:,}건")

# 2-2. 일반용역 매칭 (servc_req_site_gnrl.rprsntDlvrPlce)
print("\n  [일반용역] 매칭 중...")
result_gnrl = conn.execute("""
    UPDATE servc_cntrct SET cnstrtsiteRgnNm = (
        SELECT s.rprsntDlvrPlce FROM site.servc_req_site_gnrl s
        WHERE s.prcrmntReqNo = servc_cntrct.reqNo
        AND s.rprsntDlvrPlce IS NOT NULL AND s.rprsntDlvrPlce != ''
        LIMIT 1
    )
    WHERE reqNo IS NOT NULL AND reqNo != ''
    AND cnstrtsiteRgnNm IS NULL
    AND EXISTS (
        SELECT 1 FROM site.servc_req_site_gnrl s
        WHERE s.prcrmntReqNo = servc_cntrct.reqNo
        AND s.rprsntDlvrPlce IS NOT NULL AND s.rprsntDlvrPlce != ''
    )
""")
gnrl_count = result_gnrl.rowcount
conn.commit()
print(f"    매칭 완료: {gnrl_count:,}건")

conn.execute("DETACH DATABASE site")

# ============================================================
# Step 3: 결과 리포트
# ============================================================
print(f"\n{'=' * 70}")
print("  Step 3: 결과 리포트")
print("=" * 70)

total = conn.execute("SELECT COUNT(*) FROM servc_cntrct").fetchone()[0]
filled = conn.execute(
    "SELECT COUNT(*) FROM servc_cntrct WHERE cnstrtsiteRgnNm IS NOT NULL AND cnstrtsiteRgnNm != ''"
).fetchone()[0]
busan = conn.execute(
    "SELECT COUNT(*) FROM servc_cntrct WHERE cnstrtsiteRgnNm LIKE '%부산%'"
).fetchone()[0]
non_busan = filled - busan

print(f"  전체 용역 계약: {total:,}건")
print(f"  현장지역 채워짐: {filled:,}건 ({filled*100/total:.1f}%)")
print(f"    부산 현장: {busan:,}건")
print(f"    타지역 현장: {non_busan:,}건")
print(f"  현장지역 없음: {total - filled:,}건 ({(total-filled)*100/total:.1f}%)")

# 부산 기관 매칭 건 기준
conn_ag = sqlite3.connect('busan_agencies_master.db')
busan_cds = set(r[0] for r in conn_ag.execute("SELECT dminsttCd FROM agency_master").fetchall())
conn_ag.close()

busan_total = conn.execute(
    "SELECT COUNT(*) FROM servc_cntrct WHERE dminsttCd IN ({})".format(
        ','.join(f"'{c}'" for c in busan_cds))
).fetchone()[0]
busan_filled = conn.execute(
    "SELECT COUNT(*) FROM servc_cntrct WHERE dminsttCd IN ({}) AND cnstrtsiteRgnNm IS NOT NULL AND cnstrtsiteRgnNm != ''".format(
        ','.join(f"'{c}'" for c in busan_cds))
).fetchone()[0]
busan_site_busan = conn.execute(
    "SELECT COUNT(*) FROM servc_cntrct WHERE dminsttCd IN ({}) AND cnstrtsiteRgnNm LIKE '%부산%'".format(
        ','.join(f"'{c}'" for c in busan_cds))
).fetchone()[0]
busan_site_other = busan_filled - busan_site_busan

print(f"\n  [부산 기관only]")
print(f"    전체: {busan_total:,}건")
print(f"    현장 채워짐: {busan_filled:,}건 ({busan_filled*100/max(busan_total,1):.1f}%)")
print(f"    부산 현장: {busan_site_busan:,}건")
print(f"    타지역 현장: {busan_site_other:,}건 (← 이 건들이 필터로 배제될 대상)")

# 현장지역 Top 15
print(f"\n  === 현장지역 분포 Top 15 ===")
for r in conn.execute("""SELECT cnstrtsiteRgnNm, COUNT(*) c FROM servc_cntrct 
    WHERE cnstrtsiteRgnNm IS NOT NULL AND cnstrtsiteRgnNm != ''
    GROUP BY cnstrtsiteRgnNm ORDER BY c DESC LIMIT 15""").fetchall():
    print(f"    {r[0]}: {r[1]:,}건")

conn.close()
print(f"\n{'=' * 70}")
print(f"  완료! (기술 {tech_count:,} + 일반 {gnrl_count:,} = {tech_count+gnrl_count:,}건 매칭)")
print(f"{'=' * 70}")
