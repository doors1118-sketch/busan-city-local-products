import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')

# 1. 낙찰 데이터 기본 통계
total = conn.execute("SELECT COUNT(*) FROM busan_award_servc").fetchone()[0]
print(f"✅ 부산 지역제한 용역 낙찰정보: {total:,}건")

# 2. 연도별 분포
print("\n📊 연도별 분포:")
rows = conn.execute("SELECT SUBSTR(fnlSucsfDate, 1, 4) as yr, COUNT(*) FROM busan_award_servc GROUP BY yr ORDER BY yr").fetchall()
for yr, cnt in rows:
    print(f"  {yr}: {cnt:,}건")

# 3. 2026년 계약 중 매칭 가능한 건 확인
print("\n🔍 2026년 용역 계약 ↔ 부산 낙찰 매칭율:")
# 방법1: bidNtceNo → ntceNo 직접 매칭
match_ntce = conn.execute("""
    SELECT COUNT(DISTINCT s.untyCntrctNo) 
    FROM servc_cntrct s
    JOIN busan_award_servc a ON REPLACE(s.ntceNo, '-', '') = a.bidNtceNo
    WHERE s.cntrctDate >= '2026-01-01'
    AND s.cntrctCnclsMthdNm != '수의계약'
""").fetchone()[0]

total_comp = conn.execute("""
    SELECT COUNT(DISTINCT untyCntrctNo)
    FROM servc_cntrct 
    WHERE cntrctDate >= '2026-01-01' AND cntrctCnclsMthdNm != '수의계약'
""").fetchone()[0]

print(f"  경쟁입찰 용역: {total_comp:,}건")
print(f"  공고번호로 부산 낙찰 매칭: {match_ntce:,}건 ({match_ntce/total_comp*100:.1f}%)")

# 방법2: 사업자번호+금액으로 추가 매칭 시도
# servc_cntrct의 corpList에서 사업자번호를 추출해야 하므로 복잡. 일단 ntceNo 기준으로 확인

# 4. 키워드 필터에 걸리는 건 중 이제 구제 가능한 건
print("\n🎯 키워드 필터 대상 중 부산 낙찰 확인으로 구제 가능한 건:")
# 키워드 필터 대상이면서 부산 낙찰에 있는 건
keywords = ['서울', '인천', '대구', '대전', '광주광역', '울산', '세종', '제주',
            '경기', '강원', '충북', '충남', '전북', '전남', '경북', '경남']
kw_conditions = " OR ".join([f"s.cntrctNm LIKE '%{kw}%'" for kw in keywords])

rescued = conn.execute(f"""
    SELECT COUNT(DISTINCT s.untyCntrctNo)
    FROM servc_cntrct s
    JOIN busan_award_servc a ON REPLACE(s.ntceNo, '-', '') = a.bidNtceNo
    WHERE s.cntrctDate >= '2026-01-01'
    AND ({kw_conditions})
""").fetchone()[0]

print(f"  키워드에 걸렸지만 부산 낙찰 확인된 건: {rescued}건 (= 구제 가능!)")

# 5. 샘플 확인
print("\n📝 구제 가능 건 샘플:")
samples = conn.execute(f"""
    SELECT s.cntrctNm, a.bidNtceNm, a.dminsttNm, a.sucsfbidAmt
    FROM servc_cntrct s
    JOIN busan_award_servc a ON REPLACE(s.ntceNo, '-', '') = a.bidNtceNo
    WHERE s.cntrctDate >= '2026-01-01'
    AND ({kw_conditions})
    LIMIT 5
""").fetchall()
for nm, bname, inst, amt in samples:
    print(f"  📌 [{inst}] {nm[:40]} (낙찰: {int(amt):,}원)")

conn.close()
