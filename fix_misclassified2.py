import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('busan_agencies_master.db')
cur = conn.cursor()

# 최근 추가한 기관 중 남아있는 것 (방금 13개 삭제 후 278개)
cur.execute("SELECT dminsttCd, dminsttNm, cate_lrg, cate_mid, cate_sml FROM agency_master ORDER BY rowid DESC LIMIT 278")
recent = cur.fetchall()

# 타지역 키워드: 이 키워드가 기관명 마지막 부분에 있으면 부산 외 지역
non_busan_check = [
    '김해', '영주', '포항', '대구', '울산', '진주', '양산', '제주',
    '경남', '마산', '창원', '통영', '거제', '밀양', '사천', '거창', '함안',
    '전라남도', '전북', '전남', '충북', '충남', '경기', '강원', '장흥',
    '서울', '인천', '대전', '광주', '세종', '여수', '순천', '목포',
]

to_delete = []
for code, name, lrg, mid, sml in recent:
    # "부산광역시"로 시작하는 건 무조건 보존
    if name.startswith('부산광역시'):
        continue
    
    # 그 외: 타지역 키워드 포함 시 삭제
    for loc in non_busan_check:
        if loc in name:
            to_delete.append((code, name, lrg, mid))
            break

print(f"추가 삭제 대상: {len(to_delete)}개")
print()
for code, name, lrg, mid in to_delete:
    print(f"  ❌ {code:10s} | {name}")

# 삭제 실행
for code, name, lrg, mid in to_delete:
    cur.execute("DELETE FROM agency_master WHERE dminsttCd = ?", (code,))

conn.commit()

# 최종 확인
cur.execute("SELECT COUNT(*) FROM agency_master")
total = cur.fetchone()[0]

# 남은 최근 추가 기관 확인
remaining = 278 - len(to_delete)
cur.execute(f"SELECT dminsttCd, dminsttNm, cate_lrg, cate_mid, cate_sml FROM agency_master ORDER BY rowid DESC LIMIT {remaining}")
kept = cur.fetchall()

print(f"\n마스터DB 총 기관 수: {total}")
print(f"이번에 순수하게 추가된 부산 기관: {remaining}개")

# 남은 것 중 혹시 이상한 것 체크
print(f"\n🔍 남은 기관 중 '부산광역시'로 시작하지 않는 것:")
for code, name, lrg, mid, sml in kept:
    if not name.startswith('부산광역시'):
        print(f"  ⚠️ {code:10s} | {name:50s} | {lrg}")

conn.close()
