import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('busan_agencies_master.db')
cur = conn.cursor()

# 부산 외 지역 키워드: 이 키워드가 포함되면 부산 기관이 아님
non_busan_locations = [
    '김해', '영주', '포항', '대구', '울산', '진주', '양산', '제주',
    '경남서부', '경남동부', '경남중부', '마산', '창원', '통영', '거제',
    '전라남도', '전북', '전남', '충북', '충남', '경기', '강원',
    '서울', '인천', '대전', '광주', '세종',
]

# 방금 추가한 291개 중 오분류 찾기
cur.execute("SELECT dminsttCd, dminsttNm, cate_lrg, cate_mid, cate_sml FROM agency_master ORDER BY rowid DESC LIMIT 291")
recent = cur.fetchall()

to_delete = []
to_keep = []

for code, name, lrg, mid, sml in recent:
    is_wrong = False
    for loc in non_busan_locations:
        if loc in name:
            # "부산광역시 강서구" 같은 건 보존, "부산지방국토관리청 대구사무소" 같은 건 삭제
            # 부산광역시가 포함되면 보존
            if '부산광역시' in name:
                continue
            # 부산XX교육청 소속이면서 실제 부산 학교면 보존
            if '부산' in name and loc not in name.split('부산')[0]:
                is_wrong = True
                break
    
    if is_wrong:
        to_delete.append((code, name, lrg, mid))
    else:
        to_keep.append((code, name, lrg, mid))

print(f"✅ 정상 유지: {len(to_keep)}개")
print(f"❌ 삭제 대상: {len(to_delete)}개")
print()

print("❌ 삭제 대상:")
for code, name, lrg, mid in to_delete:
    print(f"  {code:10s} | {name:50s} | {lrg} > {mid}")

# 삭제 실행
for code, name, lrg, mid in to_delete:
    cur.execute("DELETE FROM agency_master WHERE dminsttCd = ?", (code,))

conn.commit()

cur.execute("SELECT COUNT(*) FROM agency_master")
print(f"\n마스터DB 총 기관 수: {cur.fetchone()[0]}")
conn.close()
