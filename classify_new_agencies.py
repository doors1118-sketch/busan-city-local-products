import sqlite3, sys, re
sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('busan_agencies_master.db')
c = conn.cursor()

# 미분류 기관 로드
c.execute("SELECT dminsttCd, dminsttNm FROM agency_master WHERE cate_lrg = '미분류'")
unclassified = c.fetchall()
print(f"미분류 기관 총: {len(unclassified)}건\n")

# ===== 분류 룰 (이름 패턴 기반) =====
# 구군 본청/보건소/의회 매핑 테이블
district_map = {
    '강서구': '강서구', '금정구': '금정구', '기장군': '기장군',
    '남구': '남구', '동구': '동구', '동래구': '동래구',
    '부산진구': '부산진구', '북구': '북구', '사상구': '사상구',
    '사하구': '사하구', '서구': '서구', '수영구': '수영구',
    '연제구': '연제구', '영도구': '영도구', '중구': '중구',
    '해운대구': '해운대구',
}

def classify(code, name):
    """기관명 기반 자동 분류 (대/중/소 반환)"""
    
    # 1. 부산광역시 자치구군 (본청, 보건소, 의회, 읍면동 등)
    for district, sml in district_map.items():
        if f'부산광역시 {district}' in name:
            return ('부산광역시 및 소속기관', '자치구군', sml)
    
    # 2. 부산광역시 본청 직속 / 사업소
    if name.startswith('부산광역시 ') and '교육청' not in name:
        nm_rest = name.replace('부산광역시 ', '')
        # 사업소 패턴
        if any(kw in nm_rest for kw in ['사업소', '시험', '수련원', '체육관', '의료원', '인재개발원', '소방서', '119']):
            return ('부산광역시 및 소속기관', '부산광역시', '부산광역시 사업소')
        # 직속기관 (본부, 센터 등)
        if any(kw in nm_rest for kw in ['본부', '센터', '연구원', '도서관', '재활원', '복지관', '청소년', '박물관', '미술관', '의회']):
            return ('부산광역시 및 소속기관', '부산광역시', '부산광역시 직속기관')
        # 소방
        if '소방' in nm_rest:
            return ('부산광역시 및 소속기관', '부산광역시', '소방')
        # 나머지 부산광역시 산하
        return ('부산광역시 및 소속기관', '부산광역시', '부산광역시 사업소')
    
    # 3. 부산광역시 교육청 산하
    if '부산광역시교육청' in name or '부산광역시남부교육청' in name or '부산광역시동래교육청' in name or \
       '부산광역시동부교육청' in name or '부산광역시북부교육청' in name or '부산광역시서부교육청' in name or \
       '부산광역시해운대교육청' in name:
        if '교육청' in name and ('초등학교' in name or '중학교' in name or '고등학교' in name):
            return ('부산광역시 및 소속기관', '부산광역시 교육청', '각급학교')
        if '교육청' in name and '유치원' in name:
            return ('부산광역시 및 소속기관', '부산광역시 교육청', '유아교육')
        if '특수학교' in name or '혜성학교' in name or '은애학교' in name or '혜광학교' in name:
            return ('부산광역시 및 소속기관', '부산광역시 교육청', '특수학교')
        if any(kw in name for kw in ['고등학교', '중학교', '초등학교']):
            return ('부산광역시 및 소속기관', '부산광역시 교육청', '각급학교')
        return ('부산광역시 및 소속기관', '부산광역시 교육청', '교육행정기관')
    
    # 4. 부산 산하 공기업/공단
    if '부산교통공사' in name:
        return ('부산광역시 및 소속기관', '부산광역시 산하기관', '부산광역시 공기업')
    if '부산환경공단' in name:
        return ('부산광역시 및 소속기관', '부산광역시 산하기관', '부산광역시 공단')
    if '부산광역시남구시설관리공단' in name:
        return ('부산광역시 및 소속기관', '부산광역시 산하기관', '부산광역시 공단')
    if '문화재단' in name and '부산' in name:
        return ('부산광역시 및 소속기관', '부산광역시 산하기관', '부산광역시 출연기관')
    
    # 5. 경찰청
    if '경찰청' in name and '부산' in name:
        return ('정부 및 국가공공기관', '중앙행정기관', '경찰청')
    
    # 6. 해양경찰
    if '해양경찰' in name and '부산' in name:
        return ('정부 및 국가공공기관', '중앙행정기관', '해양경찰청')
    
    # 7. 해양수산부
    if '해양수산' in name and '부산' in name:
        return ('정부 및 국가공공기관', '중앙행정기관', '해양수산부')
    
    # 8. 조달청
    if '조달청 부산' in name:
        return ('정부 및 국가공공기관', '중앙행정기관', '조달청')
    
    # 9. 국가공공기관 (공단, 공사 등)
    if any(kw in name for kw in ['공단', '공사']) and '부산' in name and '부산광역시' not in name:
        return ('정부 및 국가공공기관', '국가공공기관', '국가공단')
    
    # 10. 대학교
    if '대학교' in name or '대학' in name:
        if '병원' in name:
            return ('정부 및 국가공공기관', '고등교육기관', '대학병원')
        return ('정부 및 국가공공기관', '고등교육기관', '대학')
    
    # 11. 중소벤처기업부
    if '중소벤처기업부' in name:
        return ('정부 및 국가공공기관', '중앙행정기관', '중소벤처기업부')
    
    # 12. 기타 정부 기관
    if any(kw in name for kw in ['국립', '국가', '기상청', '국세청', '검찰청', '질병관리청']):
        return ('정부 및 국가공공기관', '중앙행정기관', '기타중앙부처')
    
    # 기타
    return ('미분류', '미분류', '미분류')

# ===== 분류 실행 =====
results = []
classified_count = 0
still_unclassified = 0

for code, name in unclassified:
    lrg, mid, sml = classify(code, name)
    results.append((code, name, lrg, mid, sml))
    
    if lrg != '미분류':
        c.execute("UPDATE agency_master SET cate_lrg=?, cate_mid=?, cate_sml=? WHERE dminsttCd=?", (lrg, mid, sml, code))
        classified_count += 1
    else:
        still_unclassified += 1

conn.commit()
print(f"✅ 분류 완료: {classified_count}건 분류됨 / {still_unclassified}건 미분류 잔존")

# ===== CSV 내보내기 (사용자 검토용) =====
import csv
csv_path = '신규기관_분류결과.csv'
with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['기관코드', '기관명', '대분류', '중분류', '소분류'])
    for code, name, lrg, mid, sml in sorted(results, key=lambda x: (x[2], x[3], x[4], x[1])):
        writer.writerow([code, name, lrg, mid, sml])

print(f"📄 CSV 파일 생성: {csv_path}")

# 분류 요약
print(f"\n=== 분류 결과 요약 ===")
from collections import Counter
summary = Counter()
for _, _, lrg, mid, sml in results:
    summary[(lrg, mid)] += 1
for (lrg, mid), cnt in sorted(summary.items()):
    print(f"  [{lrg}] > [{mid}]: {cnt}건")

conn.close()
