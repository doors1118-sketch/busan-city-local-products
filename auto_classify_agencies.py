"""
신규 기관 자동 분류 (compare_unit + cate_lrg + cate_mid)
=====================================================
기관명 패턴 기반으로 기존 분류 규칙을 추론하여 자동 적용
"""
import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')

DB = 'busan_agencies_master.db'
conn = sqlite3.connect(DB, timeout=30)

# 미분류 기관 확인
unfilled = conn.execute("""
    SELECT dminsttCd, dminsttNm FROM agency_master 
    WHERE compare_unit IS NULL OR compare_unit = ''
""").fetchall()

print(f"미분류 기관: {len(unfilled)}건\n")

# 기존 compare_unit → cate 매핑 사전 구축
unit_cate_map = {}
for r in conn.execute("""
    SELECT compare_unit, cate_lrg, cate_mid, cate_sml, cate_dtl 
    FROM agency_master 
    WHERE compare_unit IS NOT NULL AND compare_unit != ''
    GROUP BY compare_unit
""").fetchall():
    unit_cate_map[r[0]] = {'lrg': r[1], 'mid': r[2], 'sml': r[3] or '', 'dtl': r[4] or ''}

# 자동 분류 함수
def auto_classify(name):
    """기관명으로 compare_unit + cate 추론"""
    
    # 1. 대학교 패턴 (가장 많은 미분류)
    if '부경대학교' in name or '부경대' in name:
        return '부경대학교', '정부 및 국가공공기관', '고등교육기관', '', ''
    if '부산대학교' in name and '교육대' not in name:
        return '부산대학교', '정부 및 국가공공기관', '고등교육기관', '', ''
    if '부산교육대학교' in name:
        return '부산교육대학교', '정부 및 국가공공기관', '고등교육기관', '', ''
    if '동명대학교' in name or '동명대' in name:
        return '동명대학교', '정부 및 국가공공기관', '고등교육기관', '', ''
    if '동서대학교' in name or '동서대' in name:
        return '동서대학교', '정부 및 국가공공기관', '고등교육기관', '', ''
    if '동아대학교' in name or '동아대' in name:
        return '동아대학교', '정부 및 국가공공기관', '고등교육기관', '', ''
    if '신라대학교' in name:
        return '신라대학교', '정부 및 국가공공기관', '고등교육기관', '', ''
    if '동의대학교' in name:
        return '동의대학교', '정부 및 국가공공기관', '고등교육기관', '', ''
    if '한국해양대학교' in name:
        return '한국해양대학교', '정부 및 국가공공기관', '고등교육기관', '', ''
    
    # 2. 교육청 산하 학교 패턴
    for kw in ['교육청', '교육지원청']:
        if kw in name:
            for sch in ['초등학교', '중학교', '고등학교', '유치원', '특수학교']:
                if sch in name:
                    return '각급학교', '부산광역시 및 소속기관', '부산광역시 교육청', '', ''
            return '교육청 본청 및 교육행정기관', '부산광역시 및 소속기관', '부산광역시 교육청', '', ''
    
    # 3. 부산광역시 자치구군 패턴
    districts = {
        '중구': '중구', '서구': '서구', '동구': '동구', '영도구': '영도구',
        '부산진구': '부산진구', '동래구': '동래구', '남구': '남구', '북구': '북구',
        '해운대구': '해운대구', '사하구': '사하구', '금정구': '금정구', '강서구': '강서구',
        '연제구': '연제구', '수영구': '수영구', '사상구': '사상구', '기장군': '기장군',
    }
    if '부산광역시' in name:
        for dist_key, dist_val in districts.items():
            if dist_key in name:
                return dist_val, '부산광역시 및 소속기관', '자치구군', '', ''
    
    # 4. 부산시 산하기관
    if '부산광역시' in name:
        for kw, unit in [
            ('상수도사업본부', '상수도사업본부'),
            ('시설공단', '부산시설공단'),
            ('환경공단', '부산환경공단'),
            ('교통공사', '부산교통공사'),
            ('의료원', '부산광역시의료원'),
            ('소방', '소방'),
        ]:
            if kw in name:
                return unit, '부산광역시 및 소속기관', '부산광역시 산하기관', '', ''
        # 부산시 본청
        return '부산광역시 본청', '부산광역시 및 소속기관', '부산광역시 본청', '', ''
    
    # 5. 정부기관 패턴
    govt_patterns = {
        '국토교통부': '국토교통부', '해양수산부': '해양수산부', '법무부': '법무부',
        '국방부': '국방부', '경찰': '경찰청', '해양경찰': '해양경찰청',
        '국세청': '국세청', '관세청': '관세청', '조달청': '조달청',
        '행정안전부': '행정안전부', '산업통상자원부': '산업통상자원부',
        '과학기술정보통신부': '과학기술정보통신부', '우정사업': '과학기술정보통신부',
        '국민건강보험': '국민건강보험공단', '국민연금': '국민연금공단',
        '한국전력': '한국전력공사', '한전': '한국전력공사',
        '한국토지주택공사': '한국토지주택공사', 'LH': '한국토지주택공사',
        '한국수자원공사': '한국수자원공사',
        '한국도로공사': '한국도로공사',
        '국군': '국방부',
    }
    for kw, unit in govt_patterns.items():
        if kw in name:
            return unit, '정부 및 국가공공기관', '중앙행정기관', '', ''
    
    # 6. 복지/의료 패턴
    for kw in ['요양원', '요양센터', '요양시설', '복지관', '복지센터', '장애인']:
        if kw in name:
            return '복지시설', '민간 및 기타기관', '복지기관', '', ''
    
    # 7. 어린이집/유치원
    if '어린이집' in name:
        return '어린이집', '민간 및 기타기관', '보육기관', '', ''
    
    # 8. 기존 분류에서 기관명 4글자 매칭으로 추론
    for prefix_len in [8, 6, 4]:
        if len(name) >= prefix_len:
            prefix = name[:prefix_len]
            match = conn.execute("""
                SELECT compare_unit, cate_lrg, cate_mid FROM agency_master 
                WHERE dminsttNm LIKE ? AND compare_unit IS NOT NULL AND compare_unit != ''
                LIMIT 1
            """, (f'{prefix}%',)).fetchone()
            if match:
                return match[0], match[1], match[2], '', ''
    
    return None, None, None, None, None

# 분류 실행
classified = []
unclassified = []
for cd, nm in unfilled:
    unit, lrg, mid, sml, dtl = auto_classify(nm)
    if unit:
        classified.append((unit, lrg, mid, sml, dtl, cd))
        print(f"  ✅ {nm[:40]:<42} → [{unit}] ({lrg}/{mid})")
    else:
        unclassified.append((cd, nm))
        print(f"  ❓ {nm[:40]:<42} → 분류 불가")

print(f"\n{'='*60}")
print(f"자동 분류 성공: {len(classified)}건")
print(f"분류 불가: {len(unclassified)}건")

if unclassified:
    print(f"\n=== 수동 분류 필요 ===")
    for cd, nm in unclassified:
        print(f"  {cd}: {nm}")

# DB 반영
if classified:
    conn.executemany("""
        UPDATE agency_master 
        SET compare_unit=?, cate_lrg=?, cate_mid=?, cate_sml=?, cate_dtl=?
        WHERE dminsttCd=?
    """, classified)
    conn.commit()
    print(f"\n✅ DB 반영 완료: {len(classified)}건 업데이트")

# 최종 확인
remaining = conn.execute("""
    SELECT COUNT(*) FROM agency_master WHERE compare_unit IS NULL OR compare_unit = ''
""").fetchone()[0]
total = conn.execute("SELECT COUNT(*) FROM agency_master").fetchone()[0]
print(f"\n최종: {total-remaining:,}/{total:,} ({(total-remaining)/total*100:.1f}%) 분류 완료")
if remaining:
    print(f"⚠️ 미분류 잔여: {remaining}건")

conn.close()
