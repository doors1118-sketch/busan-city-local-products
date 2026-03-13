import sqlite3
import pandas as pd
import sys
import re

sys.stdout.reconfigure(encoding='utf-8')

conn_ag = sqlite3.connect('busan_agencies_master.db')
df_ag = pd.read_sql("SELECT dminsttCd FROM agency_master", conn_ag)
conn_ag.close()
master_codes = set(df_ag['dminsttCd'].astype(str).str.strip())

conn = sqlite3.connect('procurement_contracts.db')
df = pd.read_sql("""
    SELECT dminsttCd, dminsttNm, COUNT(*) as cnt, 
           SUM(CAST(prdctAmt AS REAL)) as total_amt
    FROM shopping_cntrct
    WHERE dlvrReqRcptDate >= '2026-01-01'
    GROUP BY dminsttCd, dminsttNm
    ORDER BY total_amt DESC
""", conn)
conn.close()

df['dminsttCd'] = df['dminsttCd'].astype(str).str.strip()
df_missing = df[~df['dminsttCd'].isin(master_codes)].copy()

# 엄격한 분류: "부산광역시"가 기관명에 포함된 경우만 부산 기관으로 판정
gu_map = {
    '강서구': '강서구', '금정구': '금정구', '기장군': '기장군', '남구': '남구',
    '동구': '동구', '동래구': '동래구', '부산진구': '부산진구', '북구': '북구',
    '사상구': '사상구', '사하구': '사하구', '서구': '서구', '수영구': '수영구',
    '연제구': '연제구', '영도구': '영도구', '중구': '중구', '해운대구': '해운대구',
}

def classify_strict(name):
    name = str(name)
    
    # 반드시 "부산"이 포함되어야 함
    if '부산' not in name:
        return None
    
    # 타 광역시 기관 배제 (부산 + 대구 등 겹치는 경우 방지)
    other_cities = ['서울', '인천', '대구', '대전', '광주', '울산', '세종', '제주']
    for city in other_cities:
        if city in name and '부산' in name:
            # "부산고등검찰청 울산지방검찰청" 같은 건 부산으로 분류해도 됨
            # 하지만 순수하게 타 도시 기관이면 배제
            pass  # 부산이 포함되어 있으므로 일단 진행
    
    # 부산광역시 구·군 소속
    if '부산광역시' in name:
        for gu_name, gu_val in gu_map.items():
            if gu_name in name:
                return ('부산광역시 및 소속기관', '부산광역시 구·군', gu_val)
        
        # 교육청 소속
        if '교육' in name:
            return ('부산광역시 및 소속기관', '부산광역시 본청 및 직속기관', '부산광역시교육청')
        
        # 소방서, 상수도, 선거관리위원회 등 직속기관
        if any(kw in name for kw in ['소방서', '상수도', '선거관리', '경찰', '보훈']):
            return ('부산광역시 및 소속기관', '부산광역시 본청 및 직속기관', '부산광역시')
        
        # 부산광역시 본청
        return ('부산광역시 및 소속기관', '부산광역시 본청 및 직속기관', '부산광역시')
    
    # "부산" 포함되지만 "부산광역시"가 아닌 경우: 산하기관 등
    if any(kw in name for kw in ['부산교통공사', '부산환경공단', '부산시설공단', '부산관광공사']):
        return ('부산광역시 및 소속기관', '부산광역시 산하기관', name)
    
    # 부산지방XX, 부산XX교육청 등 = 정부기관 중 부산 소재
    if any(kw in name for kw in ['부산지방', '부산고등', '부산세관', '부산체신']):
        return ('정부 및 국가공공기관', '정부 및 국가공공기관', name)
    
    # 부산XX교육청 소속 학교
    if '부산' in name and '교육' in name:
        return ('부산광역시 및 소속기관', '부산광역시 본청 및 직속기관', '부산광역시교육청')
    
    return None  # 판별 불가

busan_agencies = []
uncertain = []

for _, r in df_missing.iterrows():
    cls = classify_strict(r['dminsttNm'])
    if cls:
        busan_agencies.append({
            'code': r['dminsttCd'], 'name': r['dminsttNm'],
            'lrg': cls[0], 'mid': cls[1], 'sml': cls[2],
            'cnt': r['cnt'], 'amt': r['total_amt']
        })

print(f"✅ 부산 기관 (자동분류): {len(busan_agencies)}개")
print(f"총 발주액: {sum(a['amt'] for a in busan_agencies):,.0f}원")
print()

# 분류별 요약
from collections import Counter
mid_cnt = Counter(a['mid'] for a in busan_agencies)
for mid, cnt in mid_cnt.most_common():
    amt = sum(a['amt'] for a in busan_agencies if a['mid'] == mid)
    print(f"  {mid:25s} | {cnt:3d}개 | {amt:>15,.0f}원")

print(f"\n상위 20개:")
for a in sorted(busan_agencies, key=lambda x: -x['amt'])[:20]:
    print(f"  {a['code']:10s} | {a['name']:35s} | {a['mid']:20s} > {a['sml'][:10]:10s} | {a['amt']:>15,.0f}원")
