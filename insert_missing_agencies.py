import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn_ag = sqlite3.connect('busan_agencies_master.db')
df_ag = pd.read_sql("SELECT dminsttCd FROM agency_master", conn_ag)
master_codes = set(df_ag['dminsttCd'].astype(str).str.strip())

conn = sqlite3.connect('procurement_contracts.db')
df = pd.read_sql("""
    SELECT dminsttCd, dminsttNm, COUNT(*) as cnt
    FROM shopping_cntrct
    WHERE dlvrReqRcptDate >= '2026-01-01'
    GROUP BY dminsttCd, dminsttNm
""", conn)
conn.close()

df['dminsttCd'] = df['dminsttCd'].astype(str).str.strip()
df_missing = df[~df['dminsttCd'].isin(master_codes)].copy()

gu_map = {
    '강서구': '강서구', '금정구': '금정구', '기장군': '기장군', '남구': '남구',
    '동구': '동구', '동래구': '동래구', '부산진구': '부산진구', '북구': '북구',
    '사상구': '사상구', '사하구': '사하구', '서구': '서구', '수영구': '수영구',
    '연제구': '연제구', '영도구': '영도구', '중구': '중구', '해운대구': '해운대구',
}

def classify_strict(name):
    name = str(name)
    if '부산' not in name:
        return None
    
    if '부산광역시' in name:
        for gu_name, gu_val in gu_map.items():
            if gu_name in name:
                return ('부산광역시 및 소속기관', '부산광역시 구·군', gu_val)
        if '교육' in name:
            return ('부산광역시 및 소속기관', '부산광역시 본청 및 직속기관', '부산광역시교육청')
        if any(kw in name for kw in ['소방서', '상수도', '선거관리']):
            return ('부산광역시 및 소속기관', '부산광역시 본청 및 직속기관', '부산광역시')
        return ('부산광역시 및 소속기관', '부산광역시 본청 및 직속기관', '부산광역시')
    
    if '부산' in name and '교육' in name:
        return ('부산광역시 및 소속기관', '부산광역시 본청 및 직속기관', '부산광역시교육청')
    
    if any(kw in name for kw in ['부산지방', '부산고등', '부산세관', '부산체신', '부산지방우정']):
        return ('정부 및 국가공공기관', '정부 및 국가공공기관', name)
    
    return None

# INSERT
cur = conn_ag.cursor()
inserted = 0
for _, r in df_missing.iterrows():
    cls = classify_strict(r['dminsttNm'])
    if cls:
        cur.execute(
            "INSERT OR IGNORE INTO agency_master (dminsttCd, dminsttNm, cate_lrg, cate_mid, cate_sml) VALUES (?, ?, ?, ?, ?)",
            (r['dminsttCd'], r['dminsttNm'], cls[0], cls[1], cls[2])
        )
        if cur.rowcount > 0:
            inserted += 1

conn_ag.commit()
print(f"✅ {inserted}개 기관 추가 완료!")

# 확인
cur.execute("SELECT COUNT(*) FROM agency_master")
print(f"마스터DB 총 기관 수: {cur.fetchone()[0]}")

conn_ag.close()
