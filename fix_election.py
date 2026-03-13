import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('busan_agencies_master.db')
c = conn.cursor()

# 모든 선거관리위원회를 올바른 국가기관으로 수정
c.execute("""
    UPDATE agency_master 
    SET cate_lrg = '정부 및 국가공공기관', 
        cate_mid = '중앙행정기관', 
        cate_sml = '중앙선거관리위원회' 
    WHERE dminsttNm LIKE '%선거관리위원회%'
""")

count = c.rowcount
print(f"✅ 총 {count}건의 선거관리위원회 소속 기관 분류를 '정부 및 국가공공기관 > 중앙선거관리위원회'로 수정 완료했습니다.")

conn.commit()

# 수정 후 확인
c.execute("SELECT dminsttCd, dminsttNm, cate_lrg, cate_mid, cate_sml FROM agency_master WHERE dminsttNm LIKE '%부산광역시 남구을선거%'")
rows = c.fetchall()
for r in rows:
    print(f"  {r[1]} -> 대: {r[2]} | 중: {r[3]} | 소: {r[4]}")

conn.close()
