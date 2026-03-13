import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('busan_agencies_master.db')
c = conn.cursor()

# 남구선거관리위원회 등 선관위 전체 조회
c.execute("SELECT dminsttCd, dminsttNm, cate_lrg, cate_mid, cate_sml FROM agency_master WHERE dminsttNm LIKE '%선거관리위원회%'")
rows = c.fetchall()

print(f"선거관리위원회 분류 현황 ({len(rows)}건):")
for r in rows:
    print(f"  {r[1]} -> 대: {r[2]} | 중: {r[3]} | 소: {r[4]}")

# 부산광역시 남구 소속으로 잘못 들어간 게 있는지 확인
print("\n'부산광역시 남구' 로 분류된 선관위 확인:")
for r in rows:
    if r[4] == '남구':
        print(f"  [오류] {r[1]}")

conn.close()
