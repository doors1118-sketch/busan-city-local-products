import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('busan_agencies_master.db')
cur = conn.cursor()

# 수정 대상: 남구 → 수영구로 이관할 동
suyeong_dongs = ['광안1동', '광안2동', '광안3동', '광안4동', '남천1동', '수영동', '망미1동', '망미2동']

print("🔍 수정 대상 확인:")
for dong in suyeong_dongs:
    cur.execute("SELECT dminsttCd, dminsttNm, cate_sml FROM agency_master WHERE dminsttNm LIKE ?", (f'%{dong}%',))
    rows = cur.fetchall()
    for r in rows:
        if '남구' in r[1]:
            print(f"  {r[0]} | {r[1]} | 현재: {r[2]} → 수영구로 변경")
            cur.execute("UPDATE agency_master SET cate_sml = '수영구' WHERE dminsttCd = ?", (r[0],))

# 남구을선거관리위원회 → 정부기관
cur.execute("SELECT dminsttCd, dminsttNm, cate_lrg, cate_mid, cate_sml FROM agency_master WHERE dminsttNm LIKE '%남구을선거%'")
rows = cur.fetchall()
for r in rows:
    print(f"\n  {r[0]} | {r[1]} | 현재: {r[2]}>{r[3]}>{r[4]}")
    print(f"    → 정부 및 국가공공기관으로 변경")
    cur.execute("UPDATE agency_master SET cate_lrg = '정부 및 국가공공기관', cate_mid = '정부 및 국가공공기관', cate_sml = '선거관리위원회' WHERE dminsttCd = ?", (r[0],))

conn.commit()

# 확인
print("\n✅ 수정 후 확인:")
cur.execute("SELECT dminsttCd, dminsttNm, cate_sml FROM agency_master WHERE cate_sml = '수영구'")
print(f"\n수영구 분류 기관:")
for r in cur.fetchall():
    print(f"  {r[0]} | {r[1]}")

cur.execute("SELECT dminsttCd, dminsttNm, cate_lrg, cate_sml FROM agency_master WHERE dminsttNm LIKE '%남구을선거%'")
for r in cur.fetchall():
    print(f"\n남구을선거관리위원회: {r[2]} > {r[3]}")

conn.close()
