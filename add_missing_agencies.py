import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('busan_agencies_master.db')
cur = conn.cursor()

# 기존 분류 참고: 강서구 = 3360000, 서구 = 3260000
# cate_lrg: 부산광역시 및 소속기관
# cate_mid: 부산광역시 구·군
# cate_sml: 강서구 / 서구

new_agencies = [
    ('3360043', '부산광역시 강서구 대저2동', '부산광역시 및 소속기관', '부산광역시 구·군', '강서구'),
    ('3260021', '부산광역시 서구 의회사무국', '부산광역시 및 소속기관', '부산광역시 구·군', '서구'),
]

for code, name, lrg, mid, sml in new_agencies:
    cur.execute(
        "INSERT OR IGNORE INTO agency_master (dminsttCd, dminsttNm, cate_lrg, cate_mid, cate_sml) VALUES (?, ?, ?, ?, ?)",
        (code, name, lrg, mid, sml)
    )
    print(f"  ✅ {code} | {name} | {lrg} > {mid} > {sml}")

conn.commit()

# 확인
cur.execute("SELECT dminsttCd, dminsttNm, cate_lrg, cate_mid, cate_sml FROM agency_master WHERE dminsttCd IN ('3360043', '3260021')")
print("\n등록 확인:")
for r in cur.fetchall():
    print(f"  {r[0]} | {r[1]} | {r[2]} > {r[3]} > {r[4]}")

conn.close()
