import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')

print("=== 용역 (경쟁입찰만) ===")
total = conn.execute("SELECT COUNT(*) FROM servc_cntrct WHERE cntrctCnclsMthdNm != '수의계약'").fetchone()[0]
has = conn.execute("SELECT COUNT(*) FROM servc_cntrct WHERE cntrctCnclsMthdNm != '수의계약' AND ntceNo IS NOT NULL AND ntceNo != '' AND ntceNo != 'nan'").fetchone()[0]
print(f"  공고번호 있음: {has:,} / {total:,} ({has/total*100:.1f}%)")

# 세부 분류별
for m in ['제한경쟁', '일반경쟁', '지명경쟁', '기타']:
    t = conn.execute(f"SELECT COUNT(*) FROM servc_cntrct WHERE cntrctCnclsMthdNm = '{m}'").fetchone()[0]
    h = conn.execute(f"SELECT COUNT(*) FROM servc_cntrct WHERE cntrctCnclsMthdNm = '{m}' AND ntceNo IS NOT NULL AND ntceNo != '' AND ntceNo != 'nan'").fetchone()[0]
    if t > 0:
        print(f"    - {m}: {h:,} / {t:,} ({h/t*100:.1f}%)")

print()
print("=== 물품 (경쟁입찰만) ===")
total2 = conn.execute("SELECT COUNT(*) FROM thng_cntrct WHERE cntrctCnclsMthdNm != '수의계약'").fetchone()[0]
has2 = conn.execute("SELECT COUNT(*) FROM thng_cntrct WHERE cntrctCnclsMthdNm != '수의계약' AND ntceNo IS NOT NULL AND ntceNo != '' AND ntceNo != 'nan'").fetchone()[0]
print(f"  공고번호 있음: {has2:,} / {total2:,} ({has2/total2*100:.1f}%)")

for m in ['제한경쟁', '일반경쟁', '지명경쟁']:
    t = conn.execute(f"SELECT COUNT(*) FROM thng_cntrct WHERE cntrctCnclsMthdNm = '{m}'").fetchone()[0]
    h = conn.execute(f"SELECT COUNT(*) FROM thng_cntrct WHERE cntrctCnclsMthdNm = '{m}' AND ntceNo IS NOT NULL AND ntceNo != '' AND ntceNo != 'nan'").fetchone()[0]
    if t > 0:
        print(f"    - {m}: {h:,} / {t:,} ({h/t*100:.1f}%)")

print()
print("=== 수의계약도 혹시 ntceNo 있는지 ===")
s = conn.execute("SELECT COUNT(*) FROM servc_cntrct WHERE cntrctCnclsMthdNm = '수의계약' AND ntceNo IS NOT NULL AND ntceNo != '' AND ntceNo != 'nan'").fetchone()[0]
st = conn.execute("SELECT COUNT(*) FROM servc_cntrct WHERE cntrctCnclsMthdNm = '수의계약'").fetchone()[0]
print(f"  용역 수의계약 중 공고번호 있음: {s:,} / {st:,} ({s/st*100:.1f}%)")

conn.close()
