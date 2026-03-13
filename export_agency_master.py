import sqlite3, sys, csv
sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('busan_agencies_master.db')
c = conn.cursor()

# 전체 agency_master 내보내기
c.execute("SELECT dminsttCd, dminsttNm, cate_lrg, cate_mid, cate_sml FROM agency_master ORDER BY cate_lrg, cate_mid, cate_sml, dminsttNm")
rows = c.fetchall()

csv_path = '기관분류_전체현황.csv'
with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['기관코드', '기관명', '대분류', '중분류', '소분류'])
    for r in rows:
        writer.writerow(r)

print(f"✅ 전체 기관 분류 현황 CSV 파일 생성: {csv_path} ({len(rows)}건)")

# 분류 요약
from collections import Counter
summary_lrg = Counter()
summary_mid = Counter()
for _, _, lrg, mid, sml in rows:
    summary_lrg[lrg] += 1
    summary_mid[(lrg, mid)] += 1

print(f"\n=== 대분류별 요약 ===")
for lrg, cnt in summary_lrg.most_common():
    print(f"  [{lrg}]: {cnt}건")

print(f"\n=== 중분류별 요약 ===")
for (lrg, mid), cnt in sorted(summary_mid.items()):
    print(f"  [{lrg}] > [{mid}]: {cnt}건")

conn.close()
