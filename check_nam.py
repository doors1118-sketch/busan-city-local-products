import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('busan_agencies_master.db')
c = conn.cursor()
c.execute("SELECT dminsttCd, dminsttNm FROM agency_master WHERE cate_lrg='미분류'")
rows = c.fetchall()
print(f"미분류 잔존: {len(rows)}건")
for r in rows:
    print(f"  {r[0]} = {r[1]}")

# 수동 분류 처리
for code, name in rows:
    if '고속도로' in name or '도로' in name:
        c.execute("UPDATE agency_master SET cate_lrg='정부 및 국가공공기관', cate_mid='국가공공기관', cate_sml='국가공사' WHERE dminsttCd=?", (code,))
    elif '부산' in name:
        c.execute("UPDATE agency_master SET cate_lrg='부산광역시 및 소속기관', cate_mid='부산광역시 산하기관', cate_sml='부산광역시 출연기관' WHERE dminsttCd=?", (code,))
    else:
        c.execute("UPDATE agency_master SET cate_lrg='민간 및 기타기관', cate_mid='민간기관', cate_sml='기타기관' WHERE dminsttCd=?", (code,))
    print(f"  ✅ {name} 수동 분류 완료")

conn.commit()

# 최종 검증
c.execute("SELECT count(*) FROM agency_master WHERE cate_lrg='미분류'")
print(f"\n최종 미분류 잔존: {c.fetchone()[0]}건")

conn.close()
