import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')

before = conn.execute('SELECT COUNT(*) FROM busan_award_thng').fetchone()[0]
conn.execute("DELETE FROM busan_award_thng WHERE fnlSucsfDate < '2025-01-01'")
conn.commit()
after = conn.execute('SELECT COUNT(*) FROM busan_award_thng').fetchone()[0]
print(f"물품 정리: {before:,} -> {after:,}건 (2025+ 만 유지)")

print("\n📊 최종 낙찰 테이블 현황:")
for t in ['busan_award_servc', 'busan_award_cnstwk', 'busan_award_thng']:
    cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"  {t}: {cnt:,}건")

conn.close()
