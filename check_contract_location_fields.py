import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')

# 1. cntrctDtlInfoUrl 확인 (상세 계약 웹페이지 URL)
print("=== 물품 계약 상세 URL 확인 ===")
rows = conn.execute("SELECT cntrctDtlInfoUrl FROM thng_cntrct WHERE cntrctDtlInfoUrl IS NOT NULL AND cntrctDtlInfoUrl != '' LIMIT 3").fetchall()
for r in rows:
    print(f"  {r[0]}")

# 2. ntceNo 비율 확인  
print("\n=== ntceNo 존재율 ===")
print("  용역:", conn.execute("SELECT COUNT(*) FROM servc_cntrct WHERE ntceNo IS NOT NULL AND ntceNo != '' AND ntceNo != 'nan'").fetchone()[0], 
      "/", conn.execute("SELECT COUNT(*) FROM servc_cntrct").fetchone()[0])
print("  물품:", conn.execute("SELECT COUNT(*) FROM thng_cntrct WHERE ntceNo IS NOT NULL AND ntceNo != '' AND ntceNo != 'nan'").fetchone()[0], 
      "/", conn.execute("SELECT COUNT(*) FROM thng_cntrct").fetchone()[0])

# 3. 물품의 baseDtls (기초내역)에 납품장소 같은 정보가 있는지 샘플 확인
print("\n=== baseDtls 샘플 확인 (물품) ===")
rows = conn.execute("SELECT baseDtls FROM thng_cntrct WHERE baseDtls IS NOT NULL AND baseDtls != '' LIMIT 3").fetchall()
for r in rows:
    txt = r[0][:200] if r[0] else ''
    print(f"  {txt}")
    print()

# 4. dminsttList에서 지역 정보를 파싱할 수 있는지 확인 (수요기관명에 주소 포함?)
print("=== dminsttList 샘플 (용역) ===")
rows = conn.execute("SELECT dminsttList FROM servc_cntrct WHERE dminsttList IS NOT NULL AND dminsttList != '' LIMIT 3").fetchall()
for r in rows:
    txt = r[0][:300] if r[0] else ''
    print(f"  {txt}")
    print()

conn.close()
