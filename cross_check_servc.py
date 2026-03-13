import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')
c = conn.cursor()

print("=" * 70)
print("  DB 내 중앙조달 vs 자체조달 비율 분석 (2026년)")
print("=" * 70)

for table, label in [('cnstwk_cntrct', '공사'), ('servc_cntrct', '용역'), ('thng_cntrct', '물품')]:
    print(f"\n### [{label}] ({table}) ###")
    
    c.execute(f"SELECT count(*) FROM {table} WHERE cntrctDate >= '2026-01-01'")
    total = c.fetchone()[0]
    
    c.execute(f"SELECT count(*) FROM {table} WHERE cntrctDate >= '2026-01-01' AND cntrctInsttNm LIKE '%조달청%'")
    central = c.fetchone()[0]
    
    local = total - central
    
    print(f"  전체: {total:,}건")
    print(f"  중앙조달 (계약기관='조달청'): {central:,}건 ({central/total*100:.1f}%)")
    print(f"  자체발주 (계약기관=실제기관): {local:,}건 ({local/total*100:.1f}%)")
    
    # 자체발주 중 부산 관련 건
    c.execute(f"SELECT count(*) FROM {table} WHERE cntrctDate >= '2026-01-01' AND cntrctInsttNm NOT LIKE '%조달청%' AND cntrctInsttNm LIKE '%부산%'")
    busan_local = c.fetchone()[0]
    print(f"  └ 자체발주 중 부산 관련: {busan_local:,}건")

# 남구 재확인
print("\n" + "=" * 70)
print("  [부산광역시 남구] 재확인 - 자체발주 통합")
print("=" * 70)

for table, label in [('cnstwk_cntrct', '공사'), ('servc_cntrct', '용역'), ('thng_cntrct', '물품')]:
    c.execute(f"SELECT count(*) FROM {table} WHERE cntrctDate >= '2026-01-01' AND (cntrctInsttNm LIKE '%부산%남구%' OR dminsttList LIKE '%부산%남구%')")
    cnt = c.fetchone()[0]
    print(f"  [{label}] 남구 관련 전체: {cnt}건")
    
    if cnt > 0:
        c.execute(f"SELECT cntrctInsttNm, untyCntrctNo FROM {table} WHERE cntrctDate >= '2026-01-01' AND (cntrctInsttNm LIKE '%부산%남구%' OR dminsttList LIKE '%부산%남구%') LIMIT 3")
        for r in c.fetchall():
            print(f"    - {r[0]} | {r[1]}")

conn.close()
