import sqlite3
import os
import glob

db = 'staging_chatbot_company.db'
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row

print("=" * 80)
print("1. source_manifest 전체 (API 기반 식별)")
print("=" * 80)
rows = conn.execute("SELECT * FROM source_manifest ORDER BY source_name").fetchall()
for r in rows:
    src_type = r['source_type'] or ''
    marker = ' ★API' if 'api' in src_type.lower() or 'api' in r['source_name'].lower() else ''
    print(f"  [{r['status']}] {r['source_name']} | type={src_type} | rows={r['row_count']} | refreshed={r['source_refreshed_at']}{marker}")

print()
print("=" * 80)
print("2. etl_job_log: API 관련 job만 필터")
print("=" * 80)
rows = conn.execute("""
    SELECT job_name, source_name, status, input_row_count, inserted_count, 
           skipped_count, error_count, started_at, finished_at, error_message
    FROM etl_job_log 
    WHERE job_name LIKE '%api%' OR source_name LIKE '%api%' 
       OR job_name LIKE '%import%' OR job_name LIKE '%nts%' OR job_name LIKE '%smpp%'
       OR job_name LIKE '%probe%'
    ORDER BY started_at DESC
""").fetchall()
for r in rows:
    dur = ''
    if r['started_at'] and r['finished_at']:
        from datetime import datetime
        try:
            s = datetime.strptime(r['started_at'], "%Y-%m-%d %H:%M:%S")
            e = datetime.strptime(r['finished_at'], "%Y-%m-%d %H:%M:%S")
            dur = f" ({(e-s).total_seconds():.0f}s)"
        except: pass
    print(f"  [{r['status']}] {r['job_name']} | src={r['source_name']} | in={r['input_row_count']} → ins={r['inserted_count']}, skip={r['skipped_count']}, err={r['error_count']} | {r['started_at']}{dur}")
    if r['error_message']:
        print(f"         msg: {r['error_message']}")

print()
print("=" * 80)
print("3. API별 DB 실적재 현황")
print("=" * 80)

# 3-1. NTS 사업자등록상태
nts = conn.execute("SELECT COUNT(*) FROM company_business_status").fetchone()[0]
nts_fresh = conn.execute("SELECT COUNT(*) FROM company_business_status WHERE business_status_freshness='fresh'").fetchone()[0]
nts_failed = conn.execute("SELECT COUNT(*) FROM company_business_status WHERE business_status_freshness='api_failed'").fetchone()[0]
print(f"\n  [국세청 사업자등록상태 API] company_business_status")
print(f"    총: {nts}, fresh: {nts_fresh}, api_failed: {nts_failed}")

# 3-2. 종합쇼핑몰/MAS API (import_mas_product_api.py)
mas_api = conn.execute("SELECT COUNT(*) FROM mas_product WHERE source_name LIKE '%api%'").fetchone()[0]
sm_api = conn.execute("SELECT COUNT(*) FROM shopping_mall_product WHERE source_name LIKE '%api%'").fetchone()[0]
print(f"\n  [종합쇼핑몰 MAS API] import_mas_product_api.py")
print(f"    mas_product (API): {mas_api}")
print(f"    shopping_mall_product (API): {sm_api}")

# 3-3. 인증제품 API (import_certified_product.py / smpp)
cert_api = conn.execute("SELECT COUNT(*) FROM certified_product WHERE source_name LIKE '%api%' OR source_name LIKE '%smpp%'").fetchone()[0]
cert_excel = conn.execute("SELECT COUNT(*) FROM certified_product WHERE source_name NOT LIKE '%api%' AND source_name NOT LIKE '%smpp%'").fetchone()[0]
print(f"\n  [인증제품 API] import_certified_product.py")
print(f"    certified_product (API): {cert_api}")
print(f"    certified_product (Excel): {cert_excel}")

# 3-4. 혁신제품 API
innov_api = conn.execute("SELECT COUNT(*) FROM certified_product WHERE source_name LIKE '%innovation%' OR source_name LIKE '%pps_innovation%'").fetchone()[0]
print(f"\n  [혁신제품/혁신장터 API] import_innovation_product.py")
print(f"    certified_product (innovation API): {innov_api}")

# 3-5. 중소기업 경쟁제품
sme = conn.execute("SELECT COUNT(*) FROM search_dictionary WHERE target_type='sme_competition_product'").fetchone()[0]
print(f"\n  [중소기업 경쟁제품]")
print(f"    search_dictionary (sme): {sme}")

print()
print("=" * 80)
print("4. API 수집 스크립트 존재 여부")
print("=" * 80)
api_scripts = [
    'import_mas_product_api.py',
    'import_certified_product.py',
    'import_innovation_product.py',
    'nts_business_status_client.py',
    'batch_nts_status_sync.py',
]
for s in api_scripts:
    exists = os.path.exists(s)
    print(f"  {'✅' if exists else '❌'} {s}")

# Also check for any other import_*.py scripts
others = glob.glob("import_*.py")
for o in others:
    if o.replace('\\','/') not in [x.replace('\\','/') for x in api_scripts]:
        print(f"  📄 {o} (추가 발견)")

print()
print("=" * 80)
print("5. source_manifest에 등록되었으나 row_count=0인 API 소스")
print("=" * 80)
rows = conn.execute("SELECT source_name, status, row_count, source_refreshed_at, error_message FROM source_manifest WHERE (source_type LIKE '%api%' OR source_name LIKE '%api%') AND (row_count = 0 OR row_count IS NULL)").fetchall()
if rows:
    for r in rows:
        print(f"  ⚠️ {r['source_name']}: rows={r['row_count']}, status={r['status']}, msg={r['error_message']}")
else:
    print("  없음")

print()
print("=" * 80)
print("6. 최종 요약: API별 정상/비정상 판정")
print("=" * 80)

# Determine status for each API
apis = {
    "국세청 사업자등록상태 (nts_batch)": {"manifest": "nts_batch", "table": "company_business_status", "count": nts},
    "인증제품 (smpp_tech_product_api)": {"manifest": "smpp_tech_product_api", "table": "certified_product", "count": cert_api},
    "혁신장터 (pps_innovation_market_api)": {"manifest": "pps_innovation_market_api", "table": None, "count": innov_api},
}

for name, info in apis.items():
    mrow = conn.execute("SELECT status, row_count, source_refreshed_at FROM source_manifest WHERE source_name=?", (info['manifest'],)).fetchone()
    if mrow:
        st = mrow['status']
        rc = mrow['row_count']
        ref = mrow['source_refreshed_at']
        db_count = info['count']
        verdict = "✅ 정상" if st == 'success' and (rc > 0 or db_count > 0) else "⚠️ 확인필요"
        print(f"  {verdict} {name}")
        print(f"       manifest: status={st}, rows={rc}, refreshed={ref}")
        print(f"       DB 실적재: {db_count}건")
    else:
        print(f"  ❌ {name}: source_manifest 미등록")

# MAS API (not yet run against live API)
mas_manifest = conn.execute("SELECT status, row_count, source_refreshed_at FROM source_manifest WHERE source_name LIKE '%mas_api%'").fetchone()
if mas_manifest:
    print(f"  {'✅' if mas_manifest['status']=='success' else '⚠️'} 종합쇼핑몰 MAS API (mas_api_incremental)")
    print(f"       manifest: status={mas_manifest['status']}, rows={mas_manifest['row_count']}")
else:
    print(f"  ⏸️ 종합쇼핑몰 MAS API: 아직 API 증분 수집 미실행 (Excel bootstrap만 완료)")

conn.close()
