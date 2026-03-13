"""
test_integrity.py — 자동 검증 테스트
=====================================
build_api_cache.py 실행 후 반드시 실행하여 데이터 정합성 확인.
core_calc.py 수정 후에도 반드시 실행.
"""
import sqlite3, json, sys, os
sys.stdout.reconfigure(encoding='utf-8')

PASS = 0
FAIL = 0

def check(name, condition, detail=''):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✅ {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f"  ❌ {name}" + (f" — {detail}" if detail else ""))

print("=" * 70)
print("  자동 검증 테스트 (test_integrity.py)")
print("=" * 70)

# ======== 1. 핵심 파일 존재 ========
print("\n[1] 핵심 파일 존재")
for f in ['core_calc.py', 'build_api_cache.py', 'rate_calc_db.py',
          'api_server.py', 'daily_pipeline_sync.py', 'api_cache.json',
          'PROJECT_STATUS.md']:
    check(f, os.path.exists(f))

# ======== 2. core_calc.py 모듈 검증 ========
print("\n[2] core_calc.py 모듈")
import core_calc
import importlib
importlib.reload(core_calc)

required_funcs = ['parse_corp_shares', 'extract_dminstt_codes',
                  'is_non_busan_contract', 'check_busan_restriction',
                  'filter_cnstwk_by_site', 'filter_shopping_by_site', 'process_contract_row',
                  'load_bid_dict', 'load_award_sets']
for fn in required_funcs:
    check(f"함수: {fn}", hasattr(core_calc, fn))

# 시/군 키워드 포함 확인
kw = core_calc.NON_BUSAN_KEYWORDS
check(f"키워드 수 ≥ 80개", len(kw) >= 80, f"{len(kw)}개")
for city in ['포항', '광양', '안동', '김해', '통영', '새만금']:
    check(f"시/군 키워드: {city}", city in kw)

# 부산 예외
exc = core_calc.BUSAN_EXCEPTIONS
check("예외: 대구→해운대구", '해운대구' in exc.get('대구', []))
check("예외: 김해→김해공항", '김해공항' in exc.get('김해', []))
check("예외: 동해→동해선", '동해선' in exc.get('동해', []))

# ======== 3. core_calc import 확인 ========
print("\n[3] 스크립트 → core_calc import")
for script in ['build_api_cache.py', 'rate_calc_db.py']:
    with open(script, 'r', encoding='utf-8') as f:
        src = f.read()
    check(f"{script}: from core_calc import", 'from core_calc import' in src)
    check(f"{script}: process_contract_row", 'process_contract_row' in src)
    check(f"{script}: use_location_filter=True", 'use_location_filter=True' in src)

# ======== 4. DB 검증 ========
print("\n[4] 데이터베이스")
conn_ag = sqlite3.connect('busan_agencies_master.db')
cols = [r[1] for r in conn_ag.execute("PRAGMA table_info(agency_master)").fetchall()]
check("compare_unit 컬럼 존재", 'compare_unit' in cols)
total = conn_ag.execute("SELECT COUNT(*) FROM agency_master").fetchone()[0]
mapped = conn_ag.execute("SELECT COUNT(*) FROM agency_master WHERE compare_unit IS NOT NULL AND compare_unit != ''").fetchone()[0]
check(f"compare_unit 매핑율", mapped == total, f"{mapped}/{total} ({mapped/total*100:.0f}%)")
conn_ag.close()

conn = sqlite3.connect('procurement_contracts.db')
for tbl in ['cnstwk_cntrct', 'servc_cntrct', 'thng_cntrct', 'shopping_cntrct']:
    cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    check(f"{tbl} 존재", cnt > 0, f"{cnt:,}건")

bid_cnt = conn.execute("SELECT COUNT(*) FROM bid_notices_raw").fetchone()[0]
check("bid_notices_raw", bid_cnt > 0, f"{bid_cnt:,}건")

for tbl in ['busan_award_cnstwk', 'busan_award_servc', 'busan_award_thng']:
    cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    check(f"{tbl}", cnt > 0, f"{cnt:,}건")
conn.close()

# ======== 5. api_cache.json 검증 ========
print("\n[5] api_cache.json 캐시 데이터")
with open('api_cache.json', 'r', encoding='utf-8') as f:
    cache = json.load(f)

check("1_전체 키", '1_전체' in cache)
rate = cache.get('1_전체', {}).get('수주율', 0)
check(f"전체 수주율 {rate}%", 40 < rate < 90, "필터 적용된 정상 범위")

check("2_분야별 키", '2_분야별' in cache)
for sector in ['공사', '용역', '물품', '쇼핑몰']:
    check(f"분야: {sector}", sector in cache.get('2_분야별', {}))

check("5_기관랭킹_전체", '5_기관랭킹_전체' in cache)
check("5_기관랭킹_분야별", '5_기관랭킹_분야별' in cache)

# 국토교통부 발주액 (시/군 필터 적용 확인)
for g_data in cache.get('5_기관랭킹_전체', {}).values():
    for item in g_data.get('상위', []) + g_data.get('하위', []):
        if item['비교단위'] == '국토교통부':
            amt = item['발주액'] / 1e8
            check(f"국토교통부 {amt:.0f}억", amt < 1500, "시/군 필터 적용됨 (필터전 6505억)")

# ======== 6. API 서버 엔드포인트 ========
print("\n[6] API 서버 엔드포인트")
with open('api_server.py', 'r', encoding='utf-8') as f:
    api = f.read()
check("/api/summary", '/api/summary' in api)
check("/api/ranking", '/api/ranking' in api)
check("/api/ranking/{sector}", '/api/ranking/{sector}' in api or 'ranking/{sector}' in api)

# ======== 7. PROJECT_STATUS.md 규칙 ========
print("\n[7] PROJECT_STATUS.md 문서")
with open('PROJECT_STATUS.md', 'r', encoding='utf-8') as f:
    doc = f.read()
check("core_calc.py 규칙", 'core_calc.py' in doc)
check("시/군 단위 키워드", '시/군' in doc)
check("필터 적용 순서", '필터 적용 순서' in doc)
check("기관 랭킹 기준액", '기준액' in doc)
check("데이터 흐름", '데이터 흐름' in doc)
check("검증 기준값", '검증 기준값' in doc)

# ======== 결과 ========
print(f"\n{'='*70}")
total_checks = PASS + FAIL
if FAIL == 0:
    print(f"  🎉 결과: {PASS}/{total_checks} 전체 통과!")
else:
    print(f"  ⚠️ 결과: {PASS}/{total_checks} 통과, {FAIL}건 실패")
print(f"{'='*70}")

sys.exit(0 if FAIL == 0 else 1)
