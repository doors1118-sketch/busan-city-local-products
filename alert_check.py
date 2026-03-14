"""
alert_check.py — 수주율 이상 감지 경보 시스템
==============================================
Part A: 이전 캐시 vs 현재 캐시 비교 (사후 분석, 5개 항목)
Part B: 입찰공고 사전 경보 — 부산시 소속기관 기준이하 지역제한 미적용 감지

대상: 부산광역시 및 소속기관만 (자치구, 교육청, 산하기관 등)
     → 현장이 부산인 것이 확실한 기관만 모니터링

사용: daily_pipeline_sync.py에서 캐시 재생성 후 자동 호출
단독 실행: python alert_check.py
"""
import json, os, sys, datetime, sqlite3

sys.stdout.reconfigure(encoding='utf-8')

CACHE_FILE = 'api_cache.json'
PREV_CACHE_FILE = 'api_cache_prev.json'
ALERT_LOG_DIR = 'alert_log'
DB_PATH = 'procurement_contracts.db'
DB_AGENCIES = 'busan_agencies_master.db'

# ═══════════════ 감지 기준값 (조정 가능) ═══════════════
THRESHOLD_TOTAL_RATE_DROP = 3.0      # 전체 수주율 급락 기준 (%p)
THRESHOLD_SECTOR_RATE_CHANGE = 5.0   # 분야별 수주율 급변 기준 (%p)
THRESHOLD_PROTECTION_INCREASE = 10   # 보호제도 미적용 증가 기준 (건수)
THRESHOLD_TOTAL_AMT_CHANGE = 0.10    # 전체 발주액 급변 기준 (10%)

# 대형 유출계약 분야별 차등 기준
THRESHOLD_LEAKAGE_BY_SECTOR = {
    '공사': 50e8,    # 50억
    '용역': 5e8,     # 5억
    '물품': 5e8,     # 5억
    '쇼핑몰': 3e8,   # 3억
}
THRESHOLD_LEAKAGE_DEFAULT = 5e8

# 부산시 소속기관 지역제한 기준액 (추정가격 기준)
BUSAN_CITY_THRESHOLDS = {
    '종합': 100e8,   # 종합공사 ≤100억
    '전문': 10e8,    # 전문공사 ≤10억
    '용역': 3.3e8,   # 용역 ≤3.3억
}
# 전문공사 판별 키워드
SPECIALIZED_KEYWORDS = ['전기', '통신', '소방', '기계설비', '기계공사', '정보통신']

# 모니터링 대상 기관 그룹 (cate_lrg)
TARGET_GROUP = '부산광역시 및 소속기관'


def load_json(path):
    """JSON 파일 로드, 없으면 None 반환"""
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_busan_city_agencies():
    """부산시 소속기관 코드 목록 로드"""
    if not os.path.exists(DB_AGENCIES):
        return set()
    conn = sqlite3.connect(DB_AGENCIES)
    codes = set()
    for row in conn.execute(
        "SELECT dminsttCd FROM agency_master WHERE cate_lrg = ?",
        (TARGET_GROUP,)
    ).fetchall():
        codes.add(str(row[0]).strip())
    conn.close()
    return codes


def check_bid_notices_protection(target_date=None):
    """[Part B-1] 공고 단계 사전 경보
    부산시 소속기관의 기준이하 공고 중 지역제한경쟁이 아닌 건 감지.
    """
    if not os.path.exists(DB_PATH):
        return []

    busan_agencies = load_busan_city_agencies()
    if not busan_agencies:
        return []

    if target_date is None:
        target_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')

    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT bidNtceNo, bidNtceNm, dminsttCd, dminsttNm, presmptPrce,
               cntrctCnclsMthdNm, prtcptLmtRgnNm, sector, mainCnsttyNm
        FROM bid_notices_price
        WHERE bidNtceDt LIKE ? AND sector IN ('공사', '용역')
    """
    rows = conn.execute(query, (f'{target_date}%',)).fetchall()
    conn.close()

    if not rows:
        return []

    suspects = []
    for row in rows:
        ntce_no, ntce_nm, dm_cd, dm_nm, price_str, method, rgn_lmt, sector, main_type = row

        # 부산시 소속기관만 대상
        if str(dm_cd).strip() not in busan_agencies:
            continue

        # 추정가격 파싱
        try:
            price = float(price_str) if price_str else 0
        except (ValueError, TypeError):
            price = 0
        if price <= 0:
            continue

        # 종합/전문 판별
        if sector == '공사':
            is_specialized = False
            for kw in SPECIALIZED_KEYWORDS:
                if (main_type and kw in str(main_type)) or (ntce_nm and kw in str(ntce_nm)):
                    is_specialized = True
                    break
            ctype = '전문' if is_specialized else '종합'
            limit = BUSAN_CITY_THRESHOLDS[ctype]
        else:
            ctype = '용역'
            limit = BUSAN_CITY_THRESHOLDS['용역']

        # 기준이하인지 확인
        if price > limit:
            continue

        # 지역제한경쟁인지 확인
        rgn_lmt_str = str(rgn_lmt).strip() if rgn_lmt else ''
        method_str = str(method).strip() if method else ''
        has_busan_restriction = '부산' in rgn_lmt_str or '26' in rgn_lmt_str
        is_restricted_bid = '제한' in method_str

        # 기준이하인데 지역제한경쟁이 아님 → 미적용 의심
        if not (has_busan_restriction and is_restricted_bid):
            suspects.append({
                '공고번호': ntce_no,
                '공고명': ntce_nm,
                '수요기관': dm_nm,
                '분야': sector,
                '구분': ctype,
                '추정가격': price / 1e8,
                '기준액': limit / 1e8,
                '계약방식': method_str,
                '지역제한': rgn_lmt_str or '없음',
            })

    return suspects


def run_alert_check():
    """경보 체크 메인 함수. daily_pipeline_sync.py에서 호출됨."""
    print("\n==================================================")
    print(" 🔔 수주율 이상 감지 경보 시스템")
    print(f"    대상: {TARGET_GROUP}")
    print("==================================================\n")

    curr = load_json(CACHE_FILE)
    prev = load_json(PREV_CACHE_FILE)

    alerts = []
    today = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')

    # ══════════════════════════════════════════════════════
    # Part A: 캐시 비교 (사후 분석)
    # ══════════════════════════════════════════════════════

    if not curr:
        print("  ❌ api_cache.json 없음. 캐시 재생성을 먼저 실행하세요.")
    elif not prev:
        print("  ℹ️  이전 캐시 없음 — 첫 실행이므로 캐시 비교 스킵.")
    else:
        # ────────── 1. 전체 수주율 급락 ──────────
        curr_rate = curr.get('1_전체', {}).get('수주율', 0)
        prev_rate = prev.get('1_전체', {}).get('수주율', 0)

        if prev_rate > 0:
            rate_diff = curr_rate - prev_rate
            if rate_diff < -THRESHOLD_TOTAL_RATE_DROP:
                msg = f"🚨 [경보] 전체 수주율 급락: {prev_rate}% → {curr_rate}% ({rate_diff:+.1f}%p)"
                alerts.append(('CRITICAL', msg))
                print(f"  {msg}")
            else:
                print(f"  ✅ 정상: 전체 수주율 {prev_rate}% → {curr_rate}% ({rate_diff:+.1f}%p)")

        # ────────── 2. 분야별 수주율 급변 ──────────
        curr_sectors = curr.get('2_분야별', {})
        prev_sectors = prev.get('2_분야별', {})
        sector_alert = False

        for sector in ['공사', '용역', '물품', '쇼핑몰']:
            c_rate = curr_sectors.get(sector, {}).get('수주율', 0)
            p_rate = prev_sectors.get(sector, {}).get('수주율', 0)
            if p_rate > 0:
                diff = c_rate - p_rate
                if abs(diff) >= THRESHOLD_SECTOR_RATE_CHANGE:
                    direction = "급락" if diff < 0 else "급등"
                    msg = f"⚠️  [주의] {sector} 수주율 {direction}: {p_rate}% → {c_rate}% ({diff:+.1f}%p)"
                    alerts.append(('WARNING', msg))
                    print(f"  {msg}")
                    sector_alert = True

        if not sector_alert:
            print(f"  ✅ 정상: 분야별 수주율 변동 정상 범위")

        # ────────── 3. 대형 유출계약 신규 (분야별 차등) ──────────
        curr_leakage = curr.get('7_유출계약_주요', [])
        prev_leakage = prev.get('7_유출계약_주요', [])

        prev_keys = set()
        for item in prev_leakage:
            prev_keys.add((item.get('계약명', ''), item.get('기관', '')))

        new_large = []
        for item in curr_leakage:
            key = (item.get('계약명', ''), item.get('기관', ''))
            amt = item.get('유출액', 0)
            sector = item.get('분야', '')
            threshold = THRESHOLD_LEAKAGE_BY_SECTOR.get(sector, THRESHOLD_LEAKAGE_DEFAULT)
            if key not in prev_keys and amt >= threshold:
                new_large.append(item)

        if new_large:
            for item in new_large:
                amt_eok = item.get('유출액', 0) / 1e8
                sector = item.get('분야', '?')
                msg = f"🚨 [경보] 대형 유출계약 [{sector}]: \"{item.get('계약명', '')}\" {amt_eok:.0f}억 ({item.get('기관', '')})"
                alerts.append(('CRITICAL', msg))
                print(f"  {msg}")
        else:
            print(f"  ✅ 정상: 대형 유출계약 신규 없음")

        # ────────── 4. 보호제도 미적용 증가 ──────────
        curr_prot = curr.get('8_보호제도_현황', {})
        prev_prot = prev.get('8_보호제도_현황', {})

        curr_non = curr_prot.get('미적용', 0) if isinstance(curr_prot, dict) else 0
        prev_non = prev_prot.get('미적용', 0) if isinstance(prev_prot, dict) else 0

        if isinstance(curr_prot, dict) and not isinstance(curr_non, (int, float)):
            curr_non = sum(v.get('미적용', 0) for v in curr_prot.values()
                           if isinstance(v, dict) and '미적용' in v)
        if isinstance(prev_prot, dict) and not isinstance(prev_non, (int, float)):
            prev_non = sum(v.get('미적용', 0) for v in prev_prot.values()
                           if isinstance(v, dict) and '미적용' in v)

        if prev_non > 0:
            prot_diff = curr_non - prev_non
            if prot_diff >= THRESHOLD_PROTECTION_INCREASE:
                msg = f"⚠️  [주의] 보호제도 미적용 증가: {prev_non:,}건 → {curr_non:,}건 (+{prot_diff}건)"
                alerts.append(('WARNING', msg))
                print(f"  {msg}")
            else:
                print(f"  ✅ 정상: 보호제도 미적용 {prev_non:,}건 → {curr_non:,}건 ({prot_diff:+d}건)")

        # ────────── 5. 데이터 규모 이상 ──────────
        curr_amt = curr.get('1_전체', {}).get('발주액', 0)
        prev_amt = prev.get('1_전체', {}).get('발주액', 0)

        if prev_amt > 0:
            amt_ratio = abs(curr_amt - prev_amt) / prev_amt
            if amt_ratio >= THRESHOLD_TOTAL_AMT_CHANGE:
                direction = "증가" if curr_amt > prev_amt else "감소"
                msg = f"🚨 [경보] 전체 발주액 {direction}: {prev_amt/1e8:,.0f}억 → {curr_amt/1e8:,.0f}억 ({amt_ratio*100:.1f}%)"
                alerts.append(('CRITICAL', msg))
                print(f"  {msg}")
            else:
                print(f"  ✅ 정상: 전체 발주액 변동 {amt_ratio*100:.1f}%")

    # ══════════════════════════════════════════════════════
    # Part B: 입찰공고 사전 경보 (부산시 소속기관 한정)
    # ══════════════════════════════════════════════════════
    print(f"\n  {'─'*46}")
    print(f"  📋 공고 단계 사전 경보 (부산시 소속기관)")
    print(f"     기준이하 공고 중 지역제한경쟁 미적용 감지")
    print(f"  {'─'*46}")

    suspects = check_bid_notices_protection()

    if suspects:
        print(f"  ⚠️  지역제한 미적용 의심: {len(suspects)}건")
        suspects.sort(key=lambda x: x['추정가격'], reverse=True)
        for i, s in enumerate(suspects[:5]):
            msg = (f"⚠️  [{s['분야']}/{s['구분']}] \"{s['공고명'][:30]}\" "
                   f"{s['추정가격']:.1f}억 (기준 ≤{s['기준액']:.0f}억) "
                   f"— {s['수요기관']} [{s['계약방식']}]")
            alerts.append(('WARNING', msg))
            print(f"  {msg}")
        if len(suspects) > 5:
            print(f"     ... 외 {len(suspects) - 5}건")
    else:
        print(f"  ✅ 정상: 지역제한 미적용 의심 공고 없음")

    # ────────── 결과 요약 ──────────
    print(f"\n{'─'*50}")
    critical = sum(1 for level, _ in alerts if level == 'CRITICAL')
    warning = sum(1 for level, _ in alerts if level == 'WARNING')

    if not alerts:
        print(f"  🟢 결과: 이상 없음 (전체 정상)")
    else:
        print(f"  🔴 결과: 경보 {critical}건 + 주의 {warning}건 발생!")

    # ────────── 로그 저장 ──────────
    if alerts:
        os.makedirs(ALERT_LOG_DIR, exist_ok=True)
        log_date = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = os.path.join(ALERT_LOG_DIR, f'alert_{log_date}.log')
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(f"부산 조달 모니터링 경보 로그\n")
            f.write(f"생성시각: {today}\n")
            f.write(f"감시대상: {TARGET_GROUP}\n")
            if prev:
                f.write(f"이전 캐시: {prev.get('generated_at', 'N/A')}\n")
            if curr:
                f.write(f"현재 캐시: {curr.get('generated_at', 'N/A')}\n")
            f.write(f"{'='*60}\n\n")
            for level, msg in alerts:
                f.write(f"[{level}] {msg}\n")
            if suspects:
                f.write(f"\n--- 공고 사전 경보 상세 ({len(suspects)}건) ---\n")
                for s in suspects:
                    f.write(f"  [{s['분야']}/{s['구분']}] {s['공고번호']} "
                            f"\"{s['공고명']}\" {s['추정가격']:.1f}억 "
                            f"({s['수요기관']}, {s['계약방식']}, 지역제한: {s['지역제한']})\n")
            f.write(f"\n{'='*60}\n")
            f.write(f"총 경보: {critical}건, 주의: {warning}건\n")
        print(f"  📄 로그 저장: {log_file}")

    print("==================================================\n")
    return alerts


if __name__ == '__main__':
    alerts = run_alert_check()
    sys.exit(1 if any(level == 'CRITICAL' for level, _ in alerts) else 0)
