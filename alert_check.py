"""
alert_check.py — 수주율 이상 감지 경보 시스템
==============================================
이전 캐시(api_cache_prev.json) vs 현재 캐시(api_cache.json) 비교하여
이상 징후를 감지하고 로그를 남김.

사용: daily_pipeline_sync.py에서 캐시 재생성 후 자동 호출
단독 실행: python alert_check.py
"""
import json, os, sys, datetime

sys.stdout.reconfigure(encoding='utf-8')

CACHE_FILE = 'api_cache.json'
PREV_CACHE_FILE = 'api_cache_prev.json'
ALERT_LOG_DIR = 'alert_log'

# ═══════════════ 감지 기준값 (조정 가능) ═══════════════
THRESHOLD_TOTAL_RATE_DROP = 3.0      # 전체 수주율 급락 기준 (%p)
THRESHOLD_SECTOR_RATE_CHANGE = 5.0   # 분야별 수주율 급변 기준 (%p)
THRESHOLD_LARGE_LEAKAGE_AMT = 20e8   # 대형 유출계약 기준 (20억)
THRESHOLD_PROTECTION_INCREASE = 10   # 보호제도 미적용 증가 기준 (건수)
THRESHOLD_TOTAL_AMT_CHANGE = 0.10    # 전체 발주액 급변 기준 (10%)


def load_json(path):
    """JSON 파일 로드, 없으면 None 반환"""
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def run_alert_check():
    """경보 체크 메인 함수. daily_pipeline_sync.py에서 호출됨."""
    print("\n==================================================")
    print(" 🔔 수주율 이상 감지 경보 시스템")
    print("==================================================\n")

    curr = load_json(CACHE_FILE)
    prev = load_json(PREV_CACHE_FILE)

    if not curr:
        print("  ❌ api_cache.json 파일이 없습니다. 캐시 재생성을 먼저 실행하세요.")
        return []

    if not prev:
        print("  ℹ️  이전 캐시(api_cache_prev.json)가 없습니다. 첫 실행이므로 비교를 스킵합니다.")
        print("     → 다음 실행부터 경보 비교가 활성화됩니다.")
        return []

    alerts = []
    today = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')

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

    # ────────── 3. 대형 유출계약 신규 등장 ──────────
    curr_leakage = curr.get('7_유출계약_주요', [])
    prev_leakage = prev.get('7_유출계약_주요', [])

    # 이전 유출계약의 식별 키 (계약명+기관명)
    prev_keys = set()
    for item in prev_leakage:
        key = (item.get('계약명', ''), item.get('기관', ''))
        prev_keys.add(key)

    new_large = []
    for item in curr_leakage:
        key = (item.get('계약명', ''), item.get('기관', ''))
        amt = item.get('유출액', 0)
        if key not in prev_keys and amt >= THRESHOLD_LARGE_LEAKAGE_AMT:
            new_large.append(item)

    if new_large:
        for item in new_large:
            amt_eok = item.get('유출액', 0) / 1e8
            msg = f"🚨 [경보] 대형 유출계약 신규: \"{item.get('계약명', '')}\" {amt_eok:.0f}억원 ({item.get('기관', '')})"
            alerts.append(('CRITICAL', msg))
            print(f"  {msg}")
    else:
        print(f"  ✅ 정상: 대형 유출계약 신규 없음")

    # ────────── 4. 보호제도 미적용 증가 ──────────
    curr_prot = curr.get('8_보호제도_현황', {})
    prev_prot = prev.get('8_보호제도_현황', {})

    curr_non = curr_prot.get('미적용', 0) if isinstance(curr_prot, dict) else 0
    prev_non = prev_prot.get('미적용', 0) if isinstance(prev_prot, dict) else 0

    # 보호제도 현황이 중첩 구조일 수 있음 — 전체 미적용 건수 합산
    if isinstance(curr_prot, dict) and not isinstance(curr_non, (int, float)):
        curr_non = sum(
            v.get('미적용', 0) for v in curr_prot.values()
            if isinstance(v, dict) and '미적용' in v
        )
    if isinstance(prev_prot, dict) and not isinstance(prev_non, (int, float)):
        prev_non = sum(
            v.get('미적용', 0) for v in prev_prot.values()
            if isinstance(v, dict) and '미적용' in v
        )

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

    # ────────── 결과 요약 ──────────
    print(f"\n{'─'*50}")
    critical = sum(1 for level, _ in alerts if level == 'CRITICAL')
    warning = sum(1 for level, _ in alerts if level == 'WARNING')

    if not alerts:
        print(f"  🟢 결과: 이상 없음 (5개 항목 모두 정상)")
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
            f.write(f"이전 캐시: {prev.get('generated_at', 'N/A')}\n")
            f.write(f"현재 캐시: {curr.get('generated_at', 'N/A')}\n")
            f.write(f"{'='*60}\n\n")
            for level, msg in alerts:
                f.write(f"[{level}] {msg}\n")
            f.write(f"\n{'='*60}\n")
            f.write(f"총 경보: {critical}건, 주의: {warning}건\n")
        print(f"  📄 로그 저장: {log_file}")

    print("==================================================\n")
    return alerts


if __name__ == '__main__':
    alerts = run_alert_check()
    sys.exit(1 if any(level == 'CRITICAL' for level, _ in alerts) else 0)
