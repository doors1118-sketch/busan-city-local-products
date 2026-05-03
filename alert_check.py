"""
alert_check.py — 수주율 이상 감지 경보 시스템
==============================================
Part A: 이전 캐시 vs 현재 캐시 비교 (사후 분석, 5개 항목)
Part B-1: 공고 단계 사전 경보 — 지역제한경쟁 + 의무공동도급 미적용 감지
Part B-2: 계약 단계 사후 경보 — 외지업체 지분 60% 초과 (의무공동도급 위반)

대상: 부산광역시 및 소속기관만 (자치구, 교육청, 산하기관 등)
단독 실행: python alert_check.py
"""
import json, os, sys, datetime, sqlite3
import urllib.request, urllib.parse
import hashlib, hmac, base64, time

CONFIG_FILE = 'alert_config.json'

sys.stdout.reconfigure(encoding='utf-8')

CACHE_FILE = 'api_cache.json'
PREV_CACHE_FILE = 'api_cache_prev.json'
ALERT_LOG_DIR = 'alert_log'
DB_PATH = 'procurement_contracts.db'
DB_AGENCIES = 'busan_agencies_master.db'
DB_COMPANIES = 'busan_companies_master.db'

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

# 공사 지역제한 기준액 (부산시 소속기관)
BUSAN_CITY_THRESHOLDS = {
    '종합': 100e8,   # ≤100억
    '전문': 10e8,    # ≤10억
    '용역': 3.3e8,   # ≤3.3억
}
SPECIALIZED_KEYWORDS = ['전기', '통신', '소방', '기계설비', '기계공사', '정보통신']

# 의무공동도급: 외지업체 지분 상한 (60% 이상이면 위반)
MAX_NON_LOCAL_SHARE = 60.0

# 모니터링 대상 기관 그룹 (cate_lrg)
TARGET_GROUP = '부산광역시 및 소속기관'


def load_json(path):
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_config():
    """알림 설정 로드"""
    if not os.path.exists(CONFIG_FILE):
        return {}
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def send_telegram(message, config):
    """텔레그램 봇으로 메시지 발송"""
    tg = config.get('telegram', {})
    if not tg.get('enabled'):
        return
    token = tg.get('bot_token', '')
    chat_id = tg.get('chat_id', '')
    if not token or not chat_id or '여기에' in token:
        return

    try:
        url = f'https://api.telegram.org/bot{token}/sendMessage'
        data = urllib.parse.urlencode({
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'HTML',
        }).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        with urllib.request.urlopen(req, timeout=10) as res:
            result = json.loads(res.read().decode('utf-8'))
            if result.get('ok'):
                print(f"  📱 텔레그램 발송 완료")
            else:
                print(f"  ⚠️ 텔레그램 발송 실패: {result}")
    except Exception as e:
        print(f"  ⚠️ 텔레그램 발송 오류: {e}")


def send_gmail(subject, body, config):
    """Gmail SMTP로 이메일 발송"""
    import smtplib
    from email.mime.text import MIMEText

    email_cfg = config.get('gmail', {})
    if not email_cfg.get('enabled'):
        return
    sender = email_cfg.get('address', '')
    app_password = email_cfg.get('app_password', '')
    recipients = email_cfg.get('recipients', [])
    if not sender or not app_password or '여기에' in app_password or not recipients:
        return

    try:
        msg = MIMEText(body, 'html', 'utf-8')
        msg['Subject'] = subject
        msg['From'] = f'부산 조달 경보 <{sender}>'
        msg['To'] = ', '.join(recipients)

        with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
            smtp.starttls()
            smtp.login(sender, app_password)
            smtp.send_message(msg)
        print(f"  📧 이메일 발송 완료 → {', '.join(recipients)}")
    except Exception as e:
        print(f"  ⚠️ 이메일 발송 오류: {e}")


def send_ncp_sms(message, config):
    """네이버 클라우드 SENS API로 SMS 발송"""
    sms_cfg = config.get('ncp_sms', {})
    if not sms_cfg.get('enabled'):
        return
    
    access_key = sms_cfg.get('access_key', '')
    secret_key = sms_cfg.get('secret_key', '')
    service_id = sms_cfg.get('service_id', '')
    from_number = sms_cfg.get('from_number', '')
    recipients = sms_cfg.get('recipients', [])
    
    if not all([access_key, secret_key, service_id, from_number, recipients]):
        print("  ⚠️ NCP SMS 설정 불완전")
        return
    
    # LMS (장문) 사용 (80byte 초과 시)
    msg_bytes = message.encode('utf-8')
    msg_type = 'LMS' if len(msg_bytes) > 80 else 'SMS'
    
    # 서명 생성
    timestamp = str(int(time.time() * 1000))
    uri = f'/sms/v2/services/{service_id}/messages'
    
    sign_str = f"POST {uri}\n{timestamp}\n{access_key}"
    signature = base64.b64encode(
        hmac.new(secret_key.encode('utf-8'), sign_str.encode('utf-8'), hashlib.sha256).digest()
    ).decode('utf-8')
    
    # 메시지 본문
    body = {
        "type": msg_type,
        "from": from_number,
        "content": message,
        "messages": [{"to": r.replace('-','')} for r in recipients],
    }
    if msg_type == 'LMS':
        body["subject"] = "[부산 조달 경보]"
    
    try:
        url = f"https://sens.apigw.ntruss.com{uri}"
        data = json.dumps(body).encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        req.add_header('Content-Type', 'application/json; charset=utf-8')
        req.add_header('x-ncp-apigw-timestamp', timestamp)
        req.add_header('x-ncp-iam-access-key', access_key)
        req.add_header('x-ncp-apigw-signature-v2', signature)
        
        with urllib.request.urlopen(req, timeout=30) as res:
            result = json.loads(res.read().decode('utf-8'))
            status = result.get('statusCode', '')
            if status == '202':
                print(f"  📱 SMS 발송 완료 → {', '.join(recipients)} ({msg_type})")
            else:
                print(f"  ⚠️ SMS 발송 실패: {result}")
    except Exception as e:
        print(f"  ⚠️ SMS 발송 오류: {e}")


def send_notifications(alerts, suspects, violations, config):
    """경보 발생 시 텔레그램 + 이메일 발송"""
    if not alerts:
        return

    critical = sum(1 for level, _ in alerts if level == 'CRITICAL')
    warning = sum(1 for level, _ in alerts if level == 'WARNING')
    today = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')

    # 텔레그램 메시지 (간결하게)
    tg_lines = [f'🔔 <b>부산 조달 경보</b> ({today})']
    tg_lines.append(f'경보 {critical}건 + 주의 {warning}건\n')
    for level, msg in alerts[:10]:  # 최대 10건
        tg_lines.append(msg)
    if len(alerts) > 10:
        tg_lines.append(f'... 외 {len(alerts)-10}건')
    send_telegram('\n'.join(tg_lines), config)

    # 이메일 (상세하게)
    subject = f'[부산 조달 경보] 경보 {critical}건 + 주의 {warning}건 ({today})'
    body_lines = [f'<h2>🔔 부산 조달 모니터링 경보</h2>']
    body_lines.append(f'<p>생성시각: {today}</p>')
    body_lines.append(f'<p>감시대상: {TARGET_GROUP}</p>')
    body_lines.append('<hr>')
    for level, msg in alerts:
        color = '#dc3545' if level == 'CRITICAL' else '#ffc107'
        body_lines.append(f'<p style="color:{color}">{msg}</p>')
    if suspects:
        body_lines.append(f'<h3>공고 사전 경보 ({len(suspects)}건)</h3><ul>')
        for s in suspects[:10]:
            body_lines.append(f'<li>[{s["분야"]}] {s["공고명"]} {s["추정가격"]:.1f}억 — {s["수요기관"]}</li>')
        body_lines.append('</ul>')
    if violations:
        body_lines.append(f'<h3>외지업체 지분 초과 ({len(violations)}건)</h3><ul>')
        for v in violations[:10]:
            body_lines.append(f'<li>{v["외지업체"]} {v["외지지분"]}% ({v["계약금액"]:.1f}억) — {v["수요기관"]}</li>')
        body_lines.append('</ul>')
    send_gmail(subject, '\n'.join(body_lines), config)

    # SMS (LMS: 상세 포함, 최대 2000자)
    sms_lines = [f'[부산 조달 경보] {today}']
    sms_lines.append(f'경보 {critical}건 / 주의 {warning}건')
    sms_lines.append('')
    for level, msg in alerts[:5]:
        clean = msg.replace('🚨 ', '').replace('⚠️  ', '').replace('⚠️ ', '')
        sms_lines.append(clean[:70])
    if suspects:
        sms_lines.append(f'\n[보호제도 미적용 의심] {len(suspects)}건')
        for i, s in enumerate(suspects[:10], 1):
            sms_lines.append(f'{i}. [{s["분야"]}/{s["구분"]}] "{s["공고명"][:25]}" {s["추정가격"]:.1f}억 - {s["수요기관"]}')
        if len(suspects) > 10:
            sms_lines.append(f'  ... 외 {len(suspects)-10}건')
    if violations:
        sms_lines.append(f'\n[외지업체 지분초과] {len(violations)}건')
        for i, v in enumerate(violations[:5], 1):
            sms_lines.append(f'{i}. {v["외지업체"][:15]} {v["외지지분"]}% ({v["계약금액"]:.1f}억) - {v["수요기관"]}')
    send_ncp_sms('\n'.join(sms_lines), config)


def load_busan_city_agencies():
    """부산시 소속기관 수요기관코드 set"""
    if not os.path.exists(DB_AGENCIES):
        return set()
    conn = sqlite3.connect(DB_AGENCIES)
    codes = set(str(r[0]).strip() for r in conn.execute(
        "SELECT dminsttCd FROM agency_master WHERE cate_lrg = ?", (TARGET_GROUP,)
    ).fetchall())
    conn.close()
    return codes


def load_busan_biznos():
    """부산 업체 사업자번호 set"""
    if not os.path.exists(DB_COMPANIES):
        return set()
    conn = sqlite3.connect(DB_COMPANIES)
    biznos = set(str(r[0]).strip() for r in conn.execute(
        "SELECT bizno FROM company_master"
    ).fetchall())
    conn.close()
    return biznos


def parse_corp_shares(cl):
    """corpList 문자열에서 [(사업자번호, 지분율)] 추출 (core_calc.py와 동일 로직)"""
    biz_list = []
    cl = str(cl or '')
    if not cl or cl in ('nan', 'None', ''):
        return biz_list
    for chunk in cl.split('[')[1:]:
        chunk = chunk.split(']')[0]
        parts = chunk.split('^')
        if len(parts) >= 10:
            bno = str(parts[9]).replace('-', '').strip()
            try:
                share = float(parts[6]) if parts[6].strip() else 0.0
            except:
                share = 0.0
            biz_list.append([bno, share])
    if biz_list:
        tot = sum(s for _, s in biz_list)
        if tot == 0:
            biz_list = [[b, 100.0 / len(biz_list)] for b, _ in biz_list]
        elif tot > 100.1:
            biz_list = [[b, s / tot * 100] for b, s in biz_list]
    return biz_list


def check_bid_notices_protection(target_date=None):
    """[Part B-1] 공고 단계: 기준이하인데 지역제한경쟁 or 의무공동도급 미적용"""
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
               cntrctCnclsMthdNm, prtcptLmtRgnNm, sector, mainCnsttyNm,
               rgnDutyJntcontrctYn
        FROM bid_notices_price
        WHERE bidNtceDt LIKE ? AND sector IN ('공사', '용역')
    """
    rows = conn.execute(query, (f'{target_date}%',)).fetchall()
    conn.close()

    if not rows:
        return []

    suspects = []
    for row in rows:
        (ntce_no, ntce_nm, dm_cd, dm_nm, price_str, method,
         rgn_lmt, sector, main_type, jnt_yn) = row

        if str(dm_cd).strip() not in busan_agencies:
            continue

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

        # 기준이하 확인
        if price > limit:
            continue

        # 지역제한경쟁 적용 여부
        rgn_lmt_str = str(rgn_lmt).strip() if rgn_lmt else ''
        method_str = str(method).strip() if method else ''
        has_busan_restriction = '부산' in rgn_lmt_str or '26' in rgn_lmt_str
        is_restricted = '제한' in method_str

        # 의무공동도급 적용 여부 (공사만)
        jnt_applied = str(jnt_yn).strip().upper() == 'Y' if jnt_yn else False

        # 기준이하인데 지역제한경쟁도 아니고 의무공동도급도 아님
        if not (has_busan_restriction and is_restricted) and not jnt_applied:
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
                '의무공동도급': '적용' if jnt_applied else '미적용',
            })

    return suspects


def check_construction_share_violation(target_date=None):
    """[Part B-2] 계약 단계: 부산시 소속기관 공사에서 외지업체 지분 60% 초과 건"""
    if not os.path.exists(DB_PATH):
        return []

    busan_agencies = load_busan_city_agencies()
    busan_biznos = load_busan_biznos()
    if not busan_agencies or not busan_biznos:
        return []

    if target_date is None:
        target_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')

    conn = sqlite3.connect(DB_PATH)
    # dminsttCd, dminsttNm_req 사용 (파싱된 수요기관 코드)
    query = """
        SELECT dcsnCntrctNo, untyCntrctNo, cmmnCntrctYn, corpList,
               totCntrctAmt, cntrctCnclsDate, dminsttCd, dminsttNm_req
        FROM cnstwk_cntrct
        WHERE cntrctCnclsDate LIKE ?
    """
    rows = conn.execute(query, (f'{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}%',)).fetchall()
    conn.close()

    if not rows:
        return []

    violations = []
    for row in rows:
        (dcsn_no, unty_no, jnt_yn, corp_list, amt_str,
         cntrct_date, dm_cd, dm_nm) = row

        # 부산시 소속기관만
        if str(dm_cd).strip() not in busan_agencies:
            continue

        # 공동도급인 경우만 (단독은 해당없음)
        if str(jnt_yn).strip().upper() != 'Y':
            continue

        # 지분 파싱
        shares = parse_corp_shares(corp_list)
        if not shares:
            continue

        # 외지업체 지분 합산
        non_local_share = 0.0
        non_local_names = []
        for bno, share in shares:
            if bno and bno not in busan_biznos:
                non_local_share += share
                # 업체명 추출 (corpList에서)
                for chunk in str(corp_list).split('[')[1:]:
                    parts = chunk.split(']')[0].split('^')
                    if len(parts) >= 10 and str(parts[9]).replace('-','').strip() == bno:
                        non_local_names.append(parts[3])
                        break

        # 외지업체 지분 60% 초과
        if non_local_share > MAX_NON_LOCAL_SHARE:
            try:
                amt = float(amt_str) if amt_str else 0
            except:
                amt = 0
            violations.append({
                '계약번호': dcsn_no or unty_no,
                '수요기관': dm_nm or '',
                '계약금액': amt / 1e8,
                '외지업체': ', '.join(non_local_names),
                '외지지분': round(non_local_share, 1),
                '계약일': cntrct_date or '',
            })

    return violations


def check_pipeline_sync():
    """파이프라인 수집 실패 감지: sync_log 테이블에서 전일 수집 기록 확인"""
    alerts = []
    if not os.path.exists(DB_PATH):
        return alerts
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("CREATE TABLE IF NOT EXISTS sync_log (sync_date TEXT PRIMARY KEY, completed_at TEXT)")
        # 어제 날짜 (평일 기준 — 주말이면 금요일)
        now = datetime.datetime.now()
        yesterday = now - datetime.timedelta(days=1)
        # 주말 보정: 일요일(6) → 금요일, 토요일(5) → 금요일
        if yesterday.weekday() == 6:  # 일요일
            yesterday = now - datetime.timedelta(days=2)
        elif yesterday.weekday() == 5:  # 토요일
            yesterday = now - datetime.timedelta(days=1)
        target_date = yesterday.strftime('%Y%m%d')

        row = conn.execute("SELECT sync_date, completed_at FROM sync_log WHERE sync_date = ?",
                           (target_date,)).fetchone()

        # 최근 수집 성공 날짜도 확인
        last_row = conn.execute("SELECT sync_date, completed_at FROM sync_log ORDER BY sync_date DESC LIMIT 1").fetchone()
        last_date = last_row[0] if last_row else 'N/A'
        last_completed = last_row[1] if last_row else 'N/A'

        conn.close()

        if not row:
            # 미수집 일수 계산
            if last_row:
                last_dt = datetime.datetime.strptime(last_date, '%Y%m%d')
                gap_days = (now - last_dt).days
            else:
                gap_days = -1
            msg = (f"🚨 [경보] 일일 데이터 수집 실패: {target_date} 미수집 "
                   f"(마지막 성공: {last_date}, {gap_days}일 경과)")
            alerts.append(('CRITICAL', msg))
            print(f"  {msg}")
        else:
            print(f"  ✅ 정상: 데이터 수집 {target_date} 완료 ({row[1]})")
    except Exception as e:
        msg = f"⚠️ [주의] 수집 상태 확인 오류: {e}"
        alerts.append(('WARNING', msg))
        print(f"  {msg}")
    return alerts


def check_chatbot_pipeline_sync():
    """챗봇 DB 파이프라인 수집 실패 감지: chatbot_company.db의 etl_job_log 테이블 확인"""
    alerts = []
    chatbot_db = 'chatbot_company.db'
    if not os.path.exists(chatbot_db):
        return alerts
        
    try:
        conn = sqlite3.connect(chatbot_db)
        
        # 오늘 날짜
        today_str = datetime.datetime.now().strftime('%Y-%m-%d')
        
        # 필수로 실행되어야 하는 핵심 작업 목록
        essential_jobs = [
            'bootstrap_master_data',
            'mas_api_incremental',
            'certified_product_api_incremental'
        ]
        
        for job in essential_jobs:
            # 가장 최근 로그 조회 (오늘 발생한 에러인지 우선 확인)
            row = conn.execute("""
                SELECT status, error_message, started_at 
                FROM etl_job_log 
                WHERE job_name LIKE ? 
                ORDER BY started_at DESC LIMIT 1
            """, (f"%{job}%",)).fetchone()
            
            if not row:
                # 로그가 아예 없으면 (아직 실행된 적 없음)
                msg = f"⚠️ [주의] 챗봇 DB 적재 누락: {job} 실행 이력 없음"
                alerts.append(('WARNING', msg))
                print(f"  {msg}")
                continue
                
            status, err_msg, started_at = row
            
            # 오늘 실행되었는지 확인
            is_today = started_at.startswith(today_str)
            
            if is_today and status == 'failed':
                msg = f"🚨 [경보] 챗봇 DB 적재 실패: {job} (사유: {err_msg})"
                alerts.append(('CRITICAL', msg))
                print(f"  {msg}")
            elif not is_today:
                msg = f"🚨 [경보] 챗봇 DB 적재 지연: {job} 오늘 미실행 (최근: {started_at})"
                alerts.append(('CRITICAL', msg))
                print(f"  {msg}")
            else:
                print(f"  ✅ 정상: 챗봇 DB 적재 {job} 완료 ({started_at})")
                
        conn.close()
    except Exception as e:
        msg = f"⚠️ [주의] 챗봇 수집 상태 확인 오류: {e}"
        alerts.append(('WARNING', msg))
        print(f"  {msg}")
        
    return alerts


def run_alert_check():
    """경보 체크 메인 함수"""
    print("\n==================================================")
    print(" 🔔 수주율 이상 감지 경보 시스템")
    print(f"    대상: {TARGET_GROUP}")
    print("==================================================\n")

    curr = load_json(CACHE_FILE)
    prev = load_json(PREV_CACHE_FILE)

    alerts = []
    today = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')

    # ══════════════════════════════════════════════════════
    # Part A-0: 파이프라인 수집 실패 감지
    # ══════════════════════════════════════════════════════
    print(f"  {'─'*46}")
    print(f"  🔄 [수집 상태] 일일 파이프라인 적재 확인")
    print(f"  {'─'*46}")
    sync_alerts = check_pipeline_sync()
    alerts.extend(sync_alerts)
    
    chatbot_alerts = check_chatbot_pipeline_sync()
    alerts.extend(chatbot_alerts)

    # ══════════════════════════════════════════════════════
    # Part A: 캐시 비교 (사후 분석)
    # ══════════════════════════════════════════════════════

    if not curr:
        print("  ❌ api_cache.json 없음.")
    elif not prev:
        print("  ℹ️  이전 캐시 없음 — 캐시 비교 스킵.")
    else:
        # 1. 전체 수주율 급락
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

        # 2. 분야별 수주율 급변
        sector_alert = False
        for sector in ['공사', '용역', '물품', '쇼핑몰']:
            c_rate = curr.get('2_분야별', {}).get(sector, {}).get('수주율', 0)
            p_rate = prev.get('2_분야별', {}).get(sector, {}).get('수주율', 0)
            if p_rate > 0:
                diff = c_rate - p_rate
                if abs(diff) >= THRESHOLD_SECTOR_RATE_CHANGE:
                    d = "급락" if diff < 0 else "급등"
                    msg = f"⚠️  [주의] {sector} 수주율 {d}: {p_rate}% → {c_rate}% ({diff:+.1f}%p)"
                    alerts.append(('WARNING', msg))
                    print(f"  {msg}")
                    sector_alert = True
        if not sector_alert:
            print(f"  ✅ 정상: 분야별 수주율 변동 정상 범위")

        # 3. 대형 유출계약 신규 (분야별 차등)
        prev_keys = set((i.get('계약명',''), i.get('기관','')) for i in prev.get('7_유출계약_주요', []))
        new_large = []
        for item in curr.get('7_유출계약_주요', []):
            key = (item.get('계약명',''), item.get('기관',''))
            amt = item.get('유출액', 0)
            threshold = THRESHOLD_LEAKAGE_BY_SECTOR.get(item.get('분야',''), THRESHOLD_LEAKAGE_DEFAULT)
            if key not in prev_keys and amt >= threshold:
                new_large.append(item)
        if new_large:
            for item in new_large:
                msg = f"🚨 [경보] 대형 유출 [{item.get('분야','?')}]: \"{item.get('계약명','')}\" {item.get('유출액',0)/1e8:.0f}억 ({item.get('기관','')})"
                alerts.append(('CRITICAL', msg))
                print(f"  {msg}")
        else:
            print(f"  ✅ 정상: 대형 유출계약 신규 없음")

        # 4. 보호제도 미적용 증가
        curr_non = curr.get('8_보호제도_현황', {}).get('미적용', 0)
        prev_non = prev.get('8_보호제도_현황', {}).get('미적용', 0)
        if isinstance(curr_non, dict):
            curr_non = sum(v.get('미적용', 0) for v in curr.get('8_보호제도_현황', {}).values() if isinstance(v, dict) and '미적용' in v)
        if isinstance(prev_non, dict):
            prev_non = sum(v.get('미적용', 0) for v in prev.get('8_보호제도_현황', {}).values() if isinstance(v, dict) and '미적용' in v)
        if prev_non > 0:
            prot_diff = curr_non - prev_non
            if prot_diff >= THRESHOLD_PROTECTION_INCREASE:
                msg = f"⚠️  [주의] 보호제도 미적용 증가: {prev_non:,}건 → {curr_non:,}건 (+{prot_diff}건)"
                alerts.append(('WARNING', msg))
                print(f"  {msg}")
            else:
                print(f"  ✅ 정상: 보호제도 미적용 {prev_non:,}건 → {curr_non:,}건 ({prot_diff:+d}건)")

        # 5. 데이터 규모 이상
        curr_amt = curr.get('1_전체', {}).get('발주액', 0)
        prev_amt = prev.get('1_전체', {}).get('발주액', 0)
        if prev_amt > 0:
            amt_ratio = abs(curr_amt - prev_amt) / prev_amt
            if amt_ratio >= THRESHOLD_TOTAL_AMT_CHANGE:
                d = "증가" if curr_amt > prev_amt else "감소"
                msg = f"🚨 [경보] 전체 발주액 {d}: {prev_amt/1e8:,.0f}억 → {curr_amt/1e8:,.0f}억 ({amt_ratio*100:.1f}%)"
                alerts.append(('CRITICAL', msg))
                print(f"  {msg}")
            else:
                print(f"  ✅ 정상: 전체 발주액 변동 {amt_ratio*100:.1f}%")

    # ══════════════════════════════════════════════════════
    # Part B-0: 사전규격 대형 건 알림 + 보호제도 대상 요약
    # ══════════════════════════════════════════════════════
    prespec_alerts = []
    print(f"\n  {'─'*46}")
    print(f"  📋 [사전규격] 대형 건 및 보호제도 대상 모니터링")
    print(f"  {'─'*46}")

    if os.path.exists(DB_PATH):
        try:
            conn_ps = sqlite3.connect(DB_PATH)
            # 대형 건 알림 (미발송분): 공사 10억+, 용역 5억+
            large_prespecs = conn_ps.execute("""
                SELECT bfSpecRgstNo, bsnsDivNm, prdctClsfcNoNm, rlDminsttNm,
                       asignBdgtAmt, opninRgstClseDt, is_target, target_type
                FROM prespec_monitor
                WHERE alert_sent = 0
                AND ((bsnsDivNm = '공사' AND asignBdgtAmt >= 10e8)
                  OR (bsnsDivNm = '용역' AND asignBdgtAmt >= 5e8))
                ORDER BY asignBdgtAmt DESC
            """).fetchall()

            if large_prespecs:
                print(f"  🔔 대형 사전규격 신규: {len(large_prespecs)}건")
                for ps in large_prespecs[:5]:
                    amt_억 = ps[4] / 1e8
                    target_str = f" [보호대상:{ps[7]}]" if ps[6] else ""
                    deadline = ps[5][:10] if ps[5] else "?"
                    msg = (f"📋 [사전규격/{ps[1]}] \"{ps[2][:30]}\" "
                           f"{amt_억:.1f}억 — {ps[3]} (마감:{deadline}){target_str}")
                    alerts.append(('WARNING', msg))
                    print(f"  {msg}")
                if len(large_prespecs) > 5:
                    print(f"     ... 외 {len(large_prespecs)-5}건")

                # alert_sent 업데이트
                ids = [ps[0] for ps in large_prespecs]
                now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                for pid in ids:
                    conn_ps.execute("UPDATE prespec_monitor SET alert_sent=1, alert_sent_dt=? WHERE bfSpecRgstNo=?",
                                    (now_str, pid))
                conn_ps.commit()
            else:
                print(f"  ✅ 정상: 대형 사전규격 신규 없음")

            # 보호제도 대상 요약 (경보로는 안 보내고 로그만)
            target_count = conn_ps.execute(
                "SELECT COUNT(*) FROM prespec_monitor WHERE is_target=1 AND opninRgstClseDt >= date('now')"
            ).fetchone()[0]
            if target_count:
                print(f"  ℹ️  보호제도 대상 사전규격 (마감 전): {target_count}건 (대시보드에서 확인)")

            conn_ps.close()
        except Exception as e:
            print(f"  ⚠️ 사전규격 경보 오류: {e}")

    # ══════════════════════════════════════════════════════
    # Part B-1: 공고 단계 사전 경보
    # ══════════════════════════════════════════════════════
    print(f"\n  {'─'*46}")
    print(f"  📋 [공고 단계] 지역제한/의무공동도급 미적용 감지")
    print(f"  {'─'*46}")

    suspects = check_bid_notices_protection()
    if suspects:
        print(f"  ⚠️  보호제도 미적용 의심: {len(suspects)}건")
        suspects.sort(key=lambda x: x['추정가격'], reverse=True)
        for s in suspects[:5]:
            msg = (f"⚠️  [{s['분야']}/{s['구분']}] \"{s['공고명'][:30]}\" "
                   f"{s['추정가격']:.1f}억 (≤{s['기준액']:.0f}억) "
                   f"— {s['수요기관']} [{s['계약방식']}, 의무도급:{s['의무공동도급']}]")
            alerts.append(('WARNING', msg))
            print(f"  {msg}")
        if len(suspects) > 5:
            print(f"     ... 외 {len(suspects) - 5}건")
    else:
        print(f"  ✅ 정상: 보호제도 미적용 의심 공고 없음")

    # ══════════════════════════════════════════════════════
    # Part B-2: 계약 단계 — 외지업체 지분 60% 초과 (의무공동도급 위반)
    # ══════════════════════════════════════════════════════
    print(f"\n  {'─'*46}")
    print(f"  💰 [계약 단계] 공사 외지업체 지분 60% 초과 감지")
    print(f"  {'─'*46}")

    violations = check_construction_share_violation()
    if violations:
        print(f"  🚨 외지업체 지분 60% 초과: {len(violations)}건")
        violations.sort(key=lambda x: x['계약금액'], reverse=True)
        for v in violations[:5]:
            msg = (f"🚨 [공사] \"{v['외지업체']}\" 지분 {v['외지지분']}% "
                   f"({v['계약금액']:.1f}억) — {v['수요기관']} [{v['계약번호']}]")
            alerts.append(('CRITICAL', msg))
            print(f"  {msg}")
        if len(violations) > 5:
            print(f"     ... 외 {len(violations) - 5}건")
    else:
        print(f"  ✅ 정상: 외지업체 지분 60% 초과 건 없음")

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
                f.write(f"\n--- 공고 사전 경보 ({len(suspects)}건) ---\n")
                for s in suspects:
                    f.write(f"  [{s['분야']}/{s['구분']}] {s['공고번호']} "
                            f"\"{s['공고명']}\" {s['추정가격']:.1f}억 "
                            f"({s['수요기관']}, {s['계약방식']}, "
                            f"지역제한:{s['지역제한']}, 의무도급:{s['의무공동도급']})\n")
            if violations:
                f.write(f"\n--- 외지업체 지분 초과 ({len(violations)}건) ---\n")
                for v in violations:
                    f.write(f"  [{v['계약번호']}] {v['수요기관']} "
                            f"{v['계약금액']:.1f}억 → {v['외지업체']} {v['외지지분']}% "
                            f"(계약일: {v['계약일']})\n")
            f.write(f"\n{'='*60}\n")
            f.write(f"총 경보: {critical}건, 주의: {warning}건\n")
        print(f"  📄 로그 저장: {log_file}")

    # ────────── 경보 이력 DB 저장 ──────────
    if alerts and os.path.exists(DB_PATH):
        try:
            conn_ah = sqlite3.connect(DB_PATH)
            conn_ah.execute("""CREATE TABLE IF NOT EXISTS alert_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_dt TEXT, alert_type TEXT, severity TEXT,
                title TEXT, detail TEXT, sector TEXT, agency TEXT,
                amount REAL, ref_no TEXT,
                resolved INTEGER DEFAULT 0, resolved_dt TEXT, resolved_note TEXT
            )""")
            now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # suspects/violations → ref_no 인덱스 구축
            suspect_map = {}  # msg → suspect dict
            for s in suspects:
                key_prefix = f"{s['공고명'][:25]}"
                suspect_map[key_prefix] = s
            violation_map = {}
            for v in violations:
                key_prefix = f"{v['외지업체'][:15]}"
                violation_map[key_prefix] = v

            for level, msg in alerts:
                # 유형 분류
                if '사전규격' in msg:
                    atype = '사전규격'
                elif '보호제도' in msg or '미적용' in msg:
                    atype = '보호제도미적용'
                elif '유출' in msg:
                    atype = '대형유출'
                elif '외지업체' in msg or '지분' in msg:
                    atype = '지분초과'
                elif '수주율' in msg:
                    atype = '수주율변동'
                elif '발주액' in msg:
                    atype = '발주액변동'
                else:
                    atype = '기타'

                # 구조화 필드 추출
                _sector = ''
                _agency = ''
                _amount = 0
                _ref_no = ''

                import re as _re

                # 대형유출: 🚨 [경보] 대형 유출 [공사]: "계약명" 50억 (기관명)
                if atype == '대형유출':
                    m = _re.search(r'\[(공사|용역|물품|쇼핑몰)\]', msg)
                    if m: _sector = m.group(1)
                    m2 = _re.search(r'\(([^)]+)\)\s*$', msg)
                    if m2: _agency = m2.group(1)
                    m3 = _re.search(r'(\d+(?:\.\d+)?)억', msg)
                    if m3: _amount = float(m3.group(1)) * 1e8

                # 사전규격: 📋 [사전규격/공사] "품명" 6.2억 — 수요기관 (마감:...)
                elif atype == '사전규격':
                    m = _re.search(r'\[(사전규격)/(공사|용역)\]', msg)
                    if m: _sector = m.group(2)
                    m2 = _re.search(r'— (.+?)(?:\s*\(마감|$)', msg)
                    if m2: _agency = m2.group(1).strip()
                    m3 = _re.search(r'(\d+(?:\.\d+)?)억', msg)
                    if m3: _amount = float(m3.group(1)) * 1e8

                # 보호제도미적용: ⚠️ [공사/종합] "공고명" 6.2억 — 수요기관 [...]
                elif atype == '보호제도미적용':
                    m = _re.search(r'\[(공사|용역)/(종합|전문|용역)\]', msg)
                    if m: _sector = m.group(1)
                    m2 = _re.search(r'— (.+?)(?:\s*\[|$)', msg)
                    if m2: _agency = m2.group(1).strip()
                    m3 = _re.search(r'(\d+(?:\.\d+)?)억', msg)
                    if m3: _amount = float(m3.group(1)) * 1e8
                    # ref_no from suspects
                    for sk, sv in suspect_map.items():
                        if sk in msg:
                            _ref_no = sv.get('공고번호', '')
                            break

                # 지분초과: 🚨 [공사] "외지업체" 지분 65% (12.3억) — 수요기관 [계약번호]
                elif atype == '지분초과':
                    _sector = '공사'
                    m2 = _re.search(r'— (.+?)(?:\s*\[|$)', msg)
                    if m2: _agency = m2.group(1).strip()
                    m3 = _re.search(r'(\d+(?:\.\d+)?)억', msg)
                    if m3: _amount = float(m3.group(1)) * 1e8
                    m4 = _re.search(r'\[([^\]]+)\]\s*$', msg)
                    if m4: _ref_no = m4.group(1)

                # 수주율/발주액 변동: 전체 지표
                elif atype in ('수주율변동', '발주액변동'):
                    m = _re.search(r'\[(공사|용역|물품|쇼핑몰)\]', msg)
                    if m: _sector = m.group(1)

                conn_ah.execute(
                    "INSERT INTO alert_history (alert_dt, alert_type, severity, title, detail, sector, agency, amount, ref_no) VALUES (?,?,?,?,?,?,?,?,?)",
                    (now_str, atype, level, msg[:100], msg, _sector, _agency, _amount, _ref_no)
                )
            conn_ah.commit()
            conn_ah.close()
            print(f"  💾 경보 이력 DB 저장: {len(alerts)}건")
        except Exception as e:
            print(f"  ⚠️ 경보 이력 저장 실패: {e}")

    # ────────── 알림 발송 (텔레그램 + 이메일) ──────────
    config = load_config()
    send_notifications(alerts, suspects, violations, config)

    print("==================================================\n")
    return alerts


if __name__ == '__main__':
    alerts = run_alert_check()
    sys.exit(1 if any(level == 'CRITICAL' for level, _ in alerts) else 0)
