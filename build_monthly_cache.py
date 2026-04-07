"""
월별 수주율 추이 캐시 생성기
============================
build_api_cache.py와 동일한 core_calc 로직을 사용하여
월별 누계/단월 수주율 + 변동 원인을 계산.

출력: monthly_cache.json
"""
import sqlite3, pandas as pd, json, sys, time
from datetime import datetime
from collections import defaultdict

from core_calc import (
    parse_corp_shares, dedup_by_dcsn,
    filter_cnstwk_by_site, filter_servc_by_site, filter_shopping_by_site,
    process_contract_row,
    load_bid_dict, load_award_sets, load_expanded_biznos,
    BUSAN_BIZNO_PREFIXES,
)

sys.stdout.reconfigure(encoding='utf-8')

DB_PROCUREMENT = 'procurement_contracts.db'
DB_AGENCIES = 'busan_agencies_master.db'
DB_COMPANIES = 'busan_companies_master.db'
MONTHLY_CACHE = 'monthly_cache.json'

CURRENT_YEAR = str(datetime.now().year)


def pct(t, l):
    return round(l / t * 100, 1) if t > 0 else 0


def build_monthly():
    start = time.time()
    print("[월별캐시] 시작...")

    # ── 마스터 로딩 ──
    conn_ag = sqlite3.connect(DB_AGENCIES)
    master = pd.read_sql(
        "SELECT dminsttCd, cate_lrg, cate_mid, cate_sml, compare_unit FROM agency_master",
        conn_ag)
    conn_ag.close()
    master['dminsttCd'] = master['dminsttCd'].astype(str).str.strip()
    inst_dict = master.set_index('dminsttCd')[['cate_lrg', 'cate_mid', 'cate_sml']].to_dict('index')
    inst_unit = dict(zip(master['dminsttCd'], master['compare_unit']))
    inst_grp = dict(zip(master['dminsttCd'], master['cate_lrg']))
    inst_mid = dict(zip(master['dminsttCd'], master['cate_mid']))
    inst_sml = dict(zip(master['dminsttCd'], master['cate_sml'].fillna('')))

    # 4그룹 매핑: compare_unit 기준으로 통일 (build_api_cache.py와 정합성 확보)
    출자출연_sml = {'부산광역시 출연기관', '부산광역시 출자기관', '부산광역시 공기업', '부산광역시 공단'}

    # compare_unit별 소그룹 사전 매핑
    _unit_subgroup = {}
    for _cd in master['dminsttCd']:
        _unit = str(inst_unit.get(_cd, '')).strip()
        if not _unit:
            continue
        _sml = str(inst_sml.get(_cd, ''))
        _mid = str(inst_mid.get(_cd, ''))
        _lrg = str(inst_grp.get(_cd, ''))
        # 출자출연 우선 (한번이라도 매핑되면 유지)
        if _sml in 출자출연_sml:
            _unit_subgroup[_unit] = '출자출연기관'
        elif _unit not in _unit_subgroup:
            if _mid == '자치구군':
                _unit_subgroup[_unit] = '자치구군'
            elif _mid in ('중앙행정기관', '국가공공기관', '고등교육기관'):
                _unit_subgroup[_unit] = '정부및국가공공기관'
            elif _lrg == '부산광역시 및 소속기관':
                _unit_subgroup[_unit] = '부산광역시및산하기관'

    def get_sub_group(cd):
        unit = str(inst_unit.get(cd, '')).strip()
        return _unit_subgroup.get(unit)

    conn_cp = sqlite3.connect(DB_COMPANIES)
    _conn_pr = sqlite3.connect(DB_PROCUREMENT)
    biznos = load_expanded_biznos(conn_cp, _conn_pr)
    _conn_pr.close()

    # 주소 보강 (build_api_cache.py와 동일)
    conn = sqlite3.connect(DB_PROCUREMENT)
    for _award_tbl in ['busan_award_cnstwk', 'busan_award_servc', 'busan_award_thng']:
        try:
            for _ar in conn.execute(
                f"SELECT bidwinnrBizno, bidwinnrAdrs FROM {_award_tbl} WHERE bidwinnrAdrs LIKE '%부산%'"
            ).fetchall():
                _bno = str(_ar[0]).replace('-', '').strip()
                if _bno and len(_bno) >= 10:
                    biznos.add(_bno)
        except:
            pass
    for _cl_tbl in ['cnstwk_cntrct', 'servc_cntrct', 'thng_cntrct']:
        try:
            for (_cl,) in conn.execute(
                f"SELECT corpList FROM [{_cl_tbl}] WHERE corpList IS NOT NULL AND corpList != ''"
            ).fetchall():
                for _ch in str(_cl).split('[')[1:]:
                    _ps = _ch.split(']')[0].split('^')
                    if len(_ps) >= 12:
                        _bno = str(_ps[9]).replace('-', '').strip()
                        _addr = str(_ps[11]).strip()
                        if _bno and len(_bno) >= 10 and '부산' in _addr:
                            biznos.add(_bno)
        except:
            pass
    conn_cp.close()
    print(f"  부산업체: {len(biznos):,}")

    # ── 필터 로딩 ──
    bid_dict, bid_df = load_bid_dict(conn)
    award_sets = load_award_sets(conn)

    def get_unit(cd):
        unit = inst_unit.get(cd)
        if unit and inst_mid.get(cd, '') == '부산광역시 교육청':
            return '부산교육청'
        return unit

    # ═══════════════════════════════
    # 핵심: 각 분야 계약을 처리하고 월 태깅
    # ═══════════════════════════════
    # records: [(sector, month, cd, unit, grp, amt, loc_amt, cntrctNm), ...]
    records = []

    # ── 공사 ──
    print("  [공사] 로딩...")
    df = pd.read_sql("""SELECT untyCntrctNo, dcsnCntrctNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt,
        corpList, ntceNo, dminsttList, cnstwkNm, cntrctInsttOfclTelNo, cntrctCnclsDate
        FROM cnstwk_cntrct""", conn)
    df.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
    df = dedup_by_dcsn(df)
    df_filtered, _, _ = filter_cnstwk_by_site(df, bid_df)

    for _, row in df_filtered.iterrows():
        result = process_contract_row(row, inst_dict, biznos,
                                       use_location_filter=True,
                                       bid_dict=bid_dict,
                                       award_set=award_sets['공사'])
        if not result:
            continue
        cd, amt, loc = result
        lrg = inst_grp.get(cd)
        unit = get_unit(cd)
        if not lrg or not unit:
            continue
        if lrg == '민간 및 기타기관' or unit == '공익단체':
            continue
        dt = str(row.get('cntrctCnclsDate', '') or '')[:7]  # "2026-01"
        if not dt.startswith(CURRENT_YEAR):
            continue
        month = dt[5:7]  # "01"
        nm = str(row.get('cnstwkNm', '') or row.get('cntrctNm', '') or '')[:50]
        records.append(('공사', month, cd, unit, lrg, amt, loc, nm))

    print(f"    공사: {sum(1 for r in records if r[0]=='공사'):,}건")

    # ── 용역 / 물품 ──
    for tbl, name, award_key in [('servc_cntrct', '용역', '용역'), ('thng_cntrct', '물품', '물품')]:
        print(f"  [{name}] 로딩...")
        extra_col = ', cnstrtsiteRgnNm' if tbl == 'servc_cntrct' else ''
        df = pd.read_sql(f"""SELECT untyCntrctNo, dcsnCntrctNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt,
            corpList, ntceNo, dminsttList, cntrctNm, cntrctInsttOfclTelNo, cntrctCnclsDate{extra_col}
            FROM [{tbl}]""", conn)
        df.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
        df = dedup_by_dcsn(df)
        if tbl == 'servc_cntrct':
            df, _, _ = filter_servc_by_site(df, inst_dict)

        for _, row in df.iterrows():
            result = process_contract_row(row, inst_dict, biznos,
                                           use_location_filter=True,
                                           bid_dict=bid_dict,
                                           award_set=award_sets[award_key])
            if not result:
                continue
            cd, amt, loc = result
            lrg = inst_grp.get(cd)
            unit = get_unit(cd)
            if not lrg or not unit:
                continue
            if lrg == '민간 및 기타기관' or unit == '공익단체':
                continue
            dt = str(row.get('cntrctCnclsDate', '') or '')[:7]
            if not dt.startswith(CURRENT_YEAR):
                continue
            month = dt[5:7]
            nm = str(row.get('cntrctNm', '') or '')[:50]
            records.append((name, month, cd, unit, lrg, amt, loc, nm))

        print(f"    {name}: {sum(1 for r in records if r[0]==name):,}건")

    # ── 쇼핑몰 ──
    print("  [쇼핑몰] 로딩...")
    df = pd.read_sql("""SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, dminsttCd,
        prdctAmt, cntrctCorpBizno, prdctClsfcNoNm,
        cnstwkMtrlDrctPurchsObjYn, dlvrReqNm, dlvrReqRcptDate FROM shopping_cntrct""", conn)
    df['dlvrReqChgOrd'] = pd.to_numeric(df['dlvrReqChgOrd'], errors='coerce').fillna(0)
    df.sort_values('dlvrReqChgOrd', ascending=False, inplace=True)
    df.drop_duplicates(subset=['dlvrReqNo', 'prdctSno'], keep='first', inplace=True)
    df, _, _ = filter_shopping_by_site(df, conn, set(inst_dict.keys()), inst_dict=inst_dict)

    for _, row in df.iterrows():
        result = process_contract_row(row, inst_dict, biznos, is_shopping=True)
        if not result:
            continue
        cd, amt, loc = result
        lrg = inst_grp.get(cd)
        unit = get_unit(cd)
        if not lrg or not unit:
            continue
        if lrg == '민간 및 기타기관' or unit == '공익단체':
            continue
        dt = str(row.get('dlvrReqRcptDate', '') or '')[:7]
        if not dt.startswith(CURRENT_YEAR):
            continue
        month = dt[5:7]
        nm = str(row.get('dlvrReqNm', '') or '')[:50]
        records.append(('쇼핑몰', month, cd, unit, lrg, amt, loc, nm))

    print(f"    쇼핑몰: {sum(1 for r in records if r[0]=='쇼핑몰'):,}건")
    conn.close()

    # ═══════════════════════════════
    # 월별 집계
    # ═══════════════════════════════
    all_months = sorted(set(r[1] for r in records))
    print(f"  월 목록: {all_months}")

    # 그룹 매핑
    GRP_MAP = {
        '부산광역시 및 소속기관': '부산시',
        '정부 및 국가공공기관': '국가',
    }

    # ── 1. 누계 추이 ──
    # key: (view, label) → {month: {total, local}}
    # views: 그룹별(전체/부산시/국가), 분야별(공사/용역/물품/쇼핑몰)
    # 기관별(unit)
    월별_그룹 = defaultdict(lambda: defaultdict(lambda: {'total': 0, 'local': 0}))
    월별_분야 = defaultdict(lambda: defaultdict(lambda: {'total': 0, 'local': 0}))
    월별_기관 = defaultdict(lambda: defaultdict(lambda: {'total': 0, 'local': 0}))
    # 기관×분야
    월별_기관_분야 = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {'total': 0, 'local': 0})))
    # 소그룹별 (4그룹)
    월별_소그룹 = defaultdict(lambda: defaultdict(lambda: {'total': 0, 'local': 0}))
    # 소그룹×분야
    월별_소그룹_분야 = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {'total': 0, 'local': 0})))
    # 변동 원인용: 기관별 개별 계약
    월별_계약 = defaultdict(list)  # month → [(sector, unit, amt, loc, leakage, nm), ...]

    for sector, month, cd, unit, lrg, amt, loc, nm in records:
        grp_label = GRP_MAP.get(lrg, lrg)
        leakage = amt - loc  # 유출액

        # 전체
        월별_그룹['전체'][month]['total'] += amt
        월별_그룹['전체'][month]['local'] += loc
        # 그룹별
        월별_그룹[grp_label][month]['total'] += amt
        월별_그룹[grp_label][month]['local'] += loc
        # 분야별
        월별_분야[sector][month]['total'] += amt
        월별_분야[sector][month]['local'] += loc
        # 기관별
        월별_기관[unit][month]['total'] += amt
        월별_기관[unit][month]['local'] += loc
        # 기관×분야
        월별_기관_분야[unit][sector][month]['total'] += amt
        월별_기관_분야[unit][sector][month]['local'] += loc
        # 소그룹별 (4그룹)
        sub_g = get_sub_group(cd)
        if sub_g:
            월별_소그룹[sub_g][month]['total'] += amt
            월별_소그룹[sub_g][month]['local'] += loc
            월별_소그룹_분야[sub_g][sector][month]['total'] += amt
            월별_소그룹_분야[sub_g][sector][month]['local'] += loc
        # 유출 계약 기록
        if leakage > 0:
            월별_계약[month].append((sector, unit, amt, loc, leakage, nm))

    # ── 누계 계산 함수 ──
    def calc_cumulative(월별_data, all_months):
        """월별 단독 데이터 → 누계 시리즈"""
        result = []
        cum_total = 0
        cum_local = 0
        for m in all_months:
            d = 월별_data.get(m, {'total': 0, 'local': 0})
            cum_total += d['total']
            cum_local += d['local']
            result.append({
                '월': m,
                '발주액': round(cum_total),
                '수주액': round(cum_local),
                '수주율': pct(cum_total, cum_local),
            })
        return result

    def calc_monthly(월별_data, all_months):
        """월별 단독 수주율"""
        result = []
        for m in all_months:
            d = 월별_data.get(m, {'total': 0, 'local': 0})
            result.append({
                '월': m,
                '발주액': round(d['total']),
                '수주액': round(d['local']),
                '수주율': pct(d['total'], d['local']),
            })
        return result

    # ── 결과 조립 ──
    output = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'year': CURRENT_YEAR,
        'months': all_months,
    }

    # 1. 그룹별 누계 추이
    누계추이 = {}
    for label in ['전체', '부산시', '국가']:
        누계추이[label] = calc_cumulative(월별_그룹[label], all_months)
    output['누계_그룹'] = 누계추이

    # 2. 분야별 누계 추이
    분야누계 = {}
    for sector in ['공사', '용역', '물품', '쇼핑몰']:
        분야누계[sector] = calc_cumulative(월별_분야[sector], all_months)
    output['누계_분야'] = 분야누계

    # 3. 그룹별 월간 평균
    월간_그룹 = {}
    for label in ['전체', '부산시', '국가']:
        월간_그룹[label] = calc_monthly(월별_그룹[label], all_months)
    output['월간_그룹'] = 월간_그룹

    # 소그룹(4그룹) 누계/월간
    소그룹_labels = ['부산광역시및산하기관', '정부및국가공공기관', '자치구군', '출자출연기관']
    소그룹_누계 = {}
    소그룹_월간 = {}
    for label in 소그룹_labels:
        소그룹_누계[label] = calc_cumulative(월별_소그룹[label], all_months)
        소그룹_월간[label] = calc_monthly(월별_소그룹[label], all_months)
    output['누계_소그룹'] = 소그룹_누계
    output['월간_소그룹'] = 소그룹_월간

    # 소그룹×분야 누계/월간
    소그룹_분야_누계 = {}
    소그룹_분야_월간 = {}
    for label in 소그룹_labels:
        소그룹_분야_누계[label] = {}
        소그룹_분야_월간[label] = {}
        for sector in ['공사', '용역', '물품', '쇼핑몰']:
            소그룹_분야_누계[label][sector] = calc_cumulative(월별_소그룹_분야[label][sector], all_months)
            소그룹_분야_월간[label][sector] = calc_monthly(월별_소그룹_분야[label][sector], all_months)
    output['누계_소그룹분야'] = 소그룹_분야_누계
    output['월간_소그룹분야'] = 소그룹_분야_월간

    # 4. 분야별 월간 평균
    월간_분야 = {}
    for sector in ['공사', '용역', '물품', '쇼핑몰']:
        월간_분야[sector] = calc_monthly(월별_분야[sector], all_months)
    output['월간_분야'] = 월간_분야

    # 5. 변동 원인 분석 (누계 기준, 증가/감소 기여 계약 TOP 5)
    변동분석 = {}
    prev_cum = {}  # label → cum_total, cum_local
    for label in ['전체']:
        prev_t, prev_l = 0, 0
        for i, m in enumerate(all_months):
            d = 월별_그룹[label].get(m, {'total': 0, 'local': 0})
            cur_t = prev_t + d['total']
            cur_l = prev_l + d['local']
            if i > 0:
                prev_rate = pct(prev_t, prev_l)
                cur_rate = pct(cur_t, cur_l)
                변동 = round(cur_rate - prev_rate, 1)
                # 해당 월의 유출 계약 중 영향이 큰 TOP 5
                contracts = 월별_계약.get(m, [])
                if 변동 < 0:
                    # 감소: 유출액 큰 순
                    top = sorted(contracts, key=lambda x: x[4], reverse=True)[:5]
                    items = [{'분야': c[0], '기관': c[1], '계약명': c[5],
                              '발주액': round(c[2]), '유출액': round(c[4])} for c in top]
                else:
                    # 증가: 해당 월에 수주액이 큰 계약 TOP 5
                    month_recs = [(s, u, a, l, l, n) for s, mo, cd, u, lrg, a, l, n in records if mo == m and l > 0]
                    top = sorted(month_recs, key=lambda x: x[3], reverse=True)[:5]
                    items = [{'분야': c[0], '기관': c[1], '계약명': c[5],
                              '발주액': round(c[2]), '수주액': round(c[3])} for c in top]
                변동분석[f"{all_months[i-1]}→{m}"] = {
                    '이전율': prev_rate,
                    '현재율': cur_rate,
                    '변동': 변동,
                    '방향': '감소' if 변동 < 0 else '증가' if 변동 > 0 else '유지',
                    '주요계약': items,
                }
            prev_t, prev_l = cur_t, cur_l
    output['변동분석'] = 변동분석

    # 5-2. 분야별 변동 분석
    분야변동 = {}
    for sector in ['공사', '용역', '물품', '쇼핑몰']:
        prev_t, prev_l = 0, 0
        for i, m in enumerate(all_months):
            d = 월별_분야[sector].get(m, {'total': 0, 'local': 0})
            cur_t = prev_t + d['total']
            cur_l = prev_l + d['local']
            if i > 0:
                prev_rate = pct(prev_t, prev_l)
                cur_rate = pct(cur_t, cur_l)
                delta = round(cur_rate - prev_rate, 1)
                key = f"{all_months[i-1]}→{m}"
                # 해당 분야의 해당 월 계약에서 TOP 3
                sec_contracts = [(s, u, a, l, a-l, n) for s, mo, cd, u, lrg, a, l, n in records if mo == m and s == sector]
                if delta < 0:
                    top3 = sorted(sec_contracts, key=lambda x: x[4], reverse=True)[:3]
                    items3 = [{'분야': c[0], '기관': c[1], '계약명': c[5],
                               '발주액': round(c[2]), '유출액': round(c[4])} for c in top3]
                else:
                    top3 = sorted(sec_contracts, key=lambda x: x[3], reverse=True)[:3]
                    items3 = [{'분야': c[0], '기관': c[1], '계약명': c[5],
                               '발주액': round(c[2]), '수주액': round(c[3])} for c in top3]
                if sector not in 분야변동:
                    분야변동[sector] = {}
                분야변동[sector][key] = {
                    '이전율': prev_rate, '현재율': cur_rate,
                    '변동': delta,
                    '방향': '감소' if delta < 0 else '증가' if delta > 0 else '유지',
                    '주요계약': items3,
                }
            prev_t, prev_l = cur_t, cur_l
    output['분야변동'] = 분야변동

    # 6. 기관별 데이터 (검색용)
    기관목록 = sorted(월별_기관.keys())
    기관별 = {}
    for unit in 기관목록:
        기관별[unit] = {
            '누계': calc_cumulative(월별_기관[unit], all_months),
            '월간': calc_monthly(월별_기관[unit], all_months),
            '분야별_누계': {},
            '분야별_월간': {},
        }
        for sector in ['공사', '용역', '물품', '쇼핑몰']:
            s_data = 월별_기관_분야[unit].get(sector, {})
            if any(s_data.get(m, {}).get('total', 0) > 0 for m in all_months):
                기관별[unit]['분야별_누계'][sector] = calc_cumulative(s_data, all_months)
                기관별[unit]['분야별_월간'][sector] = calc_monthly(s_data, all_months)
    output['기관별'] = 기관별

    # ── 저장 ──
    with open(MONTHLY_CACHE, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=1)
    sz = len(json.dumps(output, ensure_ascii=False)) / 1024 / 1024
    print(f"[월별캐시] 완료! {MONTHLY_CACHE} ({sz:.1f}MB, {len(기관목록)}개 기관, {time.time()-start:.1f}초)")


if __name__ == '__main__':
    build_monthly()
