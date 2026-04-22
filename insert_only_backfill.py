#!/usr/bin/env python3
"""insert_only_backfill.py — v4: standalone, explicit busan filter"""
import sys, os, sqlite3, time, re, urllib.request, json, ssl
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

os.chdir('/opt/busan')

# SERVICE_KEY
with open('/opt/busan/daily_pipeline_sync.py') as f:
    for line in f:
        line = line.strip()
        if line.startswith('SERVICE_KEY') and '=' in line and '#' not in line.split('=')[0]:
            exec(line); break

APIS = {
    '공사_중앙': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwkPPSSrch',
    '공사_자체': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwk',
    '용역_중앙': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServcPPSSrch',
    '용역_자체': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServc',
    '물품_중앙': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThngPPSSrch',
    '물품_자체': 'https://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThng',
    '쇼핑몰': 'https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getDlvrReqDtlInfoList'
}
TABLE_MAP = {
    '공사_중앙': 'cnstwk_cntrct', '공사_자체': 'cnstwk_cntrct',
    '용역_중앙': 'servc_cntrct', '용역_자체': 'servc_cntrct',
    '물품_중앙': 'thng_cntrct', '물품_자체': 'thng_cntrct',
    '쇼핑몰': 'shopping_cntrct'
}
DB_PATH = '/opt/busan/procurement_contracts.db'
AGENCY_DB_PATH = '/opt/busan/busan_agencies_master.db'
ctx = ssl.create_default_context()

def fetch_data(api_url, bgn, end, page=1, rows=999):
    is_new = api_url.endswith(('ListCnstwk', 'ListServc', 'ListThng'))
    if is_new:
        q = f"?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDt={bgn}0000&inqryEndDt={end}2359&numOfRows={rows}&pageNo={page}&type=json"
    else:
        q = f"?serviceKey={SERVICE_KEY}&inqryDiv=1&inqryBgnDate={bgn}&inqryEndDate={end}&numOfRows={rows}&pageNo={page}&type=json"
    for _ in range(3):
        try:
            req = urllib.request.Request(api_url + q, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, context=ctx, timeout=30) as res:
                data = json.loads(res.read().decode('utf-8'))
                if data.get('response',{}).get('header',{}).get('resultCode') == '00':
                    body = data['response']['body']
                    return body.get('items', []), body.get('totalCount', 0)
                return [], 0
        except: time.sleep(1)
    return [], 0

def download_cat(cat, dt):
    items, total = fetch_data(APIS[cat], dt, dt)
    if total == 0: return cat, []
    all_items = list(items) if items else []
    if int(total) > 999:
        pages = (int(total) // 999) + 1
        with ThreadPoolExecutor(max_workers=3) as ex:
            for f in as_completed([ex.submit(fetch_data, APIS[cat], dt, dt, p) for p in range(2, pages+1)]):
                pi, _ = f.result()
                if pi: all_items.extend(pi)
    return cat, all_items

def parse_cd(dl):
    m = re.search(r'\[1\^(\w+)\^([^^]+)\^', str(dl))
    return m.group(1) if m else None

def parse_nm(dl):
    m = re.search(r'\[1\^(\w+)\^([^^]+)\^', str(dl))
    return m.group(2) if m else None

def backfill_day(dt):
    print(f"\n{'='*50}\n {dt} INSERT-only\n{'='*50}")
    all_data = {}
    try:
        with ThreadPoolExecutor(max_workers=4) as ex:
            for f in as_completed([ex.submit(download_cat, c, dt) for c in APIS]):
                cat, items = f.result()
                all_data[cat] = items
                print(f"   [{cat}] {len(items):,}건")
    except Exception as e:
        print(f"   수집 실패: {e}"); return False

    ag = sqlite3.connect(AGENCY_DB_PATH)
    busan_codes = set(str(r[0]).strip() for r in ag.execute("SELECT dminsttCd FROM agency_master"))
    ag.close()

    conn = sqlite3.connect(DB_PATH)
    total_new = 0
    for cat, items in all_data.items():
        if not items: continue
        tbl = TABLE_MAP.get(cat)
        if not tbl: continue
        df = pd.DataFrame(items)
        for c in df.columns:
            if df[c].apply(lambda x: isinstance(x,(list,dict))).any(): df[c]=df[c].astype(str)
        n_raw = len(df)

        # 1) dminsttCd 파싱 — 컬럼이 없으면 생성
        if cat != '쇼핑몰' and 'dminsttList' in df.columns:
            if 'dminsttCd' not in df.columns:
                df['dminsttCd'] = None
            nm = df['dminsttCd'].isna() | df['dminsttCd'].astype(str).str.strip().isin(['','None','nan'])
            if nm.any():
                df.loc[nm, 'dminsttCd'] = df.loc[nm, 'dminsttList'].apply(parse_cd)
                if 'dminsttNm_req' not in df.columns: df['dminsttNm_req'] = None
                df.loc[nm, 'dminsttNm_req'] = df.loc[nm, 'dminsttList'].apply(parse_nm)

        # 2) 부산 필터 — None/NaN 명시 제거 후 busan_codes 필터
        if 'dminsttCd' in df.columns:
            df = df.dropna(subset=['dminsttCd']).copy()
            df['_cd'] = df['dminsttCd'].astype(str).str.strip()
            df = df[~df['_cd'].isin(['', 'None', 'nan'])]
            df = df[df['_cd'].isin(busan_codes)]
            df = df.drop(columns=['_cd'])

        n_busan = len(df)
        if n_busan == 0:
            print(f"   [{cat}] {n_raw:,} → 부산 0"); continue

        # 3) INSERT-only
        if tbl == 'shopping_cntrct':
            if all(c in df.columns for c in ['dlvrReqNo','prdctSno','dlvrReqChgOrd']):
                df = df.drop_duplicates(subset=['dlvrReqNo','prdctSno','dlvrReqChgOrd'], keep='last')
                ex = set(tuple(str(x) for x in r) for r in conn.execute(f"SELECT dlvrReqNo, prdctSno, dlvrReqChgOrd FROM {tbl}"))
                df = df[df.apply(lambda r: (str(r['dlvrReqNo']),str(r['prdctSno']),str(r['dlvrReqChgOrd'])) not in ex, axis=1)]
        else:
            if 'dcsnCntrctNo' in df.columns:
                df = df.drop_duplicates(subset=['dcsnCntrctNo'], keep='last')
                ex = set(r[0] for r in conn.execute(f"SELECT dcsnCntrctNo FROM [{tbl}]"))
                df = df[~df['dcsnCntrctNo'].isin(ex)]

        if df.empty:
            print(f"   [{cat}] {n_raw:,} → {n_busan:,} → 0 exist"); continue
        df.to_sql(tbl, conn, if_exists='append', index=False)
        total_new += len(df)
        print(f"   [{cat}] {n_raw:,} → {n_busan:,} → +{len(df):,} NEW")

    conn.commit(); conn.close()
    print(f"   ✅ +{total_new:,}건")
    return True

if __name__ == '__main__':
    ok = 0
    for d in sys.argv[1:]:
        if backfill_day(d): ok += 1
        time.sleep(1)
    print(f"\n완료: {ok}/{len(sys.argv[1:])}일")
