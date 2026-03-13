import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

DB_PROC = 'procurement_contracts.db'
DB_AG = 'busan_agencies_master.db'
DB_COMP = 'busan_companies_master.db'

NON_BUSAN_KEYWORDS = ['서울','인천','대구','대전','광주광역','울산','세종','제주','경기','경기도','강원','강원도','충북','충청북도','충남','충청남도','전북','전라북도','전남','전라남도','경북','경상북도','경남','울릉','독도']
BUSAN_EXCEPTIONS = {'대구': ['해운대구']}

def is_non_busan(name, phone):
    name = str(name)
    phone = str(phone)
    phone_bad = phone[:3] not in ('051','070','010','***') and len(phone) >= 3
    kw_bad = False
    for kw in NON_BUSAN_KEYWORDS:
        if kw in name:
            if kw in BUSAN_EXCEPTIONS:
                if not any(ex in name for ex in BUSAN_EXCEPTIONS[kw]):
                    kw_bad = True; break
            else:
                kw_bad = True; break
    return phone_bad or kw_bad

# 수영구 기관 코드
conn_ag = sqlite3.connect(DB_AG)
df_ag = pd.read_sql("SELECT dminsttCd, dminsttNm FROM agency_master WHERE cate_sml = '수영구'", conn_ag)
conn_ag.close()
target_codes = set(df_ag['dminsttCd'].astype(str).str.strip())
print(f"🔎 부산광역시 수영구 ({len(target_codes)}개 기관)")
print(", ".join(df_ag['dminsttNm'].tolist()) + "\n")

# 지역업체
conn_cp = sqlite3.connect(DB_COMP)
busan_biznos = set(pd.read_sql("SELECT bizno FROM company_master", conn_cp)['bizno'].dropna().astype(str).str.replace('-','',regex=False).str.strip())
conn_cp.close()

conn_pr = sqlite3.connect(DB_PROC)

def calc_sector(table, date_col, is_shop=False, use_loc_filter=False):
    if is_shop:
        df = pd.read_sql(f"SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, dminsttCd, prdctAmt, cntrctCorpBizno FROM {table} WHERE {date_col} >= '2026-01-01'", conn_pr)
        df.sort_values('dlvrReqChgOrd', ascending=False, inplace=True)
        df.drop_duplicates(subset=['dlvrReqNo','prdctSno'], keep='first', inplace=True)
        df['target_cd'] = df['dminsttCd'].astype(str).str.strip()
        df = df[df['target_cd'].isin(target_codes)]
        tot = pd.to_numeric(df['prdctAmt'], errors='coerce').sum()
        loc = 0
        for _, r in df.iterrows():
            biz = str(r['cntrctCorpBizno']).replace('-','').strip()
            if biz in busan_biznos:
                loc += float(r['prdctAmt'] or 0)
    else:
        extra = ', cntrctNm, cntrctInsttOfclTelNo' if use_loc_filter else ''
        ntce = ', ntceNo' if table == 'cnstwk_cntrct' else ''
        df = pd.read_sql(f"SELECT untyCntrctNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt, corpList, dminsttList{ntce}{extra} FROM {table} WHERE {date_col} >= '2026-01-01'", conn_pr)
        df.drop_duplicates(subset=['untyCntrctNo'], keep='last', inplace=True)
        
        tot = 0; loc = 0
        for _, r in df.iterrows():
            instt = str(r['cntrctInsttCd']).strip()
            dminstt = str(r.get('dminsttList',''))
            if instt not in target_codes and not any(c in dminstt for c in target_codes):
                continue
            
            if use_loc_filter:
                if is_non_busan(r.get('cntrctNm',''), r.get('cntrctInsttOfclTelNo','')):
                    continue
            
            amt = float(r['thtmCntrctAmt'] or 0) if pd.to_numeric(r['thtmCntrctAmt'], errors='coerce') else 0
            if amt == 0:
                amt = float(r['totCntrctAmt'] or 0) if pd.to_numeric(r['totCntrctAmt'], errors='coerce') else 0
            tot += amt
            
            corps = str(r['corpList'])
            if corps and corps != 'nan':
                for part in corps.split('|'):
                    segs = part.split(',')
                    if len(segs) >= 2:
                        biz = segs[0].replace('-','').strip()
                        try: share = float(segs[1])
                        except: share = 100
                        if biz in busan_biznos:
                            loc += amt * share / 100
    
    return tot, loc

c_tot, c_loc = calc_sector('cnstwk_cntrct', 'cntrctDate')
s_tot, s_loc = calc_sector('servc_cntrct', 'cntrctDate', use_loc_filter=True)
t_tot, t_loc = calc_sector('thng_cntrct', 'cntrctDate', use_loc_filter=True)
p_tot, p_loc = calc_sector('shopping_cntrct', 'dlvrReqRcptDate', True)

conn_pr.close()

goods_tot = t_tot + p_tot
goods_loc = t_loc + p_loc

print(f"🏢 [공사]       발주: {c_tot:>15,.0f}원 / 수주: {c_loc:>15,.0f}원 ({c_loc/c_tot*100:.1f}%)" if c_tot else "🏢 [공사]       실적 없음")
print(f"🤝 [용역]       발주: {s_tot:>15,.0f}원 / 수주: {s_loc:>15,.0f}원 ({s_loc/s_tot*100:.1f}%)" if s_tot else "🤝 [용역]       실적 없음")
print(f"📦 [일반물품]    발주: {t_tot:>15,.0f}원 / 수주: {t_loc:>15,.0f}원 ({t_loc/t_tot*100:.1f}%)" if t_tot else "📦 [일반물품]    실적 없음")
print(f"🛒 [종합쇼핑몰]  발주: {p_tot:>15,.0f}원 / 수주: {p_loc:>15,.0f}원 ({p_loc/p_tot*100:.1f}%)" if p_tot else "🛒 [종합쇼핑몰]  실적 없음")
print(f"📦+🛒 [물품합계] 발주: {goods_tot:>15,.0f}원 / 수주: {goods_loc:>15,.0f}원 ({goods_loc/goods_tot*100:.1f}%)" if goods_tot else "")
print("-" * 60)
total_tot = c_tot + s_tot + goods_tot
total_loc = c_loc + s_loc + goods_loc
print(f"🌟 [종합 합계]   발주: {total_tot:>15,.0f}원 / 수주: {total_loc:>15,.0f}원 ({total_loc/total_tot*100:.1f}%)" if total_tot else "종합 실적 없음")
