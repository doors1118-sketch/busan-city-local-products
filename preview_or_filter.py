import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

NON_BUSAN_KEYWORDS = [
    '서울', '인천', '대구', '대전', '광주광역', '울산',
    '세종', '제주',
    '경기', '경기도',
    '강원', '강원도', '강원특별',
    '충북', '충청북도',
    '충남', '충청남도',
    '전북', '전라북도', '전북특별',
    '전남', '전라남도',
    '경북', '경상북도',
    '경남', '경상남도',
    '울릉', '독도',
]
BUSAN_EXCEPTIONS = {'대구': ['해운대구']}

conn_ag = sqlite3.connect('busan_agencies_master.db')
df_ag = pd.read_sql("SELECT dminsttCd, cate_lrg FROM agency_master", conn_ag)
conn_ag.close()
ag_dict = dict(zip(df_ag['dminsttCd'].astype(str).str.strip(), df_ag['cate_lrg']))

conn = sqlite3.connect('procurement_contracts.db')

excluded = []

for table, nm_col, label in [('servc_cntrct', 'cntrctNm', '용역'), ('thng_cntrct', 'cntrctNm', '물품')]:
    df = pd.read_sql(f"""
        SELECT untyCntrctNo, {nm_col} as cntrctNm, cntrctInsttNm, cntrctInsttCd, 
               cntrctInsttOfclTelNo, thtmCntrctAmt, dminsttList
        FROM {table}
        WHERE cntrctDate >= '2026-01-01' AND cntrctDate <= '2026-01-31'
    """, conn)
    
    for _, row in df.iterrows():
        inst_cd = str(row['cntrctInsttCd']).strip()
        lrg = ag_dict.get(inst_cd, None)
        
        # dminsttList에서도 찾기
        if not lrg:
            dminstt = str(row.get('dminsttList', ''))
            for chunk in dminstt.split('[')[1:]:
                parts = chunk.split(']')[0].split('^')
                if len(parts) >= 2:
                    dcd = str(parts[1]).strip()
                    if dcd in ag_dict:
                        lrg = ag_dict[dcd]
                        break
        
        if not lrg: continue
        if lrg == '부산광역시 및 소속기관': continue
        
        tel = str(row['cntrctInsttOfclTelNo']).strip()
        nm = str(row['cntrctNm']).strip()
        amt = float(row['thtmCntrctAmt']) if row['thtmCntrctAmt'] else 0
        
        is_non_tel = tel and not tel.startswith(('051', '070', '010', '****', 'nan', 'None', ''))
        has_kw = False
        matched_kws = []
        for kw in NON_BUSAN_KEYWORDS:
            if kw in nm:
                exceptions = BUSAN_EXCEPTIONS.get(kw, [])
                if any(exc in nm for exc in exceptions):
                    continue
                has_kw = True
                matched_kws.append(kw)
        
        reason = []
        if is_non_tel: reason.append(f'전화:{tel[:15]}')
        if has_kw: reason.append(f'키워드:{matched_kws}')
        
        if is_non_tel or has_kw:
            excluded.append({
                'sector': label,
                'instt': row['cntrctInsttNm'][:15],
                'name': nm[:35],
                'tel': tel[:15],
                'amt': amt,
                'reason': ' + '.join(reason)
            })

conn.close()

df_exc = pd.DataFrame(excluded)
df_exc = df_exc.sort_values('amt', ascending=False)

print(f"🚨 [OR 필터 적용 시 배제될 계약] 총 {len(df_exc)}건")
print(f"   배제 총 금액: {df_exc['amt'].sum():,.0f}원\n")

print("📋 [배제 대상 상위 20건]")
for i, (_, r) in enumerate(df_exc.head(20).iterrows()):
    print(f"  {i+1:2d}. [{r['sector']}] {r['instt']} | {r['name']}... | {r['amt']:,.0f}원")
    print(f"      사유: {r['reason']}")

# 사유별 집계
print(f"\n📊 [사유별 집계]")
tel_only = df_exc[df_exc['reason'].str.contains('전화') & ~df_exc['reason'].str.contains('키워드')]
kw_only = df_exc[~df_exc['reason'].str.contains('전화') & df_exc['reason'].str.contains('키워드')]
both = df_exc[df_exc['reason'].str.contains('전화') & df_exc['reason'].str.contains('키워드')]
print(f"  전화번호만: {len(tel_only)}건 ({tel_only['amt'].sum():,.0f}원)")
print(f"  키워드만:   {len(kw_only)}건 ({kw_only['amt'].sum():,.0f}원)")
print(f"  둘 다:      {len(both)}건 ({both['amt'].sum():,.0f}원)")
