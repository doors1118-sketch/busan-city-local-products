import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn_ag = sqlite3.connect('busan_agencies_master.db')
df_ag = pd.read_sql("SELECT dminsttCd FROM agency_master", conn_ag)
conn_ag.close()
busan_codes = set(df_ag['dminsttCd'].astype(str).str.strip())

conn = sqlite3.connect('procurement_contracts.db')
df = pd.read_sql("""
    SELECT untyCntrctNo, cntrctNm, cntrctInsttNm, cntrctInsttCd, 
           cntrctInsttOfclTelNo, cntrctInsttChrgDeptNm, thtmCntrctAmt
    FROM servc_cntrct 
    WHERE cntrctDate >= '2026-01-01' AND cntrctDate <= '2026-01-31'
      AND cntrctInsttOfclTelNo IS NOT NULL AND cntrctInsttOfclTelNo != ''
""", conn)
conn.close()

# 부산 기관 소속 용역만
df['inst_cd'] = df['cntrctInsttCd'].astype(str).str.strip()
df_busan = df[df['inst_cd'].isin(busan_codes)]

# 전화번호에서 지역번호 추출
def get_area_code(tel):
    tel = str(tel).strip().replace(' ', '')
    if tel.startswith('051'): return '051(부산)'
    elif tel.startswith('02'): return '02(서울)'
    elif tel.startswith('031'): return '031(경기)'
    elif tel.startswith('032'): return '032(인천)'
    elif tel.startswith('033'): return '033(강원)'
    elif tel.startswith('041'): return '041(충남)'
    elif tel.startswith('042'): return '042(대전)'
    elif tel.startswith('043'): return '043(충북)'
    elif tel.startswith('044'): return '044(세종)'
    elif tel.startswith('052'): return '052(울산)'
    elif tel.startswith('053'): return '053(대구)'
    elif tel.startswith('054'): return '054(경북)'
    elif tel.startswith('055'): return '055(경남)'
    elif tel.startswith('061'): return '061(전남)'
    elif tel.startswith('062'): return '062(광주)'
    elif tel.startswith('063'): return '063(전북)'
    elif tel.startswith('064'): return '064(제주)'
    elif tel.startswith('070'): return '070(인터넷)'
    elif tel.startswith('010'): return '010(휴대폰)'
    else: return f'기타({tel[:4]})'

df_busan = df_busan.copy()
df_busan['area_code'] = df_busan['cntrctInsttOfclTelNo'].apply(get_area_code)

print(f"📊 [부산 소속 기관 용역] 26년 1월 - 담당자 전화번호 지역번호 분포")
print(f"전체: {len(df_busan):,}건\n")

area_counts = df_busan['area_code'].value_counts()
for code, cnt in area_counts.items():
    pct = cnt / len(df_busan) * 100
    print(f"  {code}: {cnt:,}건 ({pct:.1f}%)")

# 부산(051)이 아닌 건 샘플
non_busan = df_busan[~df_busan['area_code'].str.startswith('051')]
non_busan = non_busan[~non_busan['area_code'].str.startswith('070')]
non_busan = non_busan[~non_busan['area_code'].str.startswith('010')]

print(f"\n🚨 [051/070/010 아닌 건] 총 {len(non_busan):,}건 (의심 대상)")
non_busan['amt'] = pd.to_numeric(non_busan['thtmCntrctAmt'], errors='coerce').fillna(0)
for _, r in non_busan.head(10).iterrows():
    print(f"  - {r['cntrctInsttNm'][:15]} | {r['cntrctNm'][:25]}... | 📞{r['cntrctInsttOfclTelNo']} | {r['amt']:,.0f}원")
