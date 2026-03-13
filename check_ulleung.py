import sqlite3
import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('procurement_contracts.db')

# 1. 울릉공항 건이 cnstwk_cntrct에 어떻게 들어있는지 확인
df_ulleung = pd.read_sql("""
    SELECT untyCntrctNo, cnstwkNm, cntrctInsttNm, cntrctInsttCd, 
           totCntrctAmt, thtmCntrctAmt, ntceNo, cntrctDate
    FROM cnstwk_cntrct 
    WHERE cnstwkNm LIKE '%울릉공항%'
    AND cntrctDate >= '2026-01-01'
""", conn)

print(f"🔍 [울릉공항 관련 공사 건수]: {len(df_ulleung)}건")
for _, r in df_ulleung.iterrows():
    print(f"  - {r['untyCntrctNo']} | {r['cnstwkNm'][:40]} | 발주기관: {r['cntrctInsttNm']} (코드:{r['cntrctInsttCd']}) | ntceNo: {r['ntceNo']}")

# 2. bid_notices_raw에서 해당 건의 현장위치 정보가 있는지 확인
for _, r in df_ulleung.iterrows():
    ntce = str(r['ntceNo']).strip()
    if ntce and ntce != 'nan':
        ntce_clean = ntce.replace('-', '')
        df_bid = pd.read_sql(f"""
            SELECT bidNtceNo, cnstrtsiteRgnNm 
            FROM bid_notices_raw 
            WHERE bidNtceNo LIKE '%{ntce_clean}%' OR bidNtceNo LIKE '%{ntce}%'
        """, conn)
        if df_bid.empty:
            print(f"  ❌ ntceNo={ntce} -> bid_notices_raw에 매칭 데이터 없음 (JOIN 실패 = 필터 우회)")
        else:
            print(f"  ✅ ntceNo={ntce} -> 현장위치: {df_bid['cnstrtsiteRgnNm'].values}")
    else:
        print(f"  ❌ ntceNo가 비어있음 -> JOIN 불가 = 필터 우회")

# 3. 해당 발주기관 코드가 부산 기관 마스터에 있는지 확인
conn_ag = sqlite3.connect('busan_agencies_master.db')
for _, r in df_ulleung.iterrows():
    cd = str(r['cntrctInsttCd']).strip()
    df_ag = pd.read_sql(f"SELECT dminsttCd, dminsttNm FROM agency_master WHERE dminsttCd = '{cd}'", conn_ag)
    if not df_ag.empty:
        print(f"  ⚠️ 발주기관 코드({cd}): 부산기관 마스터에 [{df_ag['dminsttNm'].values[0]}]으로 등록됨 → 부산 통계에 포함되는 원인")
    else:
        print(f"  ✅ 발주기관 코드({cd}): 부산기관 마스터에 없음")
conn_ag.close()

conn.close()
