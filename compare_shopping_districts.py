import pandas as pd
import sqlite3
import sys

sys.stdout.reconfigure(encoding='utf-8')

files = [
    (r'C:\Users\COMTREE\Desktop\연습\부산광역시강서구종합쇼핑몰 납품요구 물품 내역{20년 1월이후자료(조회속도향상)}.xlsx', '강서구'),
    (r'C:\Users\COMTREE\Desktop\연습\부산광역시서구종합쇼핑몰 납품요구 물품 내역{20년 1월이후자료(조회속도향상)}.xlsx', '서구'),
]

conn = sqlite3.connect('procurement_contracts.db')
conn_ag = sqlite3.connect('busan_agencies_master.db')
df_ag = pd.read_sql("SELECT dminsttCd, cate_lrg, cate_mid FROM agency_master", conn_ag)
conn_ag.close()
master_codes = set(df_ag['dminsttCd'].astype(str).str.strip())

for filepath, label in files:
    print("=" * 70)
    print(f"📊 [{label}] 종합쇼핑몰 비교")
    print("=" * 70)
    
    # 헤더 찾기
    df_raw = pd.read_excel(filepath, header=None, nrows=15)
    header_row = None
    for i, row in df_raw.iterrows():
        if any('납품요구번호' == str(v).strip() for v in row.values):
            header_row = i
            break
    
    if header_row is None:
        for i, row in df_raw.iterrows():
            if any('납품요구' in str(v) for v in row.values):
                header_row = i
                break
    
    print(f"  헤더 행: {header_row}")
    df = pd.read_excel(filepath, header=header_row)
    print(f"  엑셀 행수: {len(df):,}")
    
    # 수요기관 확인
    agency_codes = df['수요기관코드'].dropna().astype(str).str.strip().unique() if '수요기관코드' in df.columns else []
    agency_names = dict(zip(df['수요기관코드'].astype(str).str.strip(), df['수요기관명'])) if '수요기관명' in df.columns else {}
    
    print(f"  수요기관 수: {len(agency_codes)}개")
    
    # 마스터DB 등록 확인
    missing = [c for c in agency_codes if c not in master_codes]
    if missing:
        print(f"  ❌ 미등록 기관: {len(missing)}개")
        for mc in missing:
            nm = agency_names.get(mc, '?')
            print(f"    - {mc}: {nm}")
    else:
        print(f"  ✅ 전체 {len(agency_codes)}개 기관 마스터 등록 완료")
    
    # DB 비교
    all_codes_str = "','".join(agency_codes)
    df_db = pd.read_sql(f"""
        SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, prdctAmt, dlvrReqAmt, dminsttCd
        FROM shopping_cntrct 
        WHERE dlvrReqRcptDate >= '2026-01-01' AND dlvrReqRcptDate <= '2026-02-28'
          AND dminsttCd IN ('{all_codes_str}')
    """, conn)
    
    # 중복 제거
    df_db.sort_values('dlvrReqChgOrd', ascending=False, inplace=True)
    df_db_dedup = df_db.drop_duplicates(subset=['dlvrReqNo', 'prdctSno'], keep='first').copy()
    
    # 금액 비교
    df_db_dedup['prdctAmt_num'] = pd.to_numeric(df_db_dedup['prdctAmt'], errors='coerce')
    db_total = df_db_dedup['prdctAmt_num'].sum()
    
    excel_total = pd.to_numeric(df['납품금액'], errors='coerce').sum() if '납품금액' in df.columns else 0
    
    print(f"\n  💰 금액 비교:")
    print(f"    DB prdctAmt (중복제거): {db_total:,.0f}원 ({len(df_db_dedup):,}건)")
    print(f"    엑셀 납품금액:          {excel_total:,.0f}원 ({len(df):,}건)")
    print(f"    차이: {abs(db_total - excel_total):,.0f}원")
    
    # 납품요구번호 매칭
    if '납품요구번호' in df.columns:
        excel_reqnos = set(df['납품요구번호'].dropna().astype(str).str.strip())
        db_reqnos = set(df_db_dedup['dlvrReqNo'].dropna().astype(str).str.strip())
        only_excel = excel_reqnos - db_reqnos
        only_db = db_reqnos - excel_reqnos
        
        print(f"\n  📌 납품요구번호: 엑셀 {len(excel_reqnos)}개 / DB {len(db_reqnos)}개")
        print(f"    양쪽 모두: {len(excel_reqnos & db_reqnos)}개 | 엑셀만: {len(only_excel)}개 | DB만: {len(only_db)}개")
    
    print()

conn.close()
