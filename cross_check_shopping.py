import pandas as pd
import sqlite3
import sys

excel_path = r'C:/Users/COMTREE/Desktop/연습/부산광역시 테스트 종합쇼핑몰 납품요구 물품 내역{20년 1월이후자료(조회속도향상)}.xlsx'
db_path = 'procurement_contracts.db'

with open('cross_check_result.txt', 'w', encoding='utf-8') as f:
    f.write("=== 종합쇼핑몰 납품요구 데이터 정합성 검증 ===\n\n")
    
    # 1. Excel 데이터 로드
    df_excel = pd.read_excel(excel_path, header=2)
    f.write(f"Excel 컬럼 구조: {list(df_excel.columns)}\n\n")
    
    # 2026년 데이터가 있는지 확인을 위해 납품요구일자 필터링 (컬럼명이 다를 수 있으므로)
    date_col = '납품요구결재일자'
    no_col = '납품요구번호'
    
    df_exc_2026 = pd.DataFrame()
    exc_req_nos = set()
    
    if date_col in df_excel.columns:
        df_excel[date_col] = df_excel[date_col].astype(str).str.replace('-', '')
        df_exc_2026 = df_excel[df_excel[date_col].str.startswith('2026')]
        f.write(f"[Excel] 2026년 데이터 건수 (부산 소재 필터링본): {len(df_exc_2026)}건\n")
        if not df_exc_2026.empty:
            f.write(f"[Excel] 2026년 데이터 날짜 범위: {df_exc_2026[date_col].min()} ~ {df_exc_2026[date_col].max()}\n")
            exc_req_nos = set(df_exc_2026[no_col].astype(str).str.strip())
            f.write(f"[Excel] Unique 납품요구번호 개수: {len(exc_req_nos)}개\n\n")
    else:
        f.write("[Excel] Error: '납품요구일자' 컬럼을 찾을 수 없습니다.\n\n")
        
    # 2. SQLite DB 로드
    conn = sqlite3.connect(db_path)
    df_db = pd.read_sql("SELECT * FROM shopping_cntrct", conn)
    conn.close()
    
    f.write(f"[DB] 전체 적재 건수 (전국): {len(df_db)}건\n")
    
    # DB에서 부산 기관만 추출 (API 응답 필드인 dminsttRgnNm 활용)
    df_db_busan = df_db[df_db['dminsttRgnNm'].str.contains('부산', na=False)]
    f.write(f"[DB] 부산광역시 소재 기관 자체 필터링 건수: {len(df_db_busan)}건\n")
    if not df_db_busan.empty:
        f.write(f"[DB] 날짜 범위: {df_db_busan['dlvrReqRcptDate'].min()} ~ {df_db_busan['dlvrReqRcptDate'].max()}\n")
        db_req_nos = set(df_db_busan['dlvrReqNo'].astype(str).str.strip())
        f.write(f"[DB] Unique 납품요구번호 개수: {len(db_req_nos)}개\n\n")
        
    # 3. 크로스 체크 (비교)
    if not df_exc_2026.empty and not df_db_busan.empty:
        common = exc_req_nos.intersection(db_req_nos)
        only_in_excel = exc_req_nos - db_req_nos
        only_in_db = db_req_nos - exc_req_nos
        
        f.write("=== 교차 검증 결과 ===\n")
        f.write(f"1) 엑셀과 DB에 모두 존재하는 고유 납품요구번호: {len(common)}건\n")
        f.write(f"2) 엑셀에만 있고 DB에는 없는 납품요구번호: {len(only_in_excel)}건\n")
        f.write(f"3) DB에만 있고 엑셀에는 없는 납품요구번호: {len(only_in_db)}건\n")
        
        # 날짜 범위 맞추기
        min_date = df_exc_2026[date_col].min()
        max_date = df_exc_2026[date_col].max()
        df_db_matched_date = df_db_busan[(df_db_busan['dlvrReqRcptDate'] >= min_date) & (df_db_busan['dlvrReqRcptDate'] <= max_date)]
        
        matched_db_req_nos = set(df_db_matched_date['dlvrReqNo'].astype(str).str.strip())
        matched_common = exc_req_nos.intersection(matched_db_req_nos)
        
        f.write("\n=== 날짜 구간(엑셀데이터 기간 기준) 동기화 후 정밀 비교 ===\n")
        f.write(f"엑셀의 기간 ({min_date} ~ {max_date}) 내에서 추출 시:\n")
        f.write(f"엑셀의 고유 납품요구번호: {len(exc_req_nos)}건\n")
        f.write(f"DB의 부산기관 고유 납품요구번호: {len(matched_db_req_nos)}건\n")
        f.write(f"-> 교집합 완전히 일치하는 건수: {len(matched_common)}건\n")
        
        if len(only_in_excel) > 0:
            f.write(f"※ 왜 엑셀에만 있는 데이터가 있을까? (상세 조회용으로 3개 무작위 추출): {list(only_in_excel)[:3]}\n")
