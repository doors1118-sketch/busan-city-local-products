import sqlite3
import pandas as pd
import sys
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')

DB_PATH = 'busan_agencies_master.db'
CSV_PATH = '부산광역시 조달 수요기관 마스터파일_최종본_수정본.csv'

def clean_bizno(val):
    if pd.isna(val): return ''
    # Convert scientific notation or floats to pure string digits
    s = str(val).split('.')[0].replace('-', '').strip()
    return s if s.isdigit() and len(s) == 10 else ''

def clean_code(val):
    if pd.isna(val): return ''
    return str(val).strip()

def main():
    print("======================================================")
    print(" 🔄 [1회성 마이그레이션] 수요기관 카테고리 3단계 매핑 엔진")
    print("======================================================")
    
    # 1. 엑셀 사전을 DataFrame으로 로드
    df_dict = None
    for enc in ['utf-8-sig', 'euc-kr', 'cp949', 'utf-8']:
        try:
            df_dict = pd.read_csv(CSV_PATH, encoding=enc)
            print(f"[알림] CSV 파일을 '{enc}' 인코딩으로 성공적으로 읽었습니다.")
            break
        except Exception:
            continue
            
    if df_dict is None:
        print(f"CSV 로드 에러: 모든 인코딩(utf-8, euc-kr, cp949)으로 읽기에 실패했습니다. 파일을 다시 저장해주세요.")
        return
        
    df_dict['수요기관코드'] = df_dict['수요기관코드'].apply(clean_code)
    df_dict['수요기관사업자등록번호'] = df_dict['수요기관사업자등록번호'].apply(clean_bizno)
    df_dict['수요기관명'] = df_dict['수요기관명'].astype(str).str.strip()
    
    # 2. SQLite DB에서 전체 수요기관 목록 타겟 로드
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT dminsttCd, dminsttNm, bizno FROM agency_master")
    db_agencies = cursor.fetchall()
    
    print(f"✅ 대상 기관 로드: 마스터 DB {len(db_agencies):,}건 / 사용자 엑셀 사전 {len(df_dict):,}건")
    
    update_data = []
    
    match_code_count = 0
    match_bizno_count = 0
    match_name_count = 0
    unmapped_count = 0
    
    for row in db_agencies:
        db_cd = str(row[0]).strip()
        db_nm = str(row[1]).strip()
        db_biz = str(row[2]).strip()
        
        match_row = None
        
        # [1순위 매칭] 수요기관코드 (Exact Match)
        if db_cd:
            matches = df_dict[df_dict['수요기관코드'] == db_cd]
            if not matches.empty:
                match_row = matches.iloc[0]
                match_code_count += 1
                
        # [2순위 매칭] 사업자등록번호 (Exact Match)
        if match_row is None and db_biz and len(db_biz) == 10:
            matches = df_dict[df_dict['수요기관사업자등록번호'] == db_biz]
            if not matches.empty:
                match_row = matches.iloc[0]
                match_bizno_count += 1
                
        # [3순위 매칭] 기관명 텍스트 (부분/완전 일치)
        if match_row is None and db_nm:
            # 엑셀의 이름이 DB 이름에 포함되거나, DB 이름이 엑셀 이름에 포함될 경우
            for idx, dict_row in df_dict.iterrows():
                dict_nm = dict_row['수요기관명']
                if dict_nm and dict_nm != 'nan':
                    if (dict_nm in db_nm) or (db_nm in dict_nm):
                        match_row = dict_row
                        match_name_count += 1
                        break
                        
        if match_row is not None:
            # 매칭 성공 -> 업데이트 리스트에 추가
            lrg = str(match_row.get('대분류', '')).strip()
            mid = str(match_row.get('중분류', '')).strip()
            sml = str(match_row.get('소분류', '')).strip()
            dtl = str(match_row.get('세부분류', '')).strip()
            if lrg == 'nan': lrg = '미분류'
            
            update_data.append((lrg, mid, sml, dtl, db_cd))
        else:
            unmapped_count += 1
            
    # 3. DB 일괄 업데이트 (Batch UPDATE)
    if update_data:
        cursor.executemany('''
            UPDATE agency_master 
            SET cate_lrg = ?, cate_mid = ?, cate_sml = ?, cate_dtl = ?
            WHERE dminsttCd = ?
        ''', update_data)
        conn.commit()
        
    conn.close()
    
    print("\n[매칭 결과 리포트]")
    print(f"▶ 1순위 (기관코드 일치): {match_code_count:,}건")
    print(f"▶ 2순위 (사업자번호 일치): {match_bizno_count:,}건")
    print(f"▶ 3순위 (이름 부분일치) : {match_name_count:,}건")
    print(f"--------------------------------------------------")
    print(f"✅ 총 매핑 성공: {len(update_data):,}건 완료 (DB 갱신됨)")
    print(f"🚨 매핑 실패(분류 없음): {unmapped_count:,}건")
    print("======================================================")

if __name__ == '__main__':
    main()
