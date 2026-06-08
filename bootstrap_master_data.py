import sqlite3
import os
import re
import hmac
import hashlib
from datetime import datetime
import sys

SOURCE_DB = 'busan_companies_master.db'
TARGET_DB = os.environ.get("CHATBOT_DB", "staging_chatbot_company.db")
SECRET_KEY = os.environ.get("COMPANY_ID_HMAC_SECRET")

def normalize_string(val: str) -> str:
    if not val: return ""
    val = re.sub(r'\(주\)|\(유\)|\(합\)', '', str(val))
    val = re.sub(r'주식회사|유한회사|합자회사', '', val)
    val = re.sub(r'\s+', '', val)
    return val.strip()

def hash_business_no(bno: str) -> str:
    if not bno: return ""
    bno = str(bno).replace('-', '').strip()
    return hmac.new(SECRET_KEY.encode('utf-8'), bno.encode('utf-8'), hashlib.sha256).hexdigest()[:32]

def map_license_status(status: str) -> str:
    status = str(status or "").strip()
    if any(token in status for token in ["유효기간 경과", "만료"]):
        return "expired"
    if any(token in status for token in ["취소", "말소", "폐지", "정지"]):
        return "inactive"
    return "valid"

def log_etl(cursor, job_name, source_name, input_count, inserted_count):
    cursor.execute("""
        INSERT INTO etl_job_log (job_name, source_name, started_at, finished_at, status, input_row_count, inserted_count, skipped_count, error_count)
        VALUES (?, ?, datetime('now'), datetime('now'), 'success', ?, ?, ?, 0)
    """, (job_name, source_name, input_count, inserted_count, input_count - inserted_count))
    
    cursor.execute("""
        INSERT INTO source_manifest (source_name, source_type, source_refreshed_at, row_count, status)
        VALUES (?, 'db_migration', datetime('now'), ?, 'success')
        ON CONFLICT(source_name) DO UPDATE SET row_count=excluded.row_count, source_refreshed_at=excluded.source_refreshed_at
    """, (source_name, inserted_count))

def bootstrap_master():
    if not SECRET_KEY:
        print("ERROR: COMPANY_ID_HMAC_SECRET is not set. Bootstrap failed.")
        sys.exit(1)
        
    print(f"Starting bootstrap from {SOURCE_DB} to {TARGET_DB}")
    if not os.path.exists(SOURCE_DB):
        print(f"Error: {SOURCE_DB} does not exist.")
        sys.exit(1)
        
    s_conn = sqlite3.connect(SOURCE_DB)
    s_conn.row_factory = sqlite3.Row
    s_cursor = s_conn.cursor()
    
    t_conn = sqlite3.connect(TARGET_DB)
    t_cursor = t_conn.cursor()
    
    # Check if company_industry exists in source DB
    s_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='company_industry'")
    has_industry_table = s_cursor.fetchone() is not None
    
    # 1. Fetch master data
    s_cursor.execute("""
        SELECT bizno, corpNm, ceoNm, rgnNm, adrs, dtlAdrs, hdoffceDivNm, corpBsnsDivNm, mnfctDivNm, rprsntIndstrytyNm, rprsntDtlPrdnm, rprsntDtlPrdnmNo
        FROM company_master
        WHERE rgnNm LIKE '부산%' AND hdoffceDivNm = '본사'
    """)
    rows = s_cursor.fetchall()
    print(f"Found {len(rows)} busan HQ companies in source DB.")
    
    inserted_master = 0
    updated_master = 0
    inserted_license = 0
    inserted_product = 0
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    t_cursor.execute(
        "DELETE FROM company_license WHERE license_source = 'bootstrap_company_license'"
    )
    deleted_license = t_cursor.rowcount
    
    for row in rows:
        bizno = row['bizno']
        if not bizno: continue
        bizno_clean = str(bizno).replace('-', '').strip()
        
        corp_name = row['corpNm'] or ""
        rgn_nm = row['rgnNm'] or ""
        sido = "부산광역시"
        sigungu = rgn_nm.replace("부산광역시", "").strip().split(" ")[0] if rgn_nm else ""
        
        adrs = row['adrs'] or ""
        dtl = row['dtlAdrs'] or ""
        loc_detail = f"{adrs} {dtl}".strip()
        
        t_cursor.execute("SELECT company_internal_id FROM company_identity WHERE canonical_business_no = ?", (bizno_clean,))
        existing = t_cursor.fetchone()
        normalized = normalize_string(corp_name)

        if existing:
            internal_id = existing[0]
            t_cursor.execute("""
                UPDATE company_master
                SET company_name = ?,
                    company_name_normalized = ?,
                    location_sido = ?,
                    location_sigungu = ?,
                    location_detail = ?,
                    is_busan_company = 1,
                    is_headquarters = 1,
                    busan_classification_reason = 'bootstrap_master',
                    display_location = ?,
                    data_status = 'active',
                    source_refreshed_at = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE company_internal_id = ?
            """, (corp_name, normalized, sido, sigungu, loc_detail, rgn_nm, now, internal_id))
            updated_master += 1
        else:
            t_cursor.execute("""
                INSERT INTO company_master (
                    company_name, company_name_normalized, location_sido, location_sigungu,
                    location_detail, is_busan_company, is_headquarters, busan_classification_reason,
                    display_location, data_status, source_refreshed_at
                ) VALUES (?, ?, ?, ?, ?, 1, 1, 'bootstrap_master', ?, 'active', ?)
            """, (corp_name, normalized, sido, sigungu, loc_detail, rgn_nm, now))

            internal_id = t_cursor.lastrowid

            comp_id = hash_business_no(bizno_clean)
            t_cursor.execute("""
                INSERT INTO company_identity (
                    company_internal_id, canonical_business_no, company_id, identity_source, identity_status
                ) VALUES (?, ?, ?, 'busan_companies_master', 'verified')
            """, (internal_id, bizno_clean, comp_id))

            inserted_master += 1
        
        # Insert license from company_industry table if it exists
        if has_industry_table:
            s_cursor.execute("SELECT indstrytyCd, indstrytyNm, rprsntIndstrytyYn, indstrytyStatsNm, rgstDt, vldPrdExprtDt FROM company_industry WHERE bizno = ?", (bizno,))
            industry_rows = s_cursor.fetchall()
            for ind_row in industry_rows:
                l_name = ind_row['indstrytyNm']
                l_code = ind_row['indstrytyCd']
                is_rep = 1 if ind_row['rprsntIndstrytyYn'] == 'Y' else 0
                validity_status = map_license_status(ind_row['indstrytyStatsNm'])
                valid_to = ind_row['vldPrdExprtDt'] or None
                if l_name:
                    t_cursor.execute("""
                        INSERT INTO company_license (
                            company_internal_id, license_name, license_name_normalized, license_code,
                            is_representative_license, license_source, validity_status, valid_to, source_refreshed_at
                        ) VALUES (?, ?, ?, ?, ?, 'bootstrap_company_license', ?, ?, ?)
                    """, (internal_id, l_name, normalize_string(l_name), l_code, is_rep, validity_status, valid_to, now))
                    inserted_license += 1
        else:
            indstry = row['rprsntIndstrytyNm']
            if indstry:
                for l_name in indstry.split(','):
                    l_name = l_name.strip()
                    if not l_name: continue
                    t_cursor.execute("""
                        INSERT INTO company_license (
                            company_internal_id, license_name, license_name_normalized, is_representative_license, license_source, validity_status, source_refreshed_at
                        ) VALUES (?, ?, ?, 1, 'bootstrap_company_license', 'valid', ?)
                    """, (internal_id, l_name, normalize_string(l_name), now))
                    inserted_license += 1
                
        # Insert product from master table for new companies only. Existing products are maintained by separate product ETLs.
        prdnm = row['rprsntDtlPrdnm']
        prdno = row['rprsntDtlPrdnmNo']
        if prdnm and not existing:
            prd_names = [p.strip() for p in str(prdnm).split(',')]
            prd_nos = [p.strip() for p in str(prdno).split(',')] if prdno else []
            for i, p_name in enumerate(prd_names):
                if not p_name: continue
                p_code = prd_nos[i] if i < len(prd_nos) else None
                g2b_cat = p_code[:6] if p_code and len(p_code) >= 6 else None
                t_cursor.execute("""
                    INSERT INTO company_product (
                        company_internal_id, product_name, product_name_normalized, product_code, g2b_category_code, is_representative_product, product_source, source_refreshed_at
                    ) VALUES (?, ?, ?, ?, ?, 1, 'bootstrap_company_product', ?)
                """, (internal_id, p_name, normalize_string(p_name), p_code, g2b_cat, now))
                inserted_product += 1

    log_etl(t_cursor, 'bootstrap_master', 'bootstrap_master_db', len(rows), inserted_master)
    log_etl(t_cursor, 'bootstrap_license', 'bootstrap_company_license', inserted_license, inserted_license)
    log_etl(t_cursor, 'bootstrap_product', 'bootstrap_company_product', inserted_product, inserted_product)

    t_conn.commit()
    print(
        f"Synced {len(rows)} companies: inserted {inserted_master}, updated {updated_master}. "
        f"Rebuilt bootstrap licenses: deleted {deleted_license}, inserted {inserted_license}. "
        f"Inserted {inserted_product} products for new companies."
    )
    
    t_conn.close()
    s_conn.close()
    
    # 기존 데이터가 이미 다 적재되어 추가된 건이 없을 수 있으므로 정상 처리하되 알림만 출력
    if inserted_license == 0:
        print("WARNING: No company_license was inserted from company_industry.")
        
    return True

if __name__ == "__main__":
    bootstrap_master()
