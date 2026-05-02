import sqlite3
import pandas as pd
import os
import hmac
import hashlib
from datetime import datetime
import sys

TARGET_DB = os.environ.get("CHATBOT_DB", "staging_chatbot_company.db")
SECRET_KEY = os.environ.get("COMPANY_ID_HMAC_SECRET")

def hash_business_no(bno: str) -> str:
    if not bno: return ""
    bno = str(bno).replace('-', '').replace('.0', '').strip()
    return hmac.new(SECRET_KEY.encode('utf-8'), bno.encode('utf-8'), hashlib.sha256).hexdigest()[:32]

def log_etl(conn, job_name, source_name, input_count, inserted_count, skipped_count=0, msg=""):
    conn.execute("""
        INSERT INTO etl_job_log (job_name, source_name, started_at, finished_at, status, input_row_count, inserted_count, skipped_count, error_count, error_message)
        VALUES (?, ?, datetime('now'), datetime('now'), 'success', ?, ?, ?, 0, ?)
    """, (job_name, source_name, input_count, inserted_count, skipped_count, msg))
    
    conn.execute("""
        INSERT INTO source_manifest (source_name, source_type, source_refreshed_at, row_count, status, error_message)
        VALUES (?, 'file_migration', datetime('now'), ?, 'success', ?)
        ON CONFLICT(source_name) DO UPDATE SET row_count=excluded.row_count, source_refreshed_at=excluded.source_refreshed_at, error_message=excluded.error_message
    """, (source_name, inserted_count, msg))

def find_header_row(file_path, key_column="사업자등록번호"):
    # 첫 20줄 이내에서 key_column이 포함된 행을 헤더로 찾음
    try:
        df_head = pd.read_excel(file_path, nrows=20, header=None)
        for idx, row in df_head.iterrows():
            if any(key_column in str(val).replace(" ", "") for val in row.values):
                return idx
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    return 0

def get_internal_id_by_hash(conn, comp_hash):
    cur = conn.cursor()
    cur.execute("SELECT company_internal_id FROM company_identity WHERE company_id = ?", (comp_hash,))
    res = cur.fetchone()
    return res[0] if res else None

def load_policy(conn, file_path, policy_subtype, source_name):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        log_etl(conn, f'bootstrap_policy_{policy_subtype}', source_name, 0, 0, 0, "not_available/skipped: File not found")
        return
        
    print(f"Loading {policy_subtype} from {file_path}...")
    header_idx = find_header_row(file_path, "사업자등록번호")
    df = pd.read_excel(file_path, skiprows=header_idx)
    df.columns = [str(c).replace(" ", "").replace("\n", "") for c in df.columns]
    
    if '사업자등록번호' not in df.columns:
        print(f"Error: 사업자등록번호 column not found in {file_path}")
        return
        
    inserted = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for _, row in df.iterrows():
        bno = str(row['사업자등록번호'])
        if bno == 'nan' or not bno.strip(): continue
        comp_hash = hash_business_no(bno)
        internal_id = get_internal_id_by_hash(conn, comp_hash)
        
        if not internal_id:
            continue
            
        conn.execute("DELETE FROM policy_company_certification WHERE company_internal_id = ? AND policy_subtype = ?", (internal_id, policy_subtype))
        conn.execute("""
            INSERT INTO policy_company_certification (
                company_internal_id, policy_type, policy_subtype, validity_status, source_name, source_refreshed_at
            ) VALUES (?, 'policy_company', ?, 'valid', ?, ?)
        """, (internal_id, policy_subtype, source_name, now))
        inserted += 1
        
    conn.commit()
    real_count = conn.execute("SELECT COUNT(*) FROM policy_company_certification WHERE policy_subtype=? AND source_name=?", (policy_subtype, source_name)).fetchone()[0]
    print(f"Inserted {real_count} {policy_subtype} records.")
    log_etl(conn, f'bootstrap_policy_{policy_subtype}', source_name, len(df), real_count)

def load_manufacturer(conn, file_path, source_name):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        log_etl(conn, 'bootstrap_manufacturer', source_name, 0, 0, 0, "not_available/skipped: File not found")
        return
        
    print(f"Loading manufacturers from {file_path}...")
    header_idx = find_header_row(file_path, "사업자등록번호")
    df = pd.read_excel(file_path, skiprows=header_idx)
    df.columns = [str(c).replace(" ", "").replace("\n", "") for c in df.columns]
    
    if '사업자등록번호' not in df.columns:
        print(f"Error: 사업자등록번호 column not found in {file_path}")
        return
        
    inserted = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for _, row in df.iterrows():
        bno = str(row['사업자등록번호'])
        if bno == 'nan' or not bno.strip(): continue
        comp_hash = hash_business_no(bno)
        internal_id = get_internal_id_by_hash(conn, comp_hash)
        
        if not internal_id:
            continue
            
        conn.execute("DELETE FROM company_manufacturer_status WHERE company_internal_id = ?", (internal_id,))
        conn.execute("""
            INSERT INTO company_manufacturer_status (
                company_internal_id, manufacturer_type, manufacturer_label, evidence_source, validity_status, source_refreshed_at
            ) VALUES (?, 'manufacture', '제조(직접생산)', ?, 'valid', ?)
        """, (internal_id, source_name, now))
        inserted += 1
        
    conn.commit()
    real_count = conn.execute("SELECT COUNT(*) FROM company_manufacturer_status WHERE evidence_source=?", (source_name,)).fetchone()[0]
    print(f"Inserted {real_count} manufacturer records.")
    log_etl(conn, 'bootstrap_manufacturer', source_name, len(df), real_count)


def load_sme_competition(conn, file_path, source_name):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        log_etl(conn, 'bootstrap_sme_competition', source_name, 0, 0, 0, "not_available")
        return
    
    print(f"Loading SME competition products from {file_path}...")
    df = pd.read_excel(file_path, skiprows=4)
    inserted = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for _, row in df.iterrows():
        try:
            cat_name = str(row.iloc[0]).strip()
            dtl_code = str(row.iloc[3]).strip()
            dtl_name = str(row.iloc[2]).strip()
            valid_start = str(row.iloc[4]).strip()
            valid_end = str(row.iloc[5]).strip()
            
            if dtl_code == 'nan' or not dtl_code: continue
            
            # Remove .0 if it's float parsed
            dtl_code = dtl_code.replace('.0', '')
            
            conn.execute("""
                INSERT OR REPLACE INTO ref_sme_competition_product (
                    detail_category_code, category_name, detail_category_name, 
                    sme_competition_target, direct_purchase_target,
                    valid_start_date, valid_end_date, source_name, source_refreshed_at
                ) VALUES (?, ?, ?, 1, 0, ?, ?, ?, ?)
            """, (dtl_code, cat_name, dtl_name, valid_start, valid_end, source_name, now))
            inserted += 1
        except Exception as e:
            continue
    conn.commit()
    real_count = conn.execute("SELECT COUNT(*) FROM ref_sme_competition_product WHERE source_name=?", (source_name,)).fetchone()[0]
    print(f"Inserted {real_count} SME competition records.")
    log_etl(conn, 'bootstrap_sme_competition', source_name, len(df), real_count)

def load_innovation(conn, file_path, source_name):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        log_etl(conn, 'bootstrap_innovation', source_name, 0, 0, 0, "not_available")
        return
        
    print(f"Loading innovation products from {file_path}...")
    df = pd.read_excel(file_path, skiprows=4)
    
    inserted = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for _, row in df.iterrows():
        try:
            bno = str(row.get('업체사업자등록번호', '')).strip()
            if bno == 'nan' or not bno: continue
            comp_hash = hash_business_no(bno)
            internal_id = get_internal_id_by_hash(conn, comp_hash)
            if not internal_id: continue
            
            p_name = str(row.get('세부품명', '')).strip()
            
            conn.execute("""
                INSERT INTO certified_product (
                    company_internal_id, certification_type, certification_type_label,
                    product_name, product_name_normalized, validity_status, source_name, source_refreshed_at
                ) VALUES (?, 'innovation_product', '혁신제품', ?, ?, 'unknown', ?, ?)
            """, (internal_id, p_name, p_name.replace(' ', ''), source_name, now))
            inserted += 1
        except Exception as e:
            continue
    conn.commit()
    real_count = conn.execute("SELECT COUNT(*) FROM certified_product WHERE certification_type='innovation_product' AND source_name=?", (source_name,)).fetchone()[0]
    print(f"Inserted {real_count} innovation product records.")
    log_etl(conn, 'bootstrap_innovation', source_name, len(df), real_count)

def load_mas(conn, file_path, source_name):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        log_etl(conn, 'bootstrap_mas_excel', source_name, 0, 0, 0, "not_available")
        return
        
    print(f"Loading MAS products from {file_path}...")
    df = pd.read_excel(file_path, skiprows=4)
    
    inserted_product = 0
    inserted_contract = 0
    inserted_price = 0
    inserted_cert = 0
    inserted_attr = 0
    inserted_general = 0
    inserted_review = 0
    active_count = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_date = datetime.now().strftime("%Y%m%d")
    
    # procurement_label_map 로드
    label_map = {}
    for row in conn.execute("SELECT raw_label, target_domain, target_type, is_candidate_type_promotable FROM procurement_label_map WHERE is_active=1"):
        label_map[row[0]] = {'domain': row[1], 'type': row[2], 'promotable': row[3]}
        
    for _, row in df.iterrows():
        try:
            bno = str(row.get('업체사업자등록번호', '')).strip()
            if bno == 'nan' or not bno: continue
            comp_hash = hash_business_no(bno)
            internal_id = get_internal_id_by_hash(conn, comp_hash)
            if not internal_id: continue
            
            contract_no = str(row.get('계약번호', '')).strip()
            if contract_no == 'nan' or not contract_no: continue
            cno_hash = hashlib.sha256(contract_no.encode('utf-8')).hexdigest()[:16]
            
            p_name = str(row.get('품명', '')).strip()
            if p_name == 'nan': p_name = ''
            p_code = str(row.get('물품분류번호', '')).strip()
            if p_code == 'nan': p_code = ''
            dp_name = str(row.get('세부품명', '')).strip()
            if dp_name == 'nan': dp_name = ''
            dp_code = str(row.get('세부품명번호', '')).strip()
            if dp_code == 'nan': dp_code = ''
            g2b_cat = str(row.get('대분류쇼핑카테고리', '')).strip()
            if g2b_cat == 'nan': g2b_cat = ''
            
            p_name_norm = p_name.replace(' ', '')
            
            price_val = 0
            try:
                price_val = float(row.get('단가', 0))
            except:
                pass
            unit = str(row.get('단위', '')).strip()
            if unit == 'nan': unit = ''
            
            contract_period = str(row.get('계약기간', '')).strip()
            contract_start = ""
            contract_end = ""
            c_status = "unknown"
            if "~" in contract_period:
                parts = contract_period.split("~")
                if len(parts) == 2:
                    contract_start = parts[0].strip()
                    contract_end = parts[1].strip()
                    if contract_end and current_date <= contract_end:
                        c_status = "active"
                    elif contract_end and current_date > contract_end:
                        c_status = "expired"

            if c_status == "active":
                active_count += 1
            
            # mas_product (ON CONFLICT)
            conn.execute("""
                INSERT INTO mas_product (
                    company_internal_id, product_name, product_name_normalized, product_code,
                    detail_product_name, detail_product_code, g2b_category_code, contract_no_hash,
                    contract_status, price_amount, price_unit, source_name, source_refreshed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(company_internal_id, contract_no_hash, product_name_normalized, detail_product_code, source_name) 
                DO UPDATE SET 
                    contract_status=excluded.contract_status,
                    price_amount=excluded.price_amount,
                    source_refreshed_at=excluded.source_refreshed_at
            """, (internal_id, p_name, p_name_norm, p_code, dp_name, dp_code, g2b_cat, cno_hash, c_status, price_val, unit, source_name, now))
            
            mp_id = conn.execute("""
                SELECT mas_product_id FROM mas_product 
                WHERE company_internal_id=? AND contract_no_hash=? AND product_name_normalized=? AND detail_product_code=? AND source_name=?
            """, (internal_id, cno_hash, p_name_norm, dp_code, source_name)).fetchone()[0]
            
            inserted_product += 1
            
            # mas_contract (ON CONFLICT)
            conn.execute("""
                INSERT INTO mas_contract (
                    company_internal_id, contract_no_hash, product_name, product_code,
                    detail_product_name, detail_product_code, contract_start_date, contract_end_date,
                    contract_status, source_name, source_refreshed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(company_internal_id, contract_no_hash, product_code, detail_product_code, source_name)
                DO UPDATE SET contract_status=excluded.contract_status, source_refreshed_at=excluded.source_refreshed_at
            """, (internal_id, cno_hash, p_name, p_code, dp_name, dp_code, contract_start, contract_end, c_status, source_name, now))
            inserted_contract += 1

            # mas_price_condition (ON CONFLICT)
            conn.execute("""
                INSERT INTO mas_price_condition (
                    mas_product_id, price_amount, price_unit, source_name, source_refreshed_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(mas_product_id, source_name)
                DO UPDATE SET price_amount=excluded.price_amount, source_refreshed_at=excluded.source_refreshed_at
            """, (mp_id, price_val, unit, source_name, now))
            inserted_price += 1
            
            # 물품인증유형목록 분류 (Phase 6-D-2)
            cert_list = str(row.get('물품인증유형목록', '')).strip()
            if cert_list and cert_list != 'nan':
                for cert in cert_list.split(','):
                    c = cert.strip()
                    if not c: continue
                    
                    mapping = label_map.get(c)
                    
                    if mapping:
                        domain = mapping['domain']
                        target_type = mapping['type']
                        
                        if domain == 'product_certification':
                            # surrogate hash for certification_no_hash
                            surr = hashlib.sha256(
                                f"{source_name}|{c}|{internal_id}|{p_name_norm}|{dp_code}".encode('utf-8')
                            ).hexdigest()[:16]
                            
                            conn.execute("""
                                INSERT OR IGNORE INTO certified_product (
                                    company_internal_id, certification_type, certification_type_label,
                                    certification_no_hash, product_name, product_name_normalized, 
                                    validity_status, source_name, source_refreshed_at
                                ) VALUES (?, ?, ?, ?, ?, ?, 'unknown', ?, ?)
                            """, (internal_id, target_type, c, surr, p_name, p_name_norm, source_name, now))
                            inserted_cert += 1
                            
                        elif domain == 'company_procurement_attribute':
                            conn.execute("""
                                INSERT OR IGNORE INTO company_procurement_attribute (
                                    company_internal_id, attribute_type, attribute_label,
                                    product_name, product_code, detail_product_code,
                                    source_name, source_refreshed_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """, (internal_id, target_type, c, p_name, p_code, dp_code, source_name, now))
                            inserted_attr += 1
                            
                        elif domain == 'general_certification':
                            conn.execute("""
                                INSERT OR IGNORE INTO product_general_certification (
                                    company_internal_id, raw_cert_label, normalized_cert_type,
                                    product_name, product_code, detail_product_code,
                                    source_name, source_refreshed_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """, (internal_id, c, target_type, p_name, p_code, dp_code, source_name, now))
                            inserted_general += 1
                            
                        elif domain == 'ignore':
                            pass
                            
                    else:
                        # 매핑 없음 -> procurement_label_mapping_review
                        conn.execute("""
                            INSERT INTO procurement_label_mapping_review (
                                raw_label, product_name, product_code, detail_product_code,
                                company_internal_id, source_name, reason
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (c, p_name, p_code, dp_code, internal_id, source_name, 'unmapped'))
                        inserted_review += 1
                        
        except Exception as e:
            print("Error parsing row:", e)
            continue
            
    conn.commit()
    
    real_prod = conn.execute("SELECT COUNT(*) FROM mas_product WHERE source_name=?", (source_name,)).fetchone()[0]
    real_cont = conn.execute("SELECT COUNT(*) FROM mas_contract WHERE source_name=?", (source_name,)).fetchone()[0]
    real_price = conn.execute("SELECT COUNT(*) FROM mas_price_condition WHERE source_name=?", (source_name,)).fetchone()[0]
    real_cert = conn.execute("SELECT COUNT(*) FROM certified_product WHERE source_name=?", (source_name,)).fetchone()[0]
    real_attr = conn.execute("SELECT COUNT(*) FROM company_procurement_attribute WHERE source_name=?", (source_name,)).fetchone()[0]
    real_general = conn.execute("SELECT COUNT(*) FROM product_general_certification WHERE source_name=?", (source_name,)).fetchone()[0]
    real_review = conn.execute("SELECT COUNT(*) FROM procurement_label_mapping_review WHERE source_name=?", (source_name,)).fetchone()[0]
    
    print(f"MAS Results:")
    print(f"  Products: {real_prod}, Contracts: {real_cont}, Prices: {real_price}")
    print(f"  Product Certs: {real_cert}, Procurement Attrs: {real_attr}, General Certs: {real_general}, Review: {real_review}")
    print(f"  Active Contracts: {active_count}")
    
    log_etl(conn, 'bootstrap_mas_excel', source_name, len(df), real_prod)
    log_etl(conn, 'bootstrap_mas_procurement_attr', 'mas_procurement_attr', 0, real_attr)
    log_etl(conn, 'bootstrap_mas_general_cert', 'mas_general_cert', 0, real_general)
    log_etl(conn, 'bootstrap_mas_mapping_review', 'mas_mapping_review', 0, real_review)
    
    if real_prod > 0 and active_count == 0:
        print("FAIL: MAS active count is 0. Aborting.")
        sys.exit(1)

def log_missing_source(conn, job_name, source_name):
    log_etl(conn, job_name, source_name, 0, 0, 0, "not_available/skipped: File not found or API limits.")

def main():
    if not SECRET_KEY:
        print("ERROR: COMPANY_ID_HMAC_SECRET is not set. Bootstrap failed.")
        sys.exit(1)
        
    conn = sqlite3.connect(TARGET_DB)
    
    load_manufacturer(conn, "제조 조달업체 등록 내역.xlsx", "bootstrap_manufacturer_excel")
    load_policy(conn, "여성 조달업체 등록 내역.xlsx", "women_company", "bootstrap_policy_women_excel")
    load_policy(conn, "사회적 조달업체 등록 내역.xlsx", "social_enterprise", "bootstrap_policy_social_excel")
    load_policy(conn, "장애인 조달업체 등록 내역.xlsx", "disabled_company", "bootstrap_policy_disabled_excel")
    
    load_sme_competition(conn, "UI-ADOSAA-006R.중소기업 경쟁제품 및 공사용자재 내역.xlsx", "ref_sme_competition_product")
    load_innovation(conn, ".혁신장터 상품등록 내역.xlsx", "innovation_market_excel")
    
    mas_excel_path = r"C:\Users\doors\OneDrive\바탕 화면\종합쇼핑몰 부산광역시 기업 품목 등록 내역.xlsx"
    load_mas(conn, mas_excel_path, "mas_excel_bootstrap")

    
    # MAS & Certified skipped if not present in easy excel format
    
    log_missing_source(conn, 'bootstrap_certified', 'bootstrap_certified_excel')
    
    # Record manifest
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("""
        INSERT INTO source_manifest (source_name, source_type, source_refreshed_at, row_count, status)
        VALUES ('bootstrap_excel', 'file_migration', ?, 1, 'success')
        ON CONFLICT(source_name) DO UPDATE SET source_refreshed_at=excluded.source_refreshed_at
    """, (now,))
    
    conn.commit()
    conn.close()
    print("Excel bootstrap completed.")

if __name__ == "__main__":
    main()
