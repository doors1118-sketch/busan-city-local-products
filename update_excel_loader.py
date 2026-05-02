import os
import codecs

with codecs.open('bootstrap_from_excel.py', 'r', 'utf-8') as f:
    content = f.read()

new_functions = """
def load_sme_competition(conn, file_path, source_name):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        log_etl(conn, 'bootstrap_sme_competition', source_name, 0, 0, 0, "not_available")
        return
    
    print(f"Loading SME competition products from {file_path}...")
    df = pd.read_excel(file_path, skiprows=2)
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
            
            conn.execute(\"\"\"
                INSERT OR REPLACE INTO ref_sme_competition_product (
                    detail_category_code, category_name, detail_category_name, 
                    sme_competition_target, direct_purchase_target,
                    valid_start_date, valid_end_date, source_name, source_refreshed_at
                ) VALUES (?, ?, ?, 1, 0, ?, ?, ?, ?)
            \"\"\", (dtl_code, cat_name, dtl_name, valid_start, valid_end, source_name, now))
            inserted += 1
        except Exception as e:
            continue
    conn.commit()
    print(f"Inserted {inserted} SME competition records.")
    log_etl(conn, 'bootstrap_sme_competition', source_name, len(df), inserted)

def load_innovation(conn, file_path, source_name):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        log_etl(conn, 'bootstrap_innovation', source_name, 0, 0, 0, "not_available")
        return
        
    print(f"Loading innovation products from {file_path}...")
    df = pd.read_excel(file_path, skiprows=1)
    
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
            
            conn.execute(\"\"\"
                INSERT INTO certified_product (
                    company_internal_id, certification_type, certification_type_label,
                    product_name, product_name_normalized, validity_status, source_name, source_refreshed_at
                ) VALUES (?, 'innovation_product', '혁신제품', ?, ?, 'unknown', ?, ?)
            \"\"\", (internal_id, p_name, p_name.replace(' ', ''), source_name, now))
            inserted += 1
        except Exception as e:
            continue
    conn.commit()
    print(f"Inserted {inserted} innovation product records.")
    log_etl(conn, 'bootstrap_innovation', source_name, len(df), inserted)

def load_mas(conn, file_path, source_name):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        log_etl(conn, 'bootstrap_mas_excel', source_name, 0, 0, 0, "not_available")
        return
        
    print(f"Loading MAS products from {file_path}...")
    # Find header
    df = pd.read_excel(file_path, skiprows=2)
    
    inserted_product = 0
    inserted_contract = 0
    inserted_price = 0
    inserted_cert = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
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
            p_code = str(row.get('물품분류번호', '')).strip()
            dp_name = str(row.get('세부품명', '')).strip()
            dp_code = str(row.get('세부품명번호', '')).strip()
            g2b_cat = str(row.get('대분류쇼핑카테고리', '')).strip()
            price = row.get('단가', 0)
            unit = str(row.get('단위', '')).strip()
            
            # mas_product
            # Check contract_status based on 쇼핑몰등록여부 / 계약기간? Wait, the user said "active 계약만 shopping_mall_supplier 승격". We can set contract_status='active'
            status = 'active' if str(row.get('쇼핑몰등록여부')) == 'Y' else 'unknown'
            
            conn.execute(\"\"\"
                INSERT INTO mas_product (
                    company_internal_id, product_name, product_name_normalized, product_code,
                    detail_product_name, detail_product_code, g2b_category_code, contract_no_hash,
                    contract_status, price_amount, price_unit, source_name, source_refreshed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            \"\"\", (internal_id, p_name, p_name.replace(' ', ''), p_code, dp_name, dp_code, g2b_cat, cno_hash, status, price, unit, source_name, now))
            mp_id = conn.cursor().lastrowid
            inserted_product += 1
            
            # Certifications
            is_excellent = str(row.get('우수제품여부', '')).strip()
            cert_list = str(row.get('물품인증유형목록', '')).strip()
            
            if is_excellent == 'Y':
                # Map via certified_product_type_map (assume mapped to excellent_procurement_product)
                conn.execute(\"\"\"
                    INSERT INTO certified_product (
                        company_internal_id, certification_type, certification_type_label,
                        product_name, product_name_normalized, validity_status, source_name, source_refreshed_at
                    ) VALUES (?, 'excellent_procurement_product', '우수제품', ?, ?, 'valid', ?, ?)
                \"\"\", (internal_id, p_name, p_name.replace(' ', ''), source_name, now))
                inserted_cert += 1
                
            if cert_list and cert_list != 'nan':
                for cert in cert_list.split(','):
                    c = cert.strip()
                    if not c: continue
                    conn.execute(\"\"\"
                        INSERT INTO certified_product (
                            company_internal_id, certification_type, certification_type_label,
                            product_name, product_name_normalized, validity_status, source_name, source_refreshed_at
                        ) VALUES (?, 'manual_review', ?, ?, ?, 'unknown', ?, ?)
                    \"\"\", (internal_id, c, p_name, p_name.replace(' ', ''), source_name, now))
                    inserted_cert += 1
                    
        except Exception as e:
            print("Error parsing row:", e)
            continue
            
    conn.commit()
    print(f"Inserted MAS: {inserted_product} products, {inserted_cert} certs.")
    log_etl(conn, 'bootstrap_mas_excel', source_name, len(df), inserted_product)
"""

target = "def log_missing_source(conn, job_name, source_name):"
idx = content.find(target)
if idx != -1:
    before = content[:idx]
    after = content[idx:]
    
    # modify main
    main_target = "load_policy(conn, \"장애인 조달업체 등록 내역.xlsx\", \"disabled_company\", \"bootstrap_policy_disabled_excel\")"
    main_idx = after.find(main_target)
    
    main_before = after[:main_idx + len(main_target)]
    main_after = after[main_idx + len(main_target):]
    
    new_main_calls = """
    
    load_sme_competition(conn, "UI-ADOSAA-006R.중소기업 경쟁제품 및 공사용자재 내역.xlsx", "ref_sme_competition_product")
    load_innovation(conn, ".혁신장터 상품등록 내역.xlsx", "innovation_market_excel")
    
    mas_excel_path = r"C:\\Users\\doors\\OneDrive\\바탕 화면\\종합쇼핑몰 부산광역시 기업 품목 등록 내역.xlsx"
    load_mas(conn, mas_excel_path, "mas_excel_bootstrap")
"""
    # Replace the log_missing_source calls
    main_after = main_after.replace("log_missing_source(conn, 'bootstrap_mas', 'bootstrap_mas_excel')", "")
    
    new_content = before + new_functions + main_before + new_main_calls + main_after
    
    with codecs.open('bootstrap_from_excel.py', 'w', 'utf-8') as f:
        f.write(new_content)
    print("Updated bootstrap_from_excel.py successfully.")
