import re

with open('bootstrap_from_excel.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the entire load_mas function
new_load_mas = '''def load_mas(conn, file_path, source_name):
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
'''

pattern = re.compile(r"def load_mas\(conn, file_path, source_name\):.*?def log_missing_source", re.DOTALL)
content = pattern.sub(new_load_mas + "\ndef log_missing_source", content)

with open('bootstrap_from_excel.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated load_mas with Phase 6-D-2 domain-based routing.")
