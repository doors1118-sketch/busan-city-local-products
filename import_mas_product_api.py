import os
import sys
import sqlite3
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import hashlib

TARGET_DB = os.environ.get("CHATBOT_DB", "staging_chatbot_company.db")
SERVICE_KEY = os.environ.get("SHOPPING_MALL_PRDCT_SERVICE_KEY")

def log_etl(conn, job_name, source_name, input_count, inserted_count, skipped_count=0, status='success', msg=""):
    conn.execute("""
        INSERT INTO etl_job_log (job_name, source_name, started_at, finished_at, status, input_row_count, inserted_count, skipped_count, error_count, error_message)
        VALUES (?, ?, datetime('now'), datetime('now'), ?, ?, ?, ?, 0, ?)
    """, (job_name, source_name, status, input_count, inserted_count, skipped_count, msg))
    
    conn.execute("""
        INSERT INTO source_manifest (source_name, source_type, source_refreshed_at, row_count, status, error_message)
        VALUES (?, 'api_incremental', datetime('now'), ?, ?, ?)
        ON CONFLICT(source_name) DO UPDATE SET row_count=excluded.row_count, source_refreshed_at=excluded.source_refreshed_at, status=excluded.status, error_message=excluded.error_message
    """, (source_name, inserted_count, status, msg))

def get_internal_id_by_bizno(conn, bizno):
    cur = conn.cursor()
    bno_clean = str(bizno).replace('-', '').replace('.0', '').strip()
    
    cur.execute("SELECT company_internal_id FROM company_identity WHERE canonical_business_no = ?", (bno_clean,))
    res = cur.fetchone()
    return res[0] if res else None

def load_label_map(conn):
    """procurement_label_map 테이블에서 라벨 → 도메인/타입 매핑 로드"""
    label_map = {}
    try:
        for row in conn.execute("SELECT raw_label, target_domain, target_type, is_candidate_type_promotable FROM procurement_label_map WHERE is_active=1"):
            label_map[row[0]] = {'domain': row[1], 'type': row[2], 'promotable': row[3]}
    except Exception:
        pass  # 테이블이 없으면 빈 맵 반환
    return label_map

def parse_and_insert_labels(conn, cert_list_str, internal_id, p_name, p_name_norm, p_code, dp_code, label_map, source_name, now_str, counters):
    """prodctCertList(물품인증유형목록) 파싱 → 조달속성/일반인증/인증제품 분류 적재"""
    if not cert_list_str or cert_list_str == 'nan':
        return
    
    for cert in cert_list_str.split(','):
        c = cert.strip()
        if not c:
            continue
        
        mapping = label_map.get(c)
        
        if mapping:
            domain = mapping['domain']
            target_type = mapping['type']
            
            if domain == 'product_certification':
                surr = hashlib.sha256(
                    f"{source_name}|{c}|{internal_id}|{p_name_norm}|{dp_code}".encode('utf-8')
                ).hexdigest()[:16]
                
                conn.execute("""
                    INSERT OR IGNORE INTO certified_product (
                        company_internal_id, certification_type, certification_type_label,
                        certification_no_hash, product_name, product_name_normalized, 
                        validity_status, source_name, source_refreshed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'unknown', ?, ?)
                """, (internal_id, target_type, c, surr, p_name, p_name_norm, source_name, now_str))
                counters['cert'] += 1
                
            elif domain == 'company_procurement_attribute':
                conn.execute("""
                    INSERT OR IGNORE INTO company_procurement_attribute (
                        company_internal_id, attribute_type, attribute_label,
                        product_name, product_code, detail_product_code,
                        source_name, source_refreshed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (internal_id, target_type, c, p_name, p_code, dp_code, source_name, now_str))
                counters['attr'] += 1
                
            elif domain == 'general_certification':
                conn.execute("""
                    INSERT OR IGNORE INTO product_general_certification (
                        company_internal_id, raw_cert_label, normalized_cert_type,
                        product_name, product_code, detail_product_code,
                        source_name, source_refreshed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (internal_id, c, target_type, p_name, p_code, dp_code, source_name, now_str))
                counters['general'] += 1
                
            elif domain == 'ignore':
                pass
                
        else:
            # 매핑 없음 -> review 큐
            try:
                conn.execute("""
                    INSERT INTO procurement_label_mapping_review (
                        raw_label, product_name, product_code, detail_product_code,
                        company_internal_id, source_name, reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (c, p_name, p_code, dp_code, internal_id, source_name, 'unmapped_api'))
                counters['review'] += 1
            except Exception:
                pass

def fetch_mas_data(target_date_str=None, max_pages=100, num_of_rows=100, days=7, probe=False, dry_run=False, staging_write=False):
    if not SERVICE_KEY:
        print("ERROR: SHOPPING_MALL_PRDCT_SERVICE_KEY is not set.")
        try:
            conn = sqlite3.connect(TARGET_DB)
            log_etl(conn, 'mas_api_incremental', 'mas_api_incremental', 0, 0, status='failed', msg='serviceKey not configured')
            log_etl(conn, 'shopping_mall_api_incremental', 'shopping_mall_api_incremental', 0, 0, status='failed', msg='serviceKey not configured')
            conn.commit()
            conn.close()
        except Exception:
            pass
        return
        
    conn = sqlite3.connect(TARGET_DB)
    
    # 라벨 매핑 로드
    label_map = load_label_map(conn)
    label_count = len(label_map)
    print(f"  procurement_label_map loaded: {label_count} entries")
    
    if target_date_str:
        end_date = datetime.strptime(target_date_str, "%Y%m%d")
    else:
        end_date = datetime.now()
        
    start_date = end_date - timedelta(days=days)
    
    bgn_dt = start_date.strftime("%Y%m%d")
    end_dt = end_date.strftime("%Y%m%d")
    
    mode_label = "[PROBE]" if probe else ("[DRY-RUN]" if dry_run else "[LIVE]")
    print(f"{mode_label} Starting MAS API incremental fetch (chgDt) for period {bgn_dt} ~ {end_dt}")
    print(f"  max_pages={max_pages}, num_of_rows={num_of_rows}, days={days}")
    
    url = "https://apis.data.go.kr/1230000/at/ShoppingMallPrdctInfoService/getMASCntrctPrdctInfoList"
    
    page = 1
    total_inserted = 0
    total_api_items = 0
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_date = datetime.now().strftime("%Y%m%d")
    
    status = 'success'
    error_msg = ""
    source_name = 'mas_api_incremental'
    sm_source_name = 'shopping_mall_api_incremental'
    
    # 라벨 파싱 카운터
    label_counters = {'cert': 0, 'attr': 0, 'general': 0, 'review': 0}
    
    while page <= max_pages:
        params = {
            'serviceKey': SERVICE_KEY,
            'numOfRows': str(num_of_rows),
            'pageNo': str(page),
            'chgDtBgnDt': bgn_dt,
            'chgDtEndDt': end_dt
        }
        
        try:
            print(f"  Fetching page {page}...")
            resp = requests.get(url, params=params, timeout=60)
            if resp.status_code != 200:
                print(f"  API Error at page {page}: HTTP {resp.status_code}")
                status = 'partial_success' if total_inserted > 0 else 'failed'
                error_msg = f"HTTP {resp.status_code} at page {page}"
                break
                
            root = ET.fromstring(resp.content)
            res_code = root.findtext('.//resultCode')
            if res_code != '00':
                res_msg = root.findtext('.//resultMsg')
                print(f"  API Business Error: code={res_code}")
                status = 'partial_success' if total_inserted > 0 else 'failed'
                error_msg = f"API Code {res_code}: {res_msg}"
                break
            
            # Probe mode: 첫 페이지 총 건수만 확인 후 종료
            if probe:
                total_count_node = root.findtext('.//totalCount')
                total_count = int(total_count_node) if total_count_node else 0
                items = root.findall('.//item')
                has_cert_list = False
                for item in items:
                    cert_list = item.findtext('prodctCertList', '')
                    if cert_list:
                        has_cert_list = True
                        break
                print(f"  [PROBE] totalCount={total_count}, page1_items={len(items)}, prodctCertList={'있음' if has_cert_list else '없음'}, label_map={label_count}건")
                log_etl(conn, 'mas_api_probe', source_name, total_count, 0, status='success', msg=f'probe: totalCount={total_count}, label_map={label_count}')
                conn.commit()
                conn.close()
                return
                
            items = root.findall('.//item')
            if not items:
                break
                
            for item in items:
                total_api_items += 1
                bizno = item.findtext('bizrno') or item.findtext('cntrctCorpNo')
                if not bizno: continue
                
                internal_id = get_internal_id_by_bizno(conn, bizno)
                if not internal_id: continue 
                
                contract_no = item.findtext('cntrctNo') or item.findtext('shopngCntrctNo', '')
                cno_hash = hashlib.sha256(contract_no.encode('utf-8')).hexdigest()[:16]
                
                p_name = item.findtext('prdctClsfcNoNm', '')
                p_code = item.findtext('prdctClsfcNo', '')
                dp_name = item.findtext('dtlPrdctClsfcNoNm', '') or item.findtext('dtilPrdctClsfcNo', '')
                dp_code = item.findtext('dtlPrdctClsfcNo', '') or item.findtext('dtilPrdctClsfcNo', '')
                g2b_cat = item.findtext('shoppingMallCtgry', '') or item.findtext('prdctLrgclsfcCd', '')
                price = item.findtext('prdctUprc') or item.findtext('cntrctPrceAmt', '0')
                unit = item.findtext('unitNm') or item.findtext('prdctUnit', '')
                
                c_start = item.findtext('cntrctBgnDt') or item.findtext('cntrctBgnDate', '')
                c_end = item.findtext('cntrctEndDt') or item.findtext('cntrctEndDate', '')
                
                p_name_norm = p_name.replace(' ', '').lower() if p_name else ''
                
                c_status = 'unknown'
                if c_end:
                    c_end_clean = c_end.replace('-', '')[:8]
                    if current_date <= c_end_clean:
                        c_status = 'active'
                    else:
                        c_status = 'expired'
                
                try:
                    price_val = float(price)
                except:
                    price_val = 0
                
                if dry_run:
                    total_inserted += 1
                    continue
                    
                # Phase 6-G: shopping_mall_product (MAS API is always 'mas')
                conn.execute("""
                    INSERT INTO shopping_mall_product (
                        company_internal_id, product_name, product_name_normalized, product_code,
                        detail_product_name, detail_product_code, g2b_category_code, 
                        shopping_mall_registered, shopping_mall_contract_type, contract_no_hash,
                        contract_start_date, contract_end_date, contract_status, order_path_available,
                        price_amount, price_unit, source_name, source_refreshed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 'mas', ?, ?, ?, ?, 1, ?, ?, ?, ?)
                    ON CONFLICT(company_internal_id, contract_no_hash, product_name_normalized, detail_product_code, source_name) 
                    DO UPDATE SET 
                        contract_status=excluded.contract_status,
                        price_amount=excluded.price_amount,
                        source_refreshed_at=excluded.source_refreshed_at
                """, (internal_id, p_name, p_name_norm, p_code, dp_name, dp_code, g2b_cat, cno_hash, c_start, c_end, c_status, price_val, unit, sm_source_name, now_str))

                # ON CONFLICT for mas_product
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
                """, (internal_id, p_name, p_name_norm, p_code, dp_name, dp_code, g2b_cat, cno_hash, c_status, price_val, unit, source_name, now_str))
                
                mp_id = conn.execute("""
                    SELECT mas_product_id FROM mas_product 
                    WHERE company_internal_id=? AND contract_no_hash=? AND product_name_normalized=? AND detail_product_code=? AND source_name=?
                """, (internal_id, cno_hash, p_name_norm, dp_code, source_name)).fetchone()[0]
                
                # ON CONFLICT for mas_contract
                conn.execute("""
                    INSERT INTO mas_contract (
                        company_internal_id, contract_no_hash, product_name, product_code,
                        detail_product_name, detail_product_code, contract_start_date, contract_end_date,
                        contract_status, source_name, source_refreshed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(company_internal_id, contract_no_hash, product_code, detail_product_code, source_name)
                    DO UPDATE SET contract_status=excluded.contract_status, source_refreshed_at=excluded.source_refreshed_at
                """, (internal_id, cno_hash, p_name, p_code, dp_name, dp_code, c_start, c_end, c_status, source_name, now_str))
                
                # ON CONFLICT for mas_price_condition
                conn.execute("""
                    INSERT INTO mas_price_condition (
                        mas_product_id, price_amount, price_unit, source_name, source_refreshed_at
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(mas_product_id, source_name)
                    DO UPDATE SET price_amount=excluded.price_amount, source_refreshed_at=excluded.source_refreshed_at
                """, (mp_id, price_val, unit, source_name, now_str))
                
                # Phase 6-H: 물품인증유형목록(prodctCertList) 라벨 파싱
                cert_list_str = item.findtext('prodctCertList', '')
                if cert_list_str and label_map:
                    parse_and_insert_labels(
                        conn, cert_list_str, internal_id,
                        p_name, p_name_norm, p_code, dp_code,
                        label_map, source_name, now_str, label_counters
                    )
                
                total_inserted += 1
                
            total_count_node = root.findtext('.//totalCount')
            if total_count_node:
                total_count = int(total_count_node)
                if page * num_of_rows >= total_count:
                    break
            
            page += 1
            if not dry_run:
                conn.commit()
            
        except requests.exceptions.RequestException as e:
            print(f"  Network error during fetch: type={type(e).__name__}")
            status = 'failed'
            error_msg = f"Network error: {type(e).__name__}"
            break
        except ET.ParseError as e:
            print(f"  XML parsing error at page {page}")
            status = 'failed'
            error_msg = f"XML parse error at page {page}"
            break
        except Exception as e:
            print(f"  Exception during fetch: {type(e).__name__}")
            status = 'failed'
            error_msg = f"{type(e).__name__}: {str(e)[:100]}"
            break
    
    if dry_run:
        print(f"[DRY-RUN] Completed. Items in API: {total_api_items}, Would insert: {total_inserted}")
    else:
        print(f"Completed MAS API sync. Items: {total_api_items}, Updates: {total_inserted}")
        print(f"  Label parsing: cert={label_counters['cert']}, attr={label_counters['attr']}, general={label_counters['general']}, review={label_counters['review']}")
    
    label_msg = f"cert={label_counters['cert']},attr={label_counters['attr']},general={label_counters['general']}"
    full_msg = f"{error_msg} | labels: {label_msg}" if error_msg else f"labels: {label_msg}"
    
    log_etl(conn, 'mas_api_incremental', source_name, total_api_items, total_inserted if not dry_run else 0, status=status, msg=full_msg)
    log_etl(conn, 'shopping_mall_api_incremental', sm_source_name, total_api_items, total_inserted if not dry_run else 0, status=status, msg=full_msg)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="MAS/종합쇼핑몰 API 증분 수집 + 물품인증유형 라벨 파싱")
    parser.add_argument("--target-date", help="YYYYMMDD (기준 종료일)", default=None)
    parser.add_argument("--probe", action="store_true", help="API 총 건수만 확인 후 종료 (데이터 미적재)")
    parser.add_argument("--dry-run", action="store_true", help="API 호출하되 DB 미적재 (파싱만 수행)")
    parser.add_argument("--staging-write", action="store_true", help="staging DB에 적재 (기본: staging_chatbot_company.db)")
    parser.add_argument("--max-pages", type=int, default=100, help="최대 페이지 수 (기본: 100)")
    parser.add_argument("--num-rows", type=int, default=100, help="페이지당 행 수 (기본: 100)")
    parser.add_argument("--days", type=int, default=7, help="수집 기간 일수 (기본: 7)")
    args = parser.parse_args()
    
    if args.staging_write:
        TARGET_DB = os.environ.get("CHATBOT_DB", "staging_chatbot_company.db")
    
    fetch_mas_data(
        target_date_str=args.target_date,
        max_pages=args.max_pages,
        num_of_rows=args.num_rows,
        days=args.days,
        probe=args.probe,
        dry_run=args.dry_run,
        staging_write=args.staging_write
    )
