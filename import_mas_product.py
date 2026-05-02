import os
import sqlite3
import datetime
import hashlib
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MasProductImport")

DB_FILE = os.environ.get("CHATBOT_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chatbot_company.db'))

def fetch_mock_data():
    """테스트/Mock 용 데이터 반환"""
    return [
        {
            "product_name": "데스크톱컴퓨터",
            "product_code": "43211507",
            "detail_product_name": "세부데스크톱",
            "detail_product_code": "4321150701",
            "g2b_category_code": "432115",
            "b_no": "1234567890",
            "comp_name": "테스트업체",
            "contract_no": "MAS-2023-01",
            "v_from": "2023-01-01",
            "v_to": "2027-12-31",
            "contract_status": "active",
            "price_amount": 1500000,
            "price_unit": "EA"
        },
        {
            "product_name": "영상감시장치",
            "product_code": "46171622",
            "detail_product_name": "세부CCTV",
            "detail_product_code": "4617162201",
            "g2b_category_code": "461716",
            "b_no": "1234567890",
            "comp_name": "테스트업체",
            "contract_no": "MAS-2022-02",
            "v_from": "2022-01-01",
            "v_to": "2023-12-31",
            "contract_status": "expired",
            "price_amount": 2500000,
            "price_unit": "SYS"
        },
        {
            "product_name": "매칭실패제품",
            "product_code": "11111111",
            "b_no": "0000000000", # 매칭 안 되는 사업자번호
            "comp_name": "타지역업체",
            "contract_no": "MAS-UNK",
            "v_from": "2023-01-01",
            "v_to": "2025-12-31",
            "contract_status": "active",
            "price_amount": 1000,
            "price_unit": "EA"
        }
    ]

def hash_string(val: str) -> str:
    if not val:
        return ""
    return hashlib.sha256(val.encode('utf-8')).hexdigest()

def normalize_product_name(val: str) -> str:
    if not val:
        return ""
    return val.replace(" ", "").lower()

def run_import(dry_run=False, use_mock=False, use_file=False):
    logger.info(f"Starting MAS Product Import. Dry_run={dry_run}, Use_mock={use_mock}, Use_file={use_file}")
    
    conn = sqlite3.connect(DB_FILE, timeout=5.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    
    start_time = datetime.datetime.now()
    now_date = datetime.date.today().isoformat()
    now_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    
    cursor = conn.cursor()
    
    # Canonical BNO 로드 (Exact match only)
    bno_rows = cursor.execute("SELECT m.company_internal_id, i.canonical_business_no FROM company_master m JOIN company_identity i ON m.company_internal_id = i.company_internal_id").fetchall()
    bno_map = {row['canonical_business_no']: row['company_internal_id'] for row in bno_rows if row['canonical_business_no']}
    
    data = []
    if use_file:
        source_name = "mas_file_sample"
        try:
            import csv
            with open('mas_sample.csv', 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if i >= 100: break
                    data.append(row)
        except Exception as e:
            logger.error(f"Failed to read mas_sample.csv: {e}")
    elif use_mock:
        source_name = "g2b_mas_mock"
        data = fetch_mock_data()
    else:
        source_name = "g2b_mas_api"
        data = []
    
    inserted_count = 0
    skipped_count = 0
    total_count = len(data)
    
    for idx, item in enumerate(data):
        b_no = item.get('b_no', '')
        b_no_hash = hash_string(b_no)
        comp_name = item.get('comp_name', '')
        contract_no = item.get('contract_no', '')
        contract_no_hash = hash_string(contract_no)
        
        prod_name = item.get('product_name', '')
        prod_name_norm = normalize_product_name(prod_name)
        prod_code = item.get('product_code', '')
        dtl_prod_name = item.get('detail_product_name', '')
        dtl_prod_code = item.get('detail_product_code', '')
        cat_code = item.get('g2b_category_code', '')
        
        v_from = item.get('v_from', '')
        v_to = item.get('v_to', '')
        status = item.get('contract_status', 'unknown')
        
        price = item.get('price_amount', 0)
        unit = item.get('price_unit', '')
        
        if dry_run:
            continue
            
        # 1. Raw Table (Hash Only)
        cursor.execute('''
            INSERT INTO raw_mas_product_import (
                source_name, source_row_no, source_collected_at,
                raw_product_name, raw_product_code, raw_detail_product_name, raw_detail_product_code,
                raw_company_name, raw_business_no_hash, raw_contract_no_hash,
                raw_contract_start_date, raw_contract_end_date, raw_price, raw_unit, raw_contract_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            source_name, idx+1, now_str,
            prod_name, prod_code, dtl_prod_name, dtl_prod_code,
            comp_name, b_no_hash, contract_no_hash,
            v_from, v_to, str(price), unit, status
        ))
        raw_id = cursor.lastrowid
        
        # 2. Matching
        internal_id = bno_map.get(b_no)
        if not internal_id:
            # Unmatched
            cursor.execute('''
                INSERT INTO mas_product_unmatched (
                    raw_mas_import_id, source_name, raw_company_name,
                    raw_business_no_hash, raw_product_name, reason
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (raw_id, source_name, comp_name, b_no_hash, prod_name, 'business_no_not_found'))
            skipped_count += 1
            continue
            
        # Validate Expiration
        if v_to and v_to < now_date and status == 'active':
            status = 'expired'
            
        # 3. Upsert mas_product
        cursor.execute('''
            INSERT INTO mas_product (
                company_internal_id, product_name, product_name_normalized,
                product_code, detail_product_name, detail_product_code, g2b_category_code,
                contract_no_hash, contract_start_date, contract_end_date, contract_status,
                price_amount, price_unit, source_name, source_refreshed_at, match_method
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(company_internal_id, contract_no_hash, product_name_normalized, detail_product_code, source_name) DO UPDATE SET
                contract_start_date=excluded.contract_start_date,
                contract_end_date=excluded.contract_end_date,
                contract_status=excluded.contract_status,
                price_amount=excluded.price_amount,
                price_unit=excluded.price_unit,
                source_refreshed_at=excluded.source_refreshed_at,
                updated_at=CURRENT_TIMESTAMP
        ''', (
            internal_id, prod_name, prod_name_norm,
            prod_code, dtl_prod_name, dtl_prod_code, cat_code,
            contract_no_hash, v_from, v_to, status,
            price, unit, source_name, now_str, 'exact_bno'
        ))
        # get ID
        row = cursor.execute('''
            SELECT mas_product_id FROM mas_product 
            WHERE company_internal_id=? AND contract_no_hash=? AND product_name_normalized=? 
            AND detail_product_code=? AND source_name=?
        ''', (internal_id, contract_no_hash, prod_name_norm, dtl_prod_code, source_name)).fetchone()
        mas_prod_id = row['mas_product_id']

        # 4. Upsert mas_supplier
        cursor.execute('''
            SELECT 1 FROM mas_supplier WHERE company_internal_id=?
        ''', (internal_id,))
        if not cursor.fetchone():
            cursor.execute('''
                INSERT INTO mas_supplier (
                    company_internal_id, supplier_name, supplier_name_normalized,
                    supplier_business_no_hash, source_name, source_refreshed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (internal_id, comp_name, normalize_product_name(comp_name), b_no_hash, source_name, now_str))
        
        # 5. Upsert mas_contract
        cursor.execute('''
            INSERT INTO mas_contract (
                company_internal_id, contract_no_hash, product_name, product_code,
                detail_product_name, detail_product_code, contract_start_date, contract_end_date,
                contract_status, source_name, source_refreshed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(company_internal_id, contract_no_hash, product_code, detail_product_code, source_name) DO UPDATE SET
                contract_start_date=excluded.contract_start_date,
                contract_end_date=excluded.contract_end_date,
                contract_status=excluded.contract_status,
                source_refreshed_at=excluded.source_refreshed_at,
                updated_at=CURRENT_TIMESTAMP
        ''', (
            internal_id, contract_no_hash, prod_name, prod_code,
            dtl_prod_name, dtl_prod_code, v_from, v_to,
            status, source_name, now_str
        ))
        
        # 6. Upsert mas_price_condition
        cursor.execute('''
            INSERT INTO mas_price_condition (
                mas_product_id, price_amount, price_unit, source_name, source_refreshed_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(mas_product_id, source_name) DO UPDATE SET
                price_amount=excluded.price_amount,
                price_unit=excluded.price_unit,
                source_refreshed_at=excluded.source_refreshed_at,
                updated_at=CURRENT_TIMESTAMP
        ''', (mas_prod_id, price, unit, source_name, now_str))

        inserted_count += 1

    if not dry_run:
        cursor.execute('''
            INSERT INTO etl_job_log (
                job_name, source_name, started_at, finished_at, status, 
                input_row_count, inserted_count, skipped_count, error_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            "import_mas_product", source_name, now_str,
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "success", total_count, inserted_count, skipped_count, 0
        ))
        
        cursor.execute('''
            INSERT INTO source_manifest (source_name, source_type, row_count, source_refreshed_at, status)
            VALUES (?, 'api', ?, ?, 'success')
            ON CONFLICT(source_name) DO UPDATE SET
                row_count=excluded.row_count,
                source_refreshed_at=excluded.source_refreshed_at,
                status='success'
        ''', (source_name, total_count, now_str))
        
        conn.commit()
    
    conn.close()
    logger.info(f"Import Finished. Processed: {total_count}, Inserted/Updated: {inserted_count}, Skipped/Unmatched: {skipped_count}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--use-mock", action="store_true")
    parser.add_argument("--probe", action="store_true", help="Run source sample test probe (mock based)")
    parser.add_argument("--file-probe", action="store_true", help="Run source file test probe (CSV based)")
    args = parser.parse_args()
    
    if args.file_probe:
        run_import(dry_run=args.dry_run, use_mock=False, use_file=True)
    elif args.probe:
        run_import(dry_run=args.dry_run, use_mock=True, use_file=False)
    else:
        run_import(dry_run=args.dry_run, use_mock=args.use_mock, use_file=False)
