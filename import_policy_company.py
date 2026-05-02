import os
import sqlite3
import datetime
import hashlib
import json
import logging
import requests
import xml.etree.ElementTree as ET
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("PolicyImport")

DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chatbot_company.db')
SMPP_SERVICE_KEY = 'c551b235466f84865b201c21869bc5b08cdf0633cdb4a3105dfb1e19c6427865'
# API endpoint base (placeholder, assuming standard pattern)
SMPP_BASE_URL = 'https://apis.data.go.kr/B550598/smppCertInfo'

def hash_business_no(b_no: str) -> str:
    if not b_no:
        return ""
    return hashlib.sha256(b_no.encode('utf-8')).hexdigest()

def map_cert_name_to_subtype(cert_name: str) -> str:
    if not cert_name:
        return "unknown"
    if "여성기업" in cert_name:
        return "women_company"
    elif "장애인기업" in cert_name:
        return "disabled_company"
    elif "직접생산" in cert_name:
        return "direct_production"
    return "unknown"

def fetch_smpp_certs(b_no: str, use_mock=False):
    """
    Fetch certificates from SMPP API for a given business number.
    Returns a list of dicts: {"type": ..., "cert_no": ..., "v_from": ..., "v_to": ...}
    """
    if use_mock:
        # Return mock data for testing
        if b_no == "1234567890":
            return [
                {"type": "women_company", "cert_no": "W-2023-001", "v_from": "2023-01-01", "v_to": "2026-12-31"},
                {"type": "social_enterprise", "cert_no": "S-2022-005", "v_from": "2022-01-01", "v_to": "2024-12-31"}
            ]
        elif b_no == "1112223333":
            return [
                {"type": "disabled_company", "cert_no": "D-2025-001", "v_from": "2025-01-01", "v_to": "2028-12-31"}
            ]
        return []

    results = []
    # Probe commonly used operation names since exact name is unknown
    operations = ['getSmppCertInfoList', 'getSmppCertList', 'getCertInfoList']
    for op in operations:
        url = f"{SMPP_BASE_URL}/{op}"
        params = {
            'serviceKey': SMPP_SERVICE_KEY,
            'bzno': b_no,
            'pageNo': 1,
            'numOfRows': 100
        }
        try:
            resp = requests.get(url, params=params, timeout=5.0)
            if resp.status_code == 200:
                # Try to parse XML
                try:
                    root = ET.fromstring(resp.content)
                    items = root.findall('.//item')
                    if items:
                        for item in items:
                            cert_nm = item.findtext('certNm') or ''
                            v_bgn = item.findtext('vldBgnDt') or item.findtext('vldPrdBgnDt') or ''
                            v_end = item.findtext('vldEndDt') or item.findtext('vldPrdEndDt') or ''
                            cert_no = item.findtext('certNo') or item.findtext('certNum') or ''
                            
                            # Normalize dates from YYYYMMDD to YYYY-MM-DD if needed
                            if len(v_bgn) == 8:
                                v_bgn = f"{v_bgn[:4]}-{v_bgn[4:6]}-{v_bgn[6:]}"
                            if len(v_end) == 8:
                                v_end = f"{v_end[:4]}-{v_end[4:6]}-{v_end[6:]}"
                                
                            subtype = map_cert_name_to_subtype(cert_nm)
                            if subtype != "unknown":
                                results.append({
                                    "type": subtype,
                                    "cert_no": cert_no,
                                    "v_from": v_bgn,
                                    "v_to": v_end
                                })
                        return results
                except ET.ParseError:
                    pass
        except requests.RequestException:
            continue
    return results

def run_import(dry_run=False, use_mock=False):
    """
    ETL for importing policy companies from SMPP API.
    """
    logger.info(f"Starting Policy Company Import. Dry_run={dry_run}, Use_mock={use_mock}")
    
    conn = sqlite3.connect(DB_FILE, timeout=5.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    
    start_time = datetime.datetime.now()
    inserted_count = 0
    unmatched_count = 0
    total_count = 0
    
    cursor = conn.cursor()
    
    # 1. Load canonical numbers
    rows = cursor.execute("SELECT m.company_internal_id, i.canonical_business_no, m.company_name FROM company_master m JOIN company_identity i ON m.company_internal_id = i.company_internal_id WHERE m.is_busan_company = 1").fetchall()
    
    if use_mock:
        # override for testing
        rows = [
            {'company_internal_id': 1, 'canonical_business_no': '1234567890', 'company_name': '테스트업체'}
        ]
        
    bno_map = {row['canonical_business_no']: row['company_internal_id'] for row in rows if row['canonical_business_no']}
    name_map = {row['canonical_business_no']: row['company_name'] for row in rows if row['canonical_business_no']}
    
    now_date = datetime.date.today().isoformat()
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Process each business number
    for b_no, internal_id in bno_map.items():
        if dry_run and not use_mock and total_count > 10:
            break # limit API calls in dry run
            
        certs = fetch_smpp_certs(b_no, use_mock=use_mock)
        total_count += 1
        if not use_mock:
            time.sleep(0.1) # rate limit prevention
            
        for item in certs:
            b_no_hash = hash_business_no(b_no)
            cert_hash = hash_business_no(item['cert_no'])
            comp_name = name_map.get(b_no, 'Unknown')
            
            if dry_run:
                continue
            
            # Insert raw log
            cursor.execute('''
                INSERT INTO raw_policy_company_import (
                    policy_source_type, source_file_name, raw_company_name, raw_business_no_hash,
                    raw_certification_no_hash, raw_valid_from, raw_valid_to
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (item['type'], 'smpp_api', comp_name, b_no_hash, cert_hash, item['v_from'], item['v_to']))
            raw_id = cursor.lastrowid
            
            # Validity calc
            validity = "valid"
            if item['v_to'] and item['v_to'] < now_date:
                validity = "expired"
            
            cursor.execute('''
                INSERT INTO policy_company_certification (
                    company_internal_id, policy_type, policy_subtype, certification_no_hash,
                    certification_valid_from, certification_valid_to, validity_status,
                    source_name, source_refreshed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(company_internal_id, policy_subtype, source_name, certification_no_hash) DO UPDATE SET
                    certification_valid_from=excluded.certification_valid_from,
                    certification_valid_to=excluded.certification_valid_to,
                    validity_status=excluded.validity_status,
                    source_refreshed_at=excluded.source_refreshed_at
            ''', (
                internal_id, "policy_company", item['type'], cert_hash,
                item['v_from'], item['v_to'], validity, 'smpp_api', now_str
            ))
            inserted_count += 1
            
    if not dry_run:
        cursor.execute('''
            INSERT INTO etl_job_log (
                job_name, source_name, started_at, finished_at, status, 
                input_row_count, inserted_count, skipped_count, error_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            "import_policy_company", "smpp_api",
            start_time.strftime("%Y-%m-%d %H:%M:%S"),
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "success", total_count, inserted_count, unmatched_count, 0
        ))
        
        # Ensure source_name is in source_manifest, update status
        # First check if exists
        row = cursor.execute("SELECT source_id FROM source_manifest WHERE source_name = ?", ("smpp_api",)).fetchone()
        if row:
            cursor.execute('''
                UPDATE source_manifest SET 
                    source_refreshed_at = ?,
                    row_count = ?,
                    status = ?
                WHERE source_name = ?
            ''', (now_str, inserted_count, "success", "smpp_api"))
        else:
            cursor.execute('''
                INSERT INTO source_manifest (source_name, source_refreshed_at, row_count, status)
                VALUES (?, ?, ?, ?)
            ''', ("smpp_api", now_str, inserted_count, "success"))
        
        conn.commit()
    
    conn.close()
    logger.info(f"Import Finished. Processed BNOs: {total_count}, Inserted Certs: {inserted_count}, Unmatched: {unmatched_count}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--use-mock", action="store_true")
    args = parser.parse_args()
    
    run_import(dry_run=args.dry_run, use_mock=args.use_mock)
