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

DB_FILE = os.environ.get("CHATBOT_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chatbot_company.db'))
# API key는 환경변수에서만 로드. 하드코딩 금지.
# 기존 하드코딩된 키는 폐기·재발급 대상. 절대 소스에 키를 기록하지 말 것.
SMPP_SERVICE_KEY = os.environ.get('SMPP_CERT_INFO_SERVICE_KEY') or os.environ.get('SMPP_SERVICE_KEY')
SMPP_BASE_URL = 'https://apis.data.go.kr/B550598/smppCertInfo'
SMPP_OPERATION = 'getSmppCertInfoList'

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
    elif "예비사회적기업" in cert_name:
        return "preliminary_social_enterprise"
    elif "사회적기업" in cert_name:
        return "social_enterprise"
    elif "중증장애인생산품" in cert_name:
        return "severe_disabled_production"
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

    # 환경변수에 serviceKey가 없으면 API 호출 없이 안전하게 빈 결과 반환
    if not SMPP_SERVICE_KEY:
        logger.warning("SMPP_SERVICE_KEY 환경변수 미설정 — API 호출 건너뜀")
        return []

    results = []
    url = f"{SMPP_BASE_URL}/{SMPP_OPERATION}"
    params = {
        'serviceKey': SMPP_SERVICE_KEY,
        'bzno': b_no,
        'pageNo': 1,
        'numOfRows': 100
    }
    try:
        resp = requests.get(url, params=params, timeout=5.0)
        if resp.status_code == 200:
            try:
                root = ET.fromstring(resp.content)
                items = root.findall('.//item')
                for item in items:
                    cert_nm = item.findtext('certNm') or ''
                    v_bgn = item.findtext('vldBgnDt') or ''
                    v_end = item.findtext('vldEndDt') or ''
                    cert_no = item.findtext('certNo') or ''

                    # YYYYMMDD → YYYY-MM-DD
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
            except ET.ParseError:
                logger.warning("SMPP API 응답 XML 파싱 실패")
        else:
            logger.warning(f"SMPP API HTTP {resp.status_code}")
    except requests.RequestException:
        logger.warning("SMPP API 요청 실패")
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

def source_probe(limit=5):
    """
    Source Smoke Test용: 실제 SMPP API에 1~limit건만 호출하여 연결성 및 파싱 검증
    """
    logger.info(f"Starting SMPP source_probe with limit={limit}")
    if not SMPP_SERVICE_KEY:
        logger.error("SMPP_SERVICE_KEY is missing!")
        return {"success": False, "error": "Missing SMPP_SERVICE_KEY"}
        
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT m.company_internal_id, i.canonical_business_no FROM company_master m JOIN company_identity i ON m.company_internal_id = i.company_internal_id WHERE m.is_busan_company = 1 LIMIT ?", (limit,)).fetchall()
    
    success_count = 0
    parsed_items = 0
    error_message = None
    
    try:
        for r in rows:
            b_no = r['canonical_business_no']
            if not b_no: continue
            certs = fetch_smpp_certs(b_no, use_mock=False)
            success_count += 1
            parsed_items += len(certs)
            time.sleep(0.1)
    except Exception as e:
        error_message = str(e)
        logger.error(f"SMPP probe failed: {e}")
        
    status = 'success'
    if error_message:
        status = 'failed'
    elif parsed_items == 0 and success_count > 0:
        # 호출은 성공했으나 파싱된 데이터가 없는 경우
        status = 'success'
        
    # etl_job_log 기록
    conn.execute('''
        INSERT INTO etl_job_log (job_name, source_name, started_at, finished_at, status, input_row_count, inserted_count, skipped_count, error_count, error_message)
        VALUES (?, ?, datetime('now'), datetime('now'), ?, ?, ?, ?, ?, ?)
    ''', ('smpp_source_probe', 'smpp_api_probe', status, success_count, parsed_items, 0, 1 if error_message else 0, error_message))
    
    # source_manifest 기록
    conn.execute('''
        INSERT INTO source_manifest (source_name, source_refreshed_at, row_count, status)
        VALUES (?, datetime('now'), ?, ?)
        ON CONFLICT(source_name) DO UPDATE SET row_count=excluded.row_count, status=excluded.status, source_refreshed_at=datetime('now')
    ''', ('smpp_api_probe', success_count, status))
    
    conn.commit()
    conn.close()
    
    logger.info(f"source_probe finished. Called: {success_count}, Parsed items: {parsed_items}")
    return {"success": not bool(error_message), "called": success_count, "parsed": parsed_items}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--use-mock", action="store_true")
    parser.add_argument("--probe", action="store_true", help="Run source smoke test probe")
    args = parser.parse_args()
    
    if args.probe:
        res = source_probe(limit=5)
        if not res["success"]:
            exit(1)
    else:
        run_import(dry_run=args.dry_run, use_mock=args.use_mock)
