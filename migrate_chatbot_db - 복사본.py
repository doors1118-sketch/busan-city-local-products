import sqlite3
import os
import sys

DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chatbot_company.db')

def migrate():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # 1. raw_g2b_company_import (with raw_payload_ref)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS raw_g2b_company_import (
        raw_import_id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_file_name TEXT,
        source_row_no INTEGER,
        source_collected_at DATETIME,
        raw_company_name TEXT,
        raw_business_no TEXT,
        raw_representative_name TEXT,
        raw_address TEXT,
        raw_phone TEXT,
        raw_payload_db_file TEXT,
        raw_payload_table TEXT,
        raw_payload_key TEXT,
        raw_payload_retention_until DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 2. company_master
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS company_master (
        company_internal_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name TEXT,
        company_name_normalized TEXT,
        location_sido TEXT,
        location_sigungu TEXT,
        location_detail TEXT,
        is_busan_company BOOLEAN,
        is_headquarters BOOLEAN,
        busan_classification_reason TEXT,
        display_location TEXT,
        data_status TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        source_refreshed_at DATETIME,
        source_priority INTEGER DEFAULT 0
    )
    ''')

    # 3. company_identity
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS company_identity (
        company_internal_id INTEGER,
        canonical_business_no TEXT NOT NULL UNIQUE,
        company_id TEXT NOT NULL,
        company_id_version INTEGER DEFAULT 1,
        internal_join_key TEXT,
        identity_source TEXT,
        identity_status TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(company_internal_id) REFERENCES company_master(company_internal_id)
    )
    ''')
    
    # Explicit UNIQUE INDEX for company_id
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_company_identity_company_id ON company_identity(company_id)')

    # 4. company_conflict_log
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS company_conflict_log (
        log_id INTEGER PRIMARY KEY AUTOINCREMENT,
        canonical_business_no TEXT,
        conflict_reason TEXT,
        source_1 TEXT,
        source_2 TEXT,
        resolved_action TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 5. source_manifest
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS source_manifest (
        source_id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_name TEXT,
        source_type TEXT,
        source_url_or_file TEXT,
        source_refreshed_at DATETIME,
        row_count INTEGER,
        checksum TEXT,
        status TEXT,
        error_message TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 6. etl_job_log
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS etl_job_log (
        job_id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_name TEXT,
        source_name TEXT,
        started_at DATETIME,
        finished_at DATETIME,
        status TEXT,
        input_row_count INTEGER,
        inserted_count INTEGER,
        updated_count INTEGER,
        skipped_count INTEGER,
        error_count INTEGER,
        error_message TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # ==========================================
    # Phase 2 추가 테이블
    # ==========================================

    # 7. company_license
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS company_license (
        license_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER,
        license_name TEXT,
        license_name_normalized TEXT,
        license_code TEXT,
        is_representative_license BOOLEAN DEFAULT 0,
        license_source TEXT,
        validity_status TEXT,
        valid_from DATETIME,
        valid_to DATETIME,
        source_refreshed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(company_internal_id) REFERENCES company_master(company_internal_id)
    )
    ''')

    # 8. company_product
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS company_product (
        product_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER,
        product_name TEXT,
        product_name_normalized TEXT,
        product_code TEXT,
        g2b_category_code TEXT,
        is_representative_product BOOLEAN DEFAULT 0,
        product_source TEXT,
        source_refreshed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(company_internal_id) REFERENCES company_master(company_internal_id)
    )
    ''')

    # 9. g2b_product_category
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS g2b_product_category (
        category_code TEXT PRIMARY KEY,
        category_depth INTEGER,
        category_name TEXT,
        category_name_normalized TEXT,
        parent_category_code TEXT,
        source TEXT,
        source_refreshed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 10. company_manufacturer_status
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS company_manufacturer_status (
        manufacturer_status_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER,
        manufacturer_type TEXT,
        manufacturer_label TEXT,
        product_name TEXT,
        product_code TEXT,
        evidence_source TEXT,
        validity_status TEXT,
        source_refreshed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(company_internal_id) REFERENCES company_master(company_internal_id)
    )
    ''')

    # 11. search_dictionary
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS search_dictionary (
        dict_id INTEGER PRIMARY KEY AUTOINCREMENT,
        term TEXT,
        term_normalized TEXT,
        target_type TEXT,
        target_value TEXT,
        synonym_group TEXT,
        priority INTEGER DEFAULT 0,
        is_active BOOLEAN DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 12. company_business_status (Phase 3-B 고도화)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS company_business_status (
        company_internal_id INTEGER PRIMARY KEY,
        business_status TEXT NOT NULL DEFAULT 'unknown',
        business_status_freshness TEXT NOT NULL DEFAULT 'not_checked',
        tax_type TEXT,
        closed_at TEXT,
        checked_at DATETIME,
        business_status_source TEXT,
        api_result_code TEXT,
        retry_count INTEGER DEFAULT 0,
        last_error_message TEXT,
        last_attempt_at DATETIME,
        checked_by TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(company_internal_id) REFERENCES company_master(company_internal_id)
    )
    ''')

    # 13. business_status_refresh_queue
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS business_status_refresh_queue (
        queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER NOT NULL,
        priority INTEGER NOT NULL DEFAULT 100,
        reason TEXT NOT NULL,
        requested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        attempt_count INTEGER DEFAULT 0,
        last_attempt_at DATETIME,
        status TEXT DEFAULT 'pending',
        error_message TEXT,
        FOREIGN KEY(company_internal_id) REFERENCES company_master(company_internal_id)
    )
    ''')

    # ==========================================
    # Phase 4 정책기업 연동 추가 테이블
    # ==========================================

    # 14. raw_policy_company_import
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS raw_policy_company_import (
        raw_policy_import_id INTEGER PRIMARY KEY AUTOINCREMENT,
        policy_source_type TEXT NOT NULL,
        source_file_name TEXT,
        raw_company_name TEXT,
        raw_business_no_hash TEXT,
        raw_certification_no_hash TEXT,
        raw_valid_from TEXT,
        raw_valid_to TEXT,
        raw_payload_retention_until DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 15. policy_company_certification
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS policy_company_certification (
        policy_cert_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER NOT NULL,
        policy_type TEXT NOT NULL,
        policy_subtype TEXT NOT NULL,
        certification_no_hash TEXT,
        certification_valid_from DATE,
        certification_valid_to DATE,
        validity_status TEXT NOT NULL DEFAULT 'unknown',
        issuer TEXT,
        source_name TEXT,
        source_refreshed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_pcc_internal_id ON policy_company_certification(company_internal_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_pcc_subtype ON policy_company_certification(policy_subtype)')
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_policy_company_cert_unique ON policy_company_certification(company_internal_id, policy_subtype, source_name, certification_no_hash)')

    # 16. policy_company_unmatched
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS policy_company_unmatched (
        unmatched_id INTEGER PRIMARY KEY AUTOINCREMENT,
        raw_policy_import_id INTEGER NOT NULL,
        policy_source_type TEXT NOT NULL,
        raw_company_name TEXT,
        raw_business_no_hash TEXT,
        reason TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 17. policy_company_conflict_log
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS policy_company_conflict_log (
        conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER NOT NULL,
        policy_subtype TEXT NOT NULL,
        existing_cert_hash TEXT,
        new_cert_hash TEXT,
        conflict_reason TEXT,
        resolved_status TEXT DEFAULT 'pending',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # ==========================================
    # Phase 2 & 4 뷰 생성
    # ==========================================
    # 챗봇 검색 API 응답용 경량 뷰 (보안 가이드레일 적용)
    # 식별정보(사업자번호, join_key 등) 원천 차단
    cursor.execute("DROP VIEW IF EXISTS chatbot_company_candidate_view")
    cursor.execute('''
    CREATE VIEW chatbot_company_candidate_view AS
    SELECT 
        i.company_id,
        m.company_name,
        m.location_sido AS location,
        m.location_detail AS detail_address,
        m.is_busan_company,
        1 AS is_headquarters,
        
        -- 집계/매핑은 API에서 GROUP_CONCAT 하거나 개별 조회. 뷰에서는 단순 스칼라만 우선 제공.
        -- 하지만 요구사항에 따라 뷰 하나에서 최대한 제공하도록 구성.
        (SELECT GROUP_CONCAT(license_name, '|') FROM company_license cl WHERE cl.company_internal_id = m.company_internal_id) AS license_or_business_type,
        (SELECT GROUP_CONCAT(product_name, '|') FROM company_product cp WHERE cp.company_internal_id = m.company_internal_id) AS main_products,
        
        '["local_procurement_company"]' AS candidate_types,
        'local_procurement_company' AS primary_candidate_type,
        (SELECT GROUP_CONCAT(policy_subtype || ':' || validity_status, '|') FROM policy_company_certification pcc WHERE pcc.company_internal_id = m.company_internal_id) AS policy_subtypes_raw,
        
        -- Phase 5: 인증제품 연동
        (SELECT GROUP_CONCAT(cp.certification_type || ':' || 
                 IFNULL(map.is_priority_purchase_product, 0) || ':' || 
                 IFNULL(map.is_innovation_product, 0) || ':' || 
                 IFNULL(map.is_excellent_procurement_product, 0) || ':' || 
                 cp.validity_status, '|') 
         FROM certified_product cp 
         LEFT JOIN certified_product_type_map map ON cp.certification_type = map.normalized_certification_type 
         WHERE cp.company_internal_id = m.company_internal_id) AS certified_product_types_raw,
        
        (SELECT GROUP_CONCAT(
            cp.certification_type || '^^' || 
            cp.product_name || '^^' || 
            cp.validity_status || '^^' || 
            IFNULL(cp.expiration_date, '') || '^^' || 
            cp.source_name, '|||') 
         FROM certified_product cp WHERE cp.company_internal_id = m.company_internal_id) AS certified_product_summary_raw,
        
        IFNULL((SELECT manufacturer_type FROM company_manufacturer_status cms WHERE cms.company_internal_id = m.company_internal_id LIMIT 1), 'unknown') AS manufacturer_type,
        
        'unknown' AS business_status,
        'not_checked' AS business_status_freshness,
        '후보' AS display_status,
        0 AS contract_possible_auto_promoted,
        
        '["company_master"]' AS source_refs,
        m.source_refreshed_at
    FROM company_master m
    JOIN company_identity i ON m.company_internal_id = i.company_internal_id
    WHERE m.is_busan_company = 1
    ''')

    # ==========================================
    # Phase 2 인덱스 생성
    # ==========================================
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_company_license_name_norm ON company_license(license_name_normalized)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_company_license_company ON company_license(company_internal_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_company_product_name_norm ON company_product(product_name_normalized)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_company_product_company ON company_product(company_internal_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_company_product_category ON company_product(g2b_category_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_company_manufacturer_company ON company_manufacturer_status(company_internal_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_company_master_busan ON company_master(is_busan_company)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_pcc_internal_id ON policy_company_certification(company_internal_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_pcc_subtype ON policy_company_certification(policy_subtype)')

    conn.commit()
    conn.close()
    print("Chatbot DB migration completed.")

if __name__ == "__main__":
    migrate()
