import sqlite3
import os
import sys

DB_FILE = os.environ.get("CHATBOT_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chatbot_company.db'))

def migrate():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys=ON;")
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
        source_name TEXT UNIQUE NOT NULL,
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
    
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_source_manifest_name ON source_manifest(source_name)')

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
    # Phase 5 인증제품 연동 추가 테이블
    # ==========================================
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS raw_certified_product_import (
        raw_certified_product_import_id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_name TEXT NOT NULL,
        source_row_no INTEGER,
        source_collected_at DATETIME,
        raw_certification_type TEXT,
        raw_certification_no_hash TEXT,
        raw_product_name TEXT,
        raw_company_name TEXT,
        raw_business_no_hash TEXT,
        raw_representative_name_hash TEXT,
        raw_certification_date TEXT,
        raw_expiration_date TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS certified_product (
        certified_product_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER NOT NULL,
        certification_type TEXT NOT NULL,
        certification_type_label TEXT,
        certification_no_hash TEXT,
        product_name TEXT,
        product_name_normalized TEXT,
        certification_date DATE,
        expiration_date DATE,
        validity_status TEXT NOT NULL DEFAULT 'unknown',
        issuer TEXT,
        source_name TEXT,
        source_refreshed_at DATETIME,
        match_method TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_certified_product_unique ON certified_product(company_internal_id, certification_type, source_name, certification_no_hash, product_name_normalized)')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS certified_product_type_map (
        raw_certification_type TEXT PRIMARY KEY,
        normalized_certification_type TEXT NOT NULL,
        certification_group TEXT NOT NULL,
        is_priority_purchase_product BOOLEAN DEFAULT 0,
        is_innovation_product BOOLEAN DEFAULT 0,
        is_excellent_procurement_product BOOLEAN DEFAULT 0,
        is_active BOOLEAN DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Seed map
    cursor.execute('''
        INSERT OR IGNORE INTO certified_product_type_map 
        (raw_certification_type, normalized_certification_type, certification_group, is_priority_purchase_product, is_innovation_product, is_excellent_procurement_product)
        VALUES 
        ('성능인증', 'performance_certification', 'priority_purchase', 1, 0, 0),
        ('우수조달물품지정', 'excellent_procurement_product', 'priority_purchase', 1, 0, 1),
        ('우수조달물품', 'excellent_procurement_product', 'priority_purchase', 1, 0, 1),
        ('NEP', 'nep_product', 'priority_purchase', 1, 0, 0),
        ('신제품인증(NEP)', 'nep_product', 'priority_purchase', 1, 0, 0),
        ('GS인증', 'gs_certified_product', 'priority_purchase', 1, 0, 0),
        ('NET', 'net_certified_product', 'priority_purchase', 1, 0, 0),
        ('신기술인증(NET)', 'net_certified_product', 'priority_purchase', 1, 0, 0),
        ('혁신제품', 'innovation_product', 'innovation', 1, 1, 0),
        ('우수연구개발혁신제품', 'excellent_rnd_innovation_product', 'innovation', 1, 1, 0),
        ('혁신시제품', 'innovation_prototype_product', 'innovation', 1, 1, 0),
        ('기타혁신제품', 'other_innovation_product', 'innovation', 1, 1, 0),
        ('재난안전제품인증', 'disaster_safety_certified_product', 'priority_purchase', 1, 0, 0),
        ('녹색기술제품', 'green_technology_product', 'priority_purchase', 1, 0, 0),
        ('산업융합 신제품 적합성 인증', 'industrial_convergence_new_product', 'priority_purchase', 1, 0, 0),
        ('우수조달공동상표', 'excellent_procurement_joint_brand', 'priority_purchase', 1, 0, 0),
        ('물산업 우수제품 등 지정', 'water_industry_excellent_product', 'priority_purchase', 1, 0, 0),
        ('산업융합품목', 'industrial_convergence_item', 'priority_purchase', 1, 0, 0),
        ('수요처 지정형 기술개발제품', 'demand_designated_tech_product', 'priority_purchase', 1, 0, 0),
        ('구매조건부신기술개발', 'demand_designated_tech_product', 'priority_purchase', 1, 0, 0),
        ('중소기업융복합기술개발', 'demand_designated_tech_product', 'priority_purchase', 1, 0, 0),
        ('우수산업디자인(GD)', 'excellent_industrial_design', 'priority_purchase', 1, 0, 0)
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS certified_product_unmatched (
        unmatched_id INTEGER PRIMARY KEY AUTOINCREMENT,
        raw_certified_product_import_id INTEGER NOT NULL,
        source_name TEXT NOT NULL,
        raw_company_name TEXT,
        raw_business_no_hash TEXT,
        raw_product_name TEXT,
        reason TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS certified_product_conflict_log (
        conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER NOT NULL,
        certification_type TEXT NOT NULL,
        existing_cert_hash TEXT,
        new_cert_hash TEXT,
        conflict_reason TEXT,
        resolved_status TEXT DEFAULT 'pending',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # ==========================================
    # Phase 6-C MAS/종합쇼핑몰 연동 추가 테이블
    # ==========================================
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS raw_mas_product_import (
        raw_mas_import_id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_name TEXT NOT NULL,
        source_file_name TEXT,
        source_row_no INTEGER,
        source_collected_at DATETIME,
        raw_product_name TEXT,
        raw_product_code TEXT,
        raw_detail_product_name TEXT,
        raw_detail_product_code TEXT,
        raw_company_name TEXT,
        raw_business_no_hash TEXT,
        raw_contract_no_hash TEXT,
        raw_contract_start_date TEXT,
        raw_contract_end_date TEXT,
        raw_price TEXT,
        raw_unit TEXT,
        raw_contract_status TEXT,
        raw_payload_db_file TEXT,
        raw_payload_table TEXT,
        raw_payload_key TEXT,
        raw_payload_retention_until DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS mas_product (
        mas_product_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER,
        product_name TEXT NOT NULL,
        product_name_normalized TEXT,
        product_code TEXT,
        detail_product_name TEXT,
        detail_product_code TEXT,
        g2b_category_code TEXT,
        contract_no_hash TEXT,
        contract_start_date DATE,
        contract_end_date DATE,
        contract_status TEXT NOT NULL DEFAULT 'unknown',
        price_amount NUMERIC,
        price_unit TEXT,
        currency TEXT DEFAULT 'KRW',
        source_name TEXT NOT NULL,
        source_refreshed_at DATETIME,
        match_method TEXT,
        match_status TEXT NOT NULL DEFAULT 'matched',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(company_internal_id) REFERENCES company_master(company_internal_id)
    )
    ''')
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_mas_product_unique ON mas_product(company_internal_id, contract_no_hash, product_name_normalized, detail_product_code, source_name)')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS mas_supplier (
        mas_supplier_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER,
        supplier_name TEXT,
        supplier_name_normalized TEXT,
        supplier_business_no_hash TEXT,
        is_busan_company BOOLEAN,
        is_headquarters BOOLEAN,
        source_name TEXT,
        source_refreshed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(company_internal_id) REFERENCES company_master(company_internal_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS mas_contract (
        mas_contract_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER,
        contract_no_hash TEXT NOT NULL,
        product_name TEXT,
        product_code TEXT,
        detail_product_name TEXT,
        detail_product_code TEXT,
        contract_start_date DATE,
        contract_end_date DATE,
        contract_status TEXT NOT NULL DEFAULT 'unknown',
        source_name TEXT,
        source_refreshed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(company_internal_id) REFERENCES company_master(company_internal_id)
    )
    ''')
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_mas_contract_unique ON mas_contract(company_internal_id, contract_no_hash, product_code, detail_product_code, source_name)')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS mas_price_condition (
        mas_price_condition_id INTEGER PRIMARY KEY AUTOINCREMENT,
        mas_product_id INTEGER NOT NULL,
        price_amount NUMERIC,
        price_unit TEXT,
        min_order_quantity NUMERIC,
        max_order_quantity NUMERIC,
        delivery_condition TEXT,
        region_condition TEXT,
        option_summary TEXT,
        source_name TEXT,
        source_refreshed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(mas_product_id) REFERENCES mas_product(mas_product_id)
    )
    ''')
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_mas_price_condition_unique ON mas_price_condition(mas_product_id, source_name)')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS mas_product_unmatched (
        unmatched_id INTEGER PRIMARY KEY AUTOINCREMENT,
        raw_mas_import_id INTEGER,
        source_name TEXT,
        raw_company_name TEXT,
        raw_business_no_hash TEXT,
        raw_product_name TEXT,
        reason TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS mas_product_conflict_log (
        conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER,
        contract_no_hash TEXT,
        product_name TEXT,
        conflict_reason TEXT,
        source_1 TEXT,
        source_2 TEXT,
        resolved_action TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # ==========================================
    # Phase 6-D-2: 물품인증유형목록 분류 고도화
    # ==========================================

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS procurement_label_map (
        raw_label TEXT PRIMARY KEY,
        normalized_label TEXT NOT NULL,
        target_domain TEXT NOT NULL,
        target_type TEXT NOT NULL,
        is_candidate_type_promotable BOOLEAN DEFAULT 0,
        is_active BOOLEAN DEFAULT 1,
        notes TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Seed: 제품 인증·지정 (target_domain=product_certification)
    cursor.execute('''
        INSERT OR IGNORE INTO procurement_label_map
        (raw_label, normalized_label, target_domain, target_type, is_candidate_type_promotable, notes)
        VALUES
        ('성능인증', '성능인증', 'product_certification', 'performance_certification', 1, '제품 인증'),
        ('성능인증제품', '성능인증제품', 'product_certification', 'performance_certification', 1, '제품 인증 변형'),
        ('NET', 'NET', 'product_certification', 'net_certified_product', 1, '제품 인증'),
        ('신기술인증(NET)', '신기술인증(NET)', 'product_certification', 'net_certified_product', 1, '제품 인증'),
        ('NEP', 'NEP', 'product_certification', 'nep_product', 1, '제품 인증'),
        ('신제품인증(NEP)', '신제품인증(NEP)', 'product_certification', 'nep_product', 1, '제품 인증'),
        ('GS', 'GS', 'product_certification', 'gs_certified_product', 1, '제품 인증'),
        ('GS인증', 'GS인증', 'product_certification', 'gs_certified_product', 1, '제품 인증'),
        ('GS인증(1등급)', 'GS인증(1등급)', 'product_certification', 'gs_certified_product', 1, '제품 인증 등급 변형'),
        ('우수조달물품', '우수조달물품', 'product_certification', 'excellent_procurement_product', 1, '제품 인증'),
        ('우수조달물품지정', '우수조달물품지정', 'product_certification', 'excellent_procurement_product', 1, '제품 인증'),
        ('혁신제품', '혁신제품', 'product_certification', 'innovation_product', 1, '제품 인증'),
        ('혁신시제품', '혁신시제품', 'product_certification', 'innovation_prototype_product', 1, '제품 인증'),
        ('우수연구개발혁신제품', '우수연구개발혁신제품', 'product_certification', 'excellent_rnd_innovation_product', 1, '제품 인증'),
        ('기타혁신제품', '기타혁신제품', 'product_certification', 'other_innovation_product', 1, '제품 인증'),
        ('재난안전제품인증', '재난안전제품인증', 'product_certification', 'disaster_safety_certified_product', 1, '제품 인증'),
        ('녹색기술제품', '녹색기술제품', 'product_certification', 'green_technology_product', 1, '제품 인증'),
        ('녹색인증제품', '녹색인증제품', 'product_certification', 'green_technology_product', 1, '제품 인증 변형'),
        ('산업융합 신제품 적합성 인증', '산업융합 신제품 적합성 인증', 'product_certification', 'industrial_convergence_new_product', 1, '제품 인증'),
        ('우수조달공동상표', '우수조달공동상표', 'product_certification', 'excellent_procurement_joint_brand', 1, '제품 인증'),
        ('품질보증조달물품', '품질보증조달물품', 'product_certification', 'quality_assured_procurement_product', 0, '제품 인증 비승격'),
        ('우수발명품', '우수발명품', 'product_certification', 'excellent_invention_product', 0, '제품 인증 비승격'),
        ('보안성능품질인증', '보안성능품질인증', 'product_certification', 'security_quality_certification', 0, '제품 인증 비승격'),
        ('상생협력제품', '상생협력제품', 'product_certification', 'win_win_cooperation_product', 0, '제품 인증 비승격')
    ''')

    # Seed: 업체/정책 속성 (target_domain=company_procurement_attribute)
    cursor.execute('''
        INSERT OR IGNORE INTO procurement_label_map
        (raw_label, normalized_label, target_domain, target_type, is_candidate_type_promotable, notes)
        VALUES
        ('소기업', '소기업', 'company_procurement_attribute', 'small_business', 0, '업체 속성'),
        ('소상공인', '소상공인', 'company_procurement_attribute', 'small_merchant', 0, '업체 속성'),
        ('여성기업제품', '여성기업제품', 'company_procurement_attribute', 'women_company_product_label', 0, '업체/제품 정책 라벨'),
        ('장애인기업제품', '장애인기업제품', 'company_procurement_attribute', 'disabled_company_product_label', 0, '업체/제품 정책 라벨'),
        ('창업기업제품', '창업기업제품', 'company_procurement_attribute', 'startup_company_product_label', 0, '업체/제품 정책 라벨'),
        ('사회적기업제품', '사회적기업제품', 'company_procurement_attribute', 'social_enterprise_product_label', 0, '업체/제품 정책 라벨'),
        ('장애인표준사업장', '장애인표준사업장', 'company_procurement_attribute', 'disabled_standard_workplace', 0, '업체 속성'),
        ('중증장애인생산품', '중증장애인생산품', 'company_procurement_attribute', 'severe_disabled_production_label', 0, '업체 속성'),
        ('가족친화인증기업', '가족친화인증기업', 'company_procurement_attribute', 'family_friendly_company', 0, '업체 속성'),
        ('인적자원개발 우수기업', '인적자원개발 우수기업', 'company_procurement_attribute', 'excellent_hr_development', 0, '업체 속성'),
        ('정규직전환기업', '정규직전환기업', 'company_procurement_attribute', 'permanent_employment_conversion', 0, '업체 속성')
    ''')

    # Seed: 일반 인증/기타 (target_domain=general_certification)
    cursor.execute('''
        INSERT OR IGNORE INTO procurement_label_map
        (raw_label, normalized_label, target_domain, target_type, is_candidate_type_promotable, notes)
        VALUES
        ('KS', 'KS', 'general_certification', 'ks', 0, '일반 인증'),
        ('KC', 'KC', 'general_certification', 'kc', 0, '일반 인증'),
        ('KC인증', 'KC인증', 'general_certification', 'kc', 0, '일반 인증 변형'),
        ('특허', '특허', 'general_certification', 'patent', 0, '일반 인증'),
        ('단체표준', '단체표준', 'general_certification', 'group_standard', 0, '일반 인증'),
        ('단체표준인증', '단체표준인증', 'general_certification', 'group_standard', 0, '일반 인증 변형'),
        ('G-PASS', 'G-PASS', 'general_certification', 'gpass', 0, '일반 인증'),
        ('G-PASS기업', 'G-PASS기업', 'general_certification', 'gpass', 0, '일반 인증 변형'),
        ('G-PASS기업(A등급)', 'G-PASS기업(A등급)', 'general_certification', 'gpass', 0, '일반 인증 등급'),
        ('G-PASS기업(B등급)', 'G-PASS기업(B등급)', 'general_certification', 'gpass', 0, '일반 인증 등급'),
        ('ISO', 'ISO', 'general_certification', 'iso', 0, '일반 인증'),
        ('환경표지', '환경표지', 'general_certification', 'environmental_label', 0, '일반 인증'),
        ('환경표지제품', '환경표지제품', 'general_certification', 'environmental_label', 0, '일반 인증 변형'),
        ('GR', 'GR', 'general_certification', 'good_recycled_product', 0, '일반 인증'),
        ('GR(우수재활용)', 'GR(우수재활용)', 'general_certification', 'good_recycled_product', 0, '일반 인증 변형'),
        ('고효율에너지기자재', '고효율에너지기자재', 'general_certification', 'high_efficiency_energy_equipment', 0, '일반 인증'),
        ('고효율기자재', '고효율기자재', 'general_certification', 'high_efficiency_energy_equipment', 0, '일반 인증 변형'),
        ('에너지소비효율 1등급', '에너지소비효율 1등급', 'general_certification', 'energy_efficiency_grade1', 0, '일반 인증'),
        ('에너지절약', '에너지절약', 'general_certification', 'energy_saving', 0, '일반 인증'),
        ('문화상품', '문화상품', 'general_certification', 'cultural_product', 0, '기타')
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS company_procurement_attribute (
        attribute_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER NOT NULL,
        attribute_type TEXT NOT NULL,
        attribute_label TEXT,
        product_name TEXT NOT NULL DEFAULT '',
        product_code TEXT NOT NULL DEFAULT '',
        detail_product_code TEXT NOT NULL DEFAULT '',
        source_name TEXT NOT NULL DEFAULT '',
        source_refreshed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(company_internal_id) REFERENCES company_master(company_internal_id)
    )
    ''')
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_company_procurement_attribute_unique ON company_procurement_attribute(company_internal_id, attribute_type, product_name, detail_product_code, source_name)')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS product_general_certification (
        general_cert_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_internal_id INTEGER NOT NULL,
        raw_cert_label TEXT NOT NULL,
        normalized_cert_type TEXT NOT NULL DEFAULT '',
        product_name TEXT NOT NULL DEFAULT '',
        product_code TEXT NOT NULL DEFAULT '',
        detail_product_code TEXT NOT NULL DEFAULT '',
        source_name TEXT NOT NULL DEFAULT '',
        source_refreshed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(company_internal_id) REFERENCES company_master(company_internal_id)
    )
    ''')
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_product_general_cert_unique ON product_general_certification(company_internal_id, normalized_cert_type, product_name, detail_product_code, source_name)')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS procurement_label_mapping_review (
        review_id INTEGER PRIMARY KEY AUTOINCREMENT,
        raw_label TEXT NOT NULL,
        product_name TEXT,
        product_code TEXT,
        detail_product_code TEXT,
        company_internal_id INTEGER,
        source_name TEXT,
        reason TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # ==========================================
    # Phase 2 & 4 & 5 & 6-C & 6-D-2 뷰 생성
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
        
        -- Phase 6-C: MAS 쇼핑몰 연동
        -- active 계약이 존재하면 mas_registered 플래그 포함.
        (SELECT CASE WHEN COUNT(*) > 0 THEN 'mas_registered' ELSE '' END 
         FROM mas_product mp 
         WHERE mp.company_internal_id = m.company_internal_id AND mp.contract_status = 'active') AS shopping_mall_flags_raw,
         
        (SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END 
         FROM company_product cp 
         JOIN ref_sme_competition_product r ON cp.product_code = r.detail_category_code
         WHERE cp.company_internal_id = m.company_internal_id) AS is_sme_competition_product,
         
        -- active 상태인 MAS 제품을 최대 5개까지만 노출하도록 제한 처리된 서브쿼리 사용 (SQLite GROUP_CONCAT 제약 우회를 위해 ORDER BY 사용 불가할 수 있으므로 기본 스칼라 연결)
        -- SQLite에서 LIMIT을 적용한 GROUP_CONCAT은 서브쿼리 활용.
        (SELECT GROUP_CONCAT(
            mp_sub.product_name || '^^' || 
            IFNULL(mp_sub.detail_product_code, '') || '^^' || 
            mp_sub.contract_status || '^^' || 
            IFNULL(mp_sub.contract_end_date, '') || '^^' || 
            IFNULL(mp_sub.price_amount, '') || '^^' || 
            IFNULL(mp_sub.price_unit, '') || '^^' || 
            mp_sub.source_name, '|||')
         FROM (
            SELECT product_name, detail_product_code, contract_status, contract_end_date, price_amount, price_unit, source_name
            FROM mas_product
            WHERE company_internal_id = m.company_internal_id AND contract_status = 'active'
            ORDER BY contract_end_date DESC
            LIMIT 5
         ) mp_sub
        ) AS mas_product_summary_raw,

        -- Phase 6-D-2: 업체/정책 속성
        (SELECT GROUP_CONCAT(attribute_type, '|')
         FROM (
           SELECT DISTINCT attribute_type
           FROM company_procurement_attribute cpa
           WHERE cpa.company_internal_id = m.company_internal_id
         )) AS procurement_attributes_raw,

        -- Phase 6-D-2: 일반 인증/기타
        (SELECT GROUP_CONCAT(normalized_cert_type, '|')
         FROM (
           SELECT DISTINCT normalized_cert_type
           FROM product_general_certification pgc
           WHERE pgc.company_internal_id = m.company_internal_id
         )) AS general_certifications_raw,

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

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ref_sme_competition_product (
        detail_category_code TEXT PRIMARY KEY,
        category_name TEXT,
        detail_category_name TEXT,
        sme_competition_target BOOLEAN DEFAULT 1,
        direct_purchase_target BOOLEAN DEFAULT 0,
        valid_start_date DATE,
        valid_end_date DATE,
        source_name TEXT,
        source_refreshed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
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
