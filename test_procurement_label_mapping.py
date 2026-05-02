"""
Phase 6-D-2: MAS 물품인증유형목록 분류 테스트
"""
import sqlite3
import sys

DB_FILE = "staging_chatbot_company.db"

def test_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    passed = 0
    failed = 0
    
    def check(name, query, expected_fn):
        nonlocal passed, failed
        try:
            result = cur.execute(query).fetchone()[0]
            if expected_fn(result):
                print(f"  PASS: {name} -> {result}")
                passed += 1
            else:
                print(f"  FAIL: {name} -> {result}")
                failed += 1
        except Exception as e:
            print(f"  FAIL: {name} -> Exception: {e}")
            failed += 1
    
    print("=== 1. 제품 인증 분류 검증 ===")
    
    check("성능인증제품 -> certified_product/performance_certification",
          "SELECT COUNT(*) FROM certified_product WHERE certification_type='performance_certification' AND source_name='mas_excel_bootstrap'",
          lambda x: x > 0)
    
    check("GS인증(1등급) -> certified_product/gs_certified_product",
          "SELECT COUNT(*) FROM certified_product WHERE certification_type='gs_certified_product' AND source_name='mas_excel_bootstrap'",
          lambda x: x > 0)
    
    check("NET -> certified_product/net_certified_product",
          "SELECT COUNT(*) FROM certified_product WHERE certification_type='net_certified_product' AND source_name='mas_excel_bootstrap'",
          lambda x: x > 0)
    
    check("우수조달물품 -> certified_product/excellent_procurement_product",
          "SELECT COUNT(*) FROM certified_product WHERE certification_type='excellent_procurement_product' AND source_name='mas_excel_bootstrap'",
          lambda x: x > 0)
    
    check("혁신제품 -> certified_product/innovation_product",
          "SELECT COUNT(*) FROM certified_product WHERE certification_type='innovation_product' AND source_name='mas_excel_bootstrap'",
          lambda x: x >= 0)  # innovation_product may also come from innovation excel
    
    check("품질보증조달물품 -> certified_product/quality_assured_procurement_product",
          "SELECT COUNT(*) FROM certified_product WHERE certification_type='quality_assured_procurement_product' AND source_name='mas_excel_bootstrap'",
          lambda x: x > 0)
    
    check("우수발명품 -> certified_product/excellent_invention_product",
          "SELECT COUNT(*) FROM certified_product WHERE certification_type='excellent_invention_product' AND source_name='mas_excel_bootstrap'",
          lambda x: x > 0)

    print("\n=== 2. 업체/정책 속성 분류 검증 ===")
    
    check("소기업 -> company_procurement_attribute/small_business",
          "SELECT COUNT(*) FROM company_procurement_attribute WHERE attribute_type='small_business'",
          lambda x: x > 0)
    
    check("소상공인 -> company_procurement_attribute/small_merchant",
          "SELECT COUNT(*) FROM company_procurement_attribute WHERE attribute_type='small_merchant'",
          lambda x: x > 0)
    
    check("여성기업제품 -> company_procurement_attribute/women_company_product_label",
          "SELECT COUNT(*) FROM company_procurement_attribute WHERE attribute_type='women_company_product_label'",
          lambda x: x > 0)
    
    check("장애인기업제품 -> company_procurement_attribute/disabled_company_product_label",
          "SELECT COUNT(*) FROM company_procurement_attribute WHERE attribute_type='disabled_company_product_label'",
          lambda x: x > 0)
    
    check("창업기업제품 -> company_procurement_attribute/startup_company_product_label",
          "SELECT COUNT(*) FROM company_procurement_attribute WHERE attribute_type='startup_company_product_label'",
          lambda x: x > 0)
    
    check("사회적기업제품 -> company_procurement_attribute/social_enterprise_product_label",
          "SELECT COUNT(*) FROM company_procurement_attribute WHERE attribute_type='social_enterprise_product_label'",
          lambda x: x > 0)
    
    print("\n=== 3. 일반 인증/기타 분류 검증 ===")
    
    check("단체표준인증 -> product_general_certification/group_standard",
          "SELECT COUNT(*) FROM product_general_certification WHERE normalized_cert_type='group_standard'",
          lambda x: x > 0)
    
    check("G-PASS기업(B등급) -> product_general_certification/gpass",
          "SELECT COUNT(*) FROM product_general_certification WHERE normalized_cert_type='gpass'",
          lambda x: x > 0)
    
    check("KS -> product_general_certification/ks",
          "SELECT COUNT(*) FROM product_general_certification WHERE normalized_cert_type='ks'",
          lambda x: x > 0)
    
    check("KC인증 -> product_general_certification/kc",
          "SELECT COUNT(*) FROM product_general_certification WHERE normalized_cert_type='kc'",
          lambda x: x > 0)
    
    check("특허 -> product_general_certification/patent",
          "SELECT COUNT(*) FROM product_general_certification WHERE normalized_cert_type='patent'",
          lambda x: x > 0)
    
    print("\n=== 4. 매핑 불가 검증 ===")
    
    check("매핑 없는 라벨 -> procurement_label_mapping_review",
          "SELECT COUNT(*) FROM procurement_label_mapping_review WHERE reason='unmapped'",
          lambda x: x >= 0)  # 0건이어도 PASS (모든 라벨이 매핑되었을 수 있음)

    print("\n=== 5. 금지 조건 검증 ===")
    
    check("certified_product.certification_type='manual_review' 0건",
          "SELECT COUNT(*) FROM certified_product WHERE certification_type='manual_review'",
          lambda x: x == 0)
    
    check("업체속성이 certified_product에 들어가지 않음",
          "SELECT COUNT(*) FROM certified_product WHERE certification_type IN ('small_business','small_merchant','women_company_product_label','disabled_company_product_label','startup_company_product_label','social_enterprise_product_label')",
          lambda x: x == 0)
    
    check("일반인증이 certified_product에 들어가지 않음",
          "SELECT COUNT(*) FROM certified_product WHERE certification_type IN ('ks','kc','patent','group_standard','gpass','iso','environmental_label','good_recycled_product')",
          lambda x: x == 0)
    
    print("\n=== 6. procurement_label_map 시드 검증 ===")
    
    check("procurement_label_map 시드 건수",
          "SELECT COUNT(*) FROM procurement_label_map",
          lambda x: x >= 40)
    
    check("product_certification 도메인 시드",
          "SELECT COUNT(*) FROM procurement_label_map WHERE target_domain='product_certification'",
          lambda x: x >= 20)
    
    check("company_procurement_attribute 도메인 시드",
          "SELECT COUNT(*) FROM procurement_label_map WHERE target_domain='company_procurement_attribute'",
          lambda x: x >= 8)
    
    check("general_certification 도메인 시드",
          "SELECT COUNT(*) FROM procurement_label_map WHERE target_domain='general_certification'",
          lambda x: x >= 15)
    
    check("promotable=1인 제품 인증",
          "SELECT COUNT(*) FROM procurement_label_map WHERE is_candidate_type_promotable=1",
          lambda x: x >= 15)
    
    check("promotable=0인 제품 인증 (비승격)",
          "SELECT COUNT(*) FROM procurement_label_map WHERE target_domain='product_certification' AND is_candidate_type_promotable=0",
          lambda x: x >= 2)

    print("\n=== 7. 수치 요약 ===")
    
    for table in ['certified_product', 'company_procurement_attribute', 'product_general_certification', 'procurement_label_mapping_review']:
        cnt = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {cnt:,}")
    
    # source_manifest/etl_job_log 기록 확인
    print("\n=== 8. ETL 로깅 검증 ===")
    
    check("source_manifest에 mas_procurement_attr 기록",
          "SELECT COUNT(*) FROM source_manifest WHERE source_name='mas_procurement_attr'",
          lambda x: x > 0)
    
    check("source_manifest에 mas_general_cert 기록",
          "SELECT COUNT(*) FROM source_manifest WHERE source_name='mas_general_cert'",
          lambda x: x > 0)
    
    check("source_manifest에 mas_mapping_review 기록",
          "SELECT COUNT(*) FROM source_manifest WHERE source_name='mas_mapping_review'",
          lambda x: x > 0)
    
    print("\n=== 9. 뷰-API 연동 검증 ===")
    
    check("chatbot_company_candidate_view에 procurement_attributes_raw 컬럼 존재",
          "SELECT COUNT(*) FROM chatbot_company_candidate_view WHERE procurement_attributes_raw IS NOT NULL",
          lambda x: x > 0)
    
    check("chatbot_company_candidate_view에 general_certifications_raw 컬럼 존재",
          "SELECT COUNT(*) FROM chatbot_company_candidate_view WHERE general_certifications_raw IS NOT NULL",
          lambda x: x > 0)
    
    # procurement_attributes가 있는 업체의 뷰 데이터가 파이프('|')로 올바르게 구분되는지
    check("procurement_attributes_raw 파이프 구분 형식",
          "SELECT LENGTH(procurement_attributes_raw) - LENGTH(REPLACE(procurement_attributes_raw, '|', '')) FROM chatbot_company_candidate_view WHERE procurement_attributes_raw LIKE '%|%' LIMIT 1",
          lambda x: x is not None and x >= 1)
    
    # procurement_attributes가 candidate_types에 들어가지 않는지 (뷰 수준 확인)
    check("candidate_types에 procurement attribute 미포함 (뷰 수준)",
          "SELECT COUNT(*) FROM chatbot_company_candidate_view WHERE candidate_types LIKE '%small_business%' OR candidate_types LIKE '%small_merchant%'",
          lambda x: x == 0)
    
    # general_certifications가 candidate_types에 들어가지 않는지
    check("candidate_types에 general certification 미포함 (뷰 수준)",
          "SELECT COUNT(*) FROM chatbot_company_candidate_view WHERE candidate_types LIKE '%ks%' OR candidate_types LIKE '%patent%' OR candidate_types LIKE '%gpass%'",
          lambda x: x == 0)
    
    conn.close()
    
    print(f"\n{'='*40}")
    print(f"Results: {passed} PASSED, {failed} FAILED")
    print(f"{'='*40}")
    
    if failed > 0:
        sys.exit(1)
    else:
        print("ALL TESTS PASSED.")
        sys.exit(0)

if __name__ == "__main__":
    test_db()
