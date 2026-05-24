# 부산 지역업체 DB 및 챗봇 API 사양서

> 최종 업데이트: 2026-05-08  
> 용도: 부산 조달 법령 자문 챗봇에 **지역업체 정보**를 제공하기 위한 전용 DB 및 API

---

## 1. 개요

이 시스템은 **모니터링 대시보드용 DB와 별개로**, 챗봇 에이전트가 부산 지역업체의 면허·인증·제품·정책 정보를 조회할 수 있도록 구축된 **챗봇 전용 인프라**이다.

```
busanproduct.co.kr (조달업체 등록정보)
조달청 Open API (인증제품/MAS/혁신제품)
정책기업 엑셀 (여성/장애인/사회적기업)
        │
        ▼  ETL 파이프라인
┌─────────────────────────────┐
│   chatbot_company.db (52MB) │  ← SQLite, /opt/busan/
│   36개 테이블 · 46,464 업체  │
└────────────┬────────────────┘
             │
             ▼  FastAPI (api_server.py)
┌─────────────────────────────┐
│  /api/chatbot/* (25개 엔드포인트) │  ← 포트 8000
└────────────┬────────────────┘
             │  HTTP
             ▼
┌─────────────────────────────┐
│  법령 자문 챗봇 (/opt/advisor) │  ← 포트 8502
│  candidate_policy.py 에서 호출 │
└─────────────────────────────┘
```

---

## 2. 데이터베이스 (`chatbot_company.db`)

### 2.1 핵심 테이블

| 테이블 | 건수 | 용도 |
|---|---:|---|
| `company_master` | 46,464 | 업체 기본정보 (소재지, 부산업체 여부, 본사/지점 구분) |
| `company_identity` | 46,464 | 업체 식별자 매핑 (`company_id` ↔ `company_internal_id`) |
| `company_business_status` | 46,029 | NTS 사업자 상태 (영업중/휴업/폐업, 갱신 시각) |
| `company_license` | 41,558 | 면허·업종 정보 (건설업 면허, 물품 업종 등) |
| `company_product` | 20,431 | 대표품명·물품분류코드 (조달청 세부품명 기준) |
| `certified_product` | 6,097 | 인증제품 (NEP/NET/GS/KS 등, 유효기간 포함) |
| `policy_company_certification` | 3,200 | 정책기업 인증 (여성/장애인/사회적기업/벤처 등) |
| `mas_product` | 2,603 | MAS 계약물품 (다수공급자 계약, 단가·수량 정보) |
| `shopping_mall_product` | 2,603 | 종합쇼핑몰 등록상품 (MAS/제3자단가/일반단가/우수조달) |
| `source_manifest` | 24 | ETL 수집 이력 (소스별 최종 수집일·건수) |

### 2.2 보조 테이블

| 테이블 | 용도 |
|---|---|
| `certified_product_type_map` | 인증유형 → 우선구매/혁신/우수조달 분류 매핑 |
| `procurement_label_map` | 조달 라벨 → 도메인(제품인증/기업속성/일반인증) 분류 |
| `company_procurement_attribute` | 업체 조달 속성 (중소기업/소상공인 등) |
| `product_general_certification` | 일반 인증 (ISO/KS/Q-Mark 등) |
| `company_manufacturer_status` | 제조업체 여부·유형 |
| `ref_sme_competition_product` | 중소기업 간 경쟁제품 코드 참조 테이블 |
| `search_dictionary` | 검색어 사전 (미적재) |
| `mas_contract` / `mas_price_condition` / `mas_supplier` | MAS 계약·단가·공급업체 상세 |
| `business_status_refresh_queue` | NTS 상태 갱신 대기열 |
| `*_conflict_log` / `*_unmatched` | ETL 충돌·미매칭 로그 |
| `raw_*_import` | 원시 임포트 데이터 (PII 해시 처리됨) |
| `etl_job_log` | ETL 작업 실행 로그 |

### 2.3 핵심 뷰

#### `chatbot_company_candidate_view`

모든 검색 API의 기본 데이터 소스. `company_master` + `company_identity` + `company_business_status`를 조인하고, 서브쿼리로 면허·품목·인증·정책·쇼핑몰 정보를 집계하여 단일 Row로 제공한다.

주요 컬럼:

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `company_id` | text | 외부 노출용 식별자 (HMAC 해시) |
| `company_name` | text | 업체명 |
| `location` / `detail_address` | text | 시도 / 상세주소 |
| `license_or_business_type` | text | `|` 구분 면허명 목록 |
| `main_products` | text | `|` 구분 대표품명 목록 |
| `policy_subtypes_raw` | text | `subtype:validity_status` 형식 |
| `certified_product_types_raw` | text | 인증유형·우선구매·혁신·우수조달 플래그 |
| `shopping_mall_flags_raw` | text | 쇼핑몰 등록 플래그 |
| `shopping_mall_product_summary_raw` | text | 쇼핑몰 상품 요약 (최대 5개) |
| `mas_product_summary_raw` | text | MAS 상품 요약 (최대 5개) |
| `business_status` | text | `active` / `closed` / `suspended` / `unknown` |
| `manufacturer_type` | text | 제조업체 유형 |
| `is_sme_competition_product` | int | 중소기업 경쟁제품 보유 여부 |

---

## 3. ETL 파이프라인

### 3.1 초기 구축 (1회성)

| 순서 | 스크립트 | 데이터 소스 | 설명 |
|:---:|---|---|---|
| 1 | `migrate_chatbot_db.py` | - | 스키마 생성 (36개 테이블 + 뷰 + 인덱스) |
| 2 | `bootstrap_master_data.py` | `busan_companies_master.db` | 부산 업체 마스터 46,464건 이관 |
| 3 | `bootstrap_from_excel.py` | 조달데이터허브 엑셀 | 정책기업·제조업체·MAS·혁신제품·중소기업경쟁제품 |

### 3.2 일일 자동 갱신 (`daily_pipeline_sync.py`)

| Step | 스크립트 | 주기 | 설명 |
|:---:|---|---|---|
| 3.5 | `migrate_chatbot_db.py` | 매일 03:00 | 스키마 마이그레이션 (변경 시) |
| 4.1 | `import_certified_product_api.py` | 매일 | 조달청 API → 인증제품 갱신 |
| 4.2 | `import_innovation_product_api.py` | 매일 | 조달청 API → 혁신제품 갱신 |
| 4.3 | `import_mas_product_api.py` | 매일 | 조달청 API → MAS/쇼핑몰 상품 갱신 |

### 3.3 PII 보호

- 사업자번호 등 민감정보는 **HMAC-SHA256 해시**로 변환하여 저장
- `raw_*_import` 테이블에도 평문 데이터 없음
- API 응답에서 `representative_name`, `corporate_phone`은 `None`으로 마스킹

---

## 4. API 엔드포인트

**Base URL**: `http://49.50.133.160:8000`

### 4.1 시스템

| 메서드 | 경로 | 용도 |
|---|---|---|
| GET | `/api/chatbot/health` | DB 연결 상태 + 테이블별 건수 |
| GET | `/api/chatbot/version` | API 버전·스키마·기능 목록 |

### 4.2 업체 검색 (Step 1)

| 메서드 | 경로 | 주요 파라미터 | 용도 |
|---|---|---|---|
| GET | `/api/chatbot/company/license-search` | `license_name` | 면허·업종명으로 업체 검색 |
| GET | `/api/chatbot/company/product-search` | `product_name` | 대표품명으로 업체 검색 |
| GET | `/api/chatbot/company/category-search` | `category_code` 또는 `category_name` | 물품분류코드/명으로 검색 |
| GET | `/api/chatbot/company/license-list` | - | 면허업종별 업체 수 통계 |
| GET | `/api/chatbot/company/product-list` | - | 대표품명별 업체 수 통계 |
| GET | `/api/chatbot/company/category-list` | - | 물품분류별 업체 수 통계 |
| GET | `/api/chatbot/company/manufacturers` | - | 제조업체 전체 목록 |

### 4.3 업체 상세 (Step 2)

| 메서드 | 경로 | 주요 파라미터 | 용도 |
|---|---|---|---|
| GET | `/api/chatbot/company/detail` | `company_id` (필수) | **종합 프로필** — 면허·인증·MAS·정책·NTS 상태 통합 조회 |

> **핵심 API**: 검색(Step 1)에서 `company_id`를 얻은 후, 반드시 이 API로 상세 조회해야 완전한 정보를 얻을 수 있다.

### 4.4 정책기업 검색

| 메서드 | 경로 | 주요 파라미터 | 용도 |
|---|---|---|---|
| GET | `/api/chatbot/company/policy-search` | `policy_subtype` | 여성/장애인/사회적기업/벤처 등 |
| GET | `/api/chatbot/company/policy-list` | - | 정책유형별 통계 |

### 4.5 인증제품 검색 (Phase 5)

| 메서드 | 경로 | 주요 파라미터 | 용도 |
|---|---|---|---|
| GET | `/api/chatbot/product/certified-search` | `product_name`, `certification_type` | 인증유형+제품명 검색 |
| GET | `/api/chatbot/product/innovation-search` | `product_name` | 혁신제품 검색 |
| GET | `/api/chatbot/product/priority-purchase-search` | `product_name` | 우선구매 대상 제품 |
| GET | `/api/chatbot/product/excellent-procurement-search` | `product_name` | 우수조달물품 |
| GET | `/api/chatbot/product/certified-list` | - | 인증유형별 통계 |

### 4.6 MAS 검색 (Phase 6-C)

| 메서드 | 경로 | 주요 파라미터 | 용도 |
|---|---|---|---|
| GET | `/api/chatbot/mas/search` | `query` | MAS 종합 검색 |
| GET | `/api/chatbot/mas/product-search` | `product_name` | MAS 제품명 검색 |
| GET | `/api/chatbot/mas/supplier-search` | `company_keyword` | MAS 공급업체 검색 |
| GET | `/api/chatbot/mas/list` | - | MAS 세부품명별 통계 |

### 4.7 종합쇼핑몰 검색 (Phase 6-G)

| 메서드 | 경로 | 주요 파라미터 | 용도 |
|---|---|---|---|
| GET | `/api/chatbot/shopping-mall/search` | `query` | 종합쇼핑몰 종합 검색 |
| GET | `/api/chatbot/shopping-mall/product-search` | `product_name` | 쇼핑몰 제품명 검색 |
| GET | `/api/chatbot/shopping-mall/supplier-search` | `company_keyword` | 쇼핑몰 공급업체 검색 |
| GET | `/api/chatbot/shopping-mall/list` | - | 계약유형별 통계 |

---

## 5. 공통 필터

모든 검색 API에 공통으로 적용되는 필터:

| 필터 | 값 | 기본값 | 설명 |
|---|---|:---:|---|
| `status_filter` | `exclude_closed` | ✅ | 휴·폐업 업체 제외 |
| | `all` | | 전체 표시 |
| | `active_only` | | 영업중만 |
| | `needs_check` | | 상태 미확인 업체 |
| `validity_filter` | `valid_only` | ✅ | 유효 인증만 |
| | `include_unknown` | | 미확인 포함 |
| | `all` | | 만료 포함 |
| `contract_status_filter` | `active_only` | ✅ | 유효 계약만 |
| | `include_unknown` | | 미확인 포함 |
| | `all` | | 만료 계약 포함 |
| `contract_type_filter` | `all` | ✅ | 전체 계약유형 |
| | `mas` | | MAS만 |
| | `third_party_unit_price` | | 제3자단가만 |
| | `general_unit_price` | | 일반단가만 |
| | `excellent_procurement` | | 우수조달만 |

---

## 6. 응답 구조

모든 챗봇 API는 `_build_chatbot_response()`를 통해 통일된 형식으로 응답한다.

```json
{
  "meta": {
    "query_params": { ... },
    "candidate_counts_by_type": { "local_procurement_company": 15 },
    "source_refreshed_at": "2026-05-08T03:00:00"
  },
  "candidates": [
    {
      "company_id": "C-a1b2c3d4",
      "company_name": "(주)부산전자",
      "location": "부산광역시",
      "license_or_business_type": ["전기공사업", "정보통신공사업"],
      "main_products": ["CCTV", "네트워크장비"],
      "candidate_types": ["local_procurement_company", "shopping_mall_supplier"],
      "policy_subtypes": ["woman_enterprise"],
      "certified_product_types": ["NEP"],
      "shopping_mall_flags": ["shopping_mall_registered", "mas_registered"],
      "business_status": "active",
      "display_status": "영업중"
    }
  ],
  "company_source_status": "db_success",
  "company_search_status": "ok",
  "company_cache_used": false,
  "company_cache_mode": "none"
}
```

### 필드 화이트리스트 (27개)

API 응답의 `candidates` 배열에는 `ALLOWED_CANDIDATE_FIELDS`에 정의된 27개 필드만 포함된다. PII(사업자번호, 대표자명 등)는 화이트리스트에서 제외되어 자동 차단된다.

---

## 7. 안전장치

| 항목 | 내용 |
|---|---|
| **PII 차단** | `ALLOWED_CANDIDATE_FIELDS` 화이트리스트 27개 필드만 노출 |
| **PII 마스킹** | `detail` API에서 `representative_name`, `corporate_phone` → `None` |
| **폐업 기본 제외** | `status_filter=exclude_closed` 기본값 |
| **인증 유효성** | `validity_filter=valid_only` — 만료 인증 기본 제외 |
| **법적 결론 금지** | 에이전트 가이드에서 계약 체결 가부 등 법적 판단 출력 차단 |
| **사업자번호 해시** | DB 저장 시 HMAC-SHA256, 내부적으로 `company_internal_id`만 사용 |

---

## 8. 관련 파일

| 파일 | 위치 | 역할 |
|---|---|---|
| `api_server.py` (640~2145줄) | `/opt/busan/` | 챗봇 API 엔드포인트 구현 |
| `migrate_chatbot_db.py` | `/opt/busan/` | DB 스키마 생성·마이그레이션 |
| `bootstrap_master_data.py` | `/opt/busan/` | 업체 마스터 초기 이관 |
| `bootstrap_from_excel.py` | `/opt/busan/` | 엑셀 데이터 초기 적재 |
| `import_certified_product_api.py` | `/opt/busan/` | 인증제품 일일 갱신 |
| `import_innovation_product_api.py` | `/opt/busan/` | 혁신제품 일일 갱신 |
| `import_mas_product_api.py` | `/opt/busan/` | MAS/쇼핑몰 일일 갱신 |
| `chatbot_agent_api_guide.md` | `/opt/busan/docs/` | 챗봇 에이전트용 API 사용 지침 |
