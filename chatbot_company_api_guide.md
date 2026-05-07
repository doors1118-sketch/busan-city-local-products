# 부산 지역업체 DB 및 API 가이드

> 이 문서는 챗봇 에이전트가 부산 지역업체 데이터를 조회하고 활용할 때 참고하는 레퍼런스입니다.
> API 서버: `https://busanproduct.co.kr` (내부: `http://localhost:8000`)

---

## 1. 데이터 개요

부산광역시에 소재한 조달업체 **28,163개사**(영업중 기준)의 통합 데이터베이스입니다.

| 데이터 항목 | 건수 | 설명 |
|------------|-----:|------|
| 전체 업체 (부산) | 46,461 | 나라장터 등록 전체 (폐업 포함) |
| 영업중 업체 | 28,163 | 국세청 확인 영업중 업체 |
| 폐업 업체 | 17,658 | 국세청 확인 폐업 |
| 휴업 업체 | 117 | 국세청 확인 휴업 |
| 보유 면허 | 41,557 | 건축공사업, 전기공사업 등 |
| 등록 물품 | 20,431 | 물품분류별 등록 내역 |
| 종합쇼핑몰 등록 | 2,597 | MAS·일반단가·제3자단가·우수조달 |
| 인증제품 | 6,097 | NEP, NET, 혁신제품 등 19종 |
| 정책업체 | 3,200 | 사회적기업·여성기업·장애인기업 |

### 데이터 갱신 주기
- **매일 새벽 3시**: 나라장터 계약/낙찰 데이터 수집
- **매일 새벽 5시**: 챗봇 DB 갱신 (업체 마스터, 인증제품, 혁신장터, 종합쇼핑몰)
- **매주 일요일**: 국세청 영업상태 일괄 갱신 (영업/폐업/휴업 확인)

---

## 2. 업체 검색 API

### 2-1. 면허별 업체 조회

특정 면허(건설면허, 전기면허 등)를 보유한 부산 업체를 검색합니다.

```
GET /api/chatbot/company/license-search
  ?license_name=건축공사업    (필수: 면허명)
  &status_filter=exclude_closed  (선택: exclude_closed|active_only|all)
  &limit=50                  (선택: 최대 반환 건수)
```

**활용 예시**: "건축공사업 면허를 가진 부산 업체 알려줘"

### 2-2. 면허 목록 조회

등록된 모든 면허 종류와 보유 업체 수를 확인합니다.

```
GET /api/chatbot/company/license-list
  ?limit=100
```

**활용 예시**: "부산에 어떤 면허를 가진 업체가 많아?"

### 2-3. 물품별 업체 조회

특정 물품(컴퓨터, 사무용가구 등)을 등록한 부산 업체를 검색합니다.

```
GET /api/chatbot/company/product-search
  ?product_name=컴퓨터       (필수: 물품명)
  &limit=50
```

**활용 예시**: "컴퓨터를 납품할 수 있는 부산 업체는?"

### 2-4. 물품 목록 조회

등록된 모든 물품 분류와 업체 수를 확인합니다.

```
GET /api/chatbot/company/product-list
  ?limit=100
```

### 2-5. 업체 상세 조회

특정 업체의 면허, 물품, 인증, 쇼핑몰 등록, 정책업체 여부 등 전체 정보를 확인합니다.

```
GET /api/chatbot/company/detail
  ?company_id=xxxx           (필수: 업체 식별자)
```

**활용 예시**: "○○기업의 조달 자격 현황을 알려줘"

### 2-6. 제조업체 조회

부산 소재 제조업체 목록을 조회합니다.

```
GET /api/chatbot/company/manufacturers
  ?limit=50
```

---

## 3. 정책업체 API

사회적기업, 여성기업, 장애인기업 등 정책 인증을 보유한 업체를 조회합니다.

### 3-1. 정책업체 검색

```
GET /api/chatbot/company/policy-search
  ?policy_subtype=social_enterprise  (선택: 정책유형)
  &limit=50
```

### 3-2. 정책유형 목록

```
GET /api/chatbot/company/policy-list
```

### 정책유형 코드

| 코드 | 한글명 |
|------|--------|
| `social_enterprise` | 사회적기업 |
| `women_company` | 여성기업 |
| `disabled_company` | 장애인기업 |

**활용 예시**: "부산에 사회적기업으로 등록된 업체 알려줘"

---

## 4. 인증제품 API

기술개발제품(13종), 혁신제품, 우수조달물품 등 인증을 보유한 업체와 제품을 조회합니다.

### 4-1. 인증제품 검색

```
GET /api/chatbot/product/certified-search
  ?q=영상감시장치             (필수: 검색어)
  &cert_type=nep_product     (선택: 인증유형)
  &limit=50
```

### 4-2. 혁신제품 검색

```
GET /api/chatbot/product/innovation-search
  ?q=스마트                  (필수: 검색어)
  &limit=50
```

### 4-3. 우수조달물품 검색

```
GET /api/chatbot/product/excellent-procurement-search
  ?q=LED                     (필수: 검색어)
  &limit=50
```

### 4-4. 우선구매 대상제품 검색

```
GET /api/chatbot/product/priority-purchase-search
  ?q=정수기                  (필수: 검색어)
  &limit=50
```

### 4-5. 인증유형 목록

```
GET /api/chatbot/product/certified-list
```

### 인증유형 코드

| 코드 | 한글명 | 분류 |
|------|--------|------|
| `nep_product` | 신제품(NEP) 인증 | 기술개발제품 |
| `net_certified_product` | 신기술(NET) 인증 | 기술개발제품 |
| `performance_certification` | 성능인증 | 기술개발제품 |
| `green_technology_product` | 녹색기술인증 | 기술개발제품 |
| `gs_certified_product` | GS인증(소프트웨어) | 기술개발제품 |
| `demand_designated_tech_product` | 수요지정 기술개발제품 | 기술개발제품 |
| `excellent_industrial_design` | 우수디자인(GD) | 기술개발제품 |
| `excellent_invention_product` | 우수발명품 | 기술개발제품 |
| `excellent_rnd_innovation_product` | 우수연구개발 혁신제품 | 기술개발제품 |
| `industrial_convergence_item` | 산업융합 품목 | 기술개발제품 |
| `water_industry_excellent_product` | 물산업 우수제품 | 기술개발제품 |
| `disaster_safety_certified_product` | 재난안전 인증제품 | 기술개발제품 |
| `security_quality_certification` | 보안 품질인증 | 기술개발제품 |
| `innovation_product` | 혁신제품(혁신장터) | 혁신제품 |
| `innovation_prototype_product` | 혁신시제품 | 혁신제품 |
| `other_innovation_product` | 기타 혁신제품 | 혁신제품 |
| `excellent_procurement_product` | 우수조달물품 | 우수조달 |
| `quality_assured_procurement_product` | 품질보증조달물품 | 우수조달 |
| `win_win_cooperation_product` | 상생협력 제품 | 기타 |

**활용 예시**: "NEP 인증을 받은 부산 업체가 있어?", "혁신제품 등록된 부산 업체 찾아줘"

---

## 5. 종합쇼핑몰 API

나라장터 종합쇼핑몰(MAS, 일반단가계약, 제3자단가계약)에 등록된 부산 업체와 물품을 조회합니다.

### 5-1. 종합쇼핑몰 통합 검색

```
GET /api/chatbot/shopping-mall/search
  ?q=소독                    (필수: 검색어)
  &contract_type=mas         (선택: 계약유형)
  &limit=50
```

### 5-2. 물품명으로 검색

```
GET /api/chatbot/shopping-mall/product-search
  ?product_name=소독제       (필수: 물품명)
  &limit=50
```

### 5-3. 업체명으로 검색

```
GET /api/chatbot/shopping-mall/supplier-search
  ?company_name=○○기업      (필수: 업체명)
  &limit=50
```

### 5-4. 종합쇼핑몰 등록 현황

```
GET /api/chatbot/shopping-mall/list
  ?contract_type=mas         (선택: 계약유형)
```

### 계약유형 코드

| 코드 | 한글명 | 등록 건수 |
|------|--------|----------:|
| `mas` | 다수공급자계약(MAS) | 2,374 |
| `third_party_unit_price` | 제3자단가계약 | 132 |
| `excellent_procurement` | 우수조달물품 | 65 |
| `general_unit_price` | 일반단가계약 | 26 |

**활용 예시**: "MAS 등록된 부산 업체 알려줘", "종합쇼핑몰에서 소독제 납품 가능한 업체는?"

---

## 6. MAS(다수공급자계약) 전용 API

### 6-1. MAS 통합 검색

```
GET /api/chatbot/mas/search
  ?q=컴퓨터                  (필수: 검색어)
  &limit=50
```

### 6-2. MAS 물품 검색

```
GET /api/chatbot/mas/product-search
  ?product_name=데스크톱     (필수: 물품명)
```

### 6-3. MAS 업체 검색

```
GET /api/chatbot/mas/supplier-search
  ?company_name=○○전자      (필수: 업체명)
```

### 6-4. MAS 등록 목록

```
GET /api/chatbot/mas/list
```

---

## 7. 엑셀 다운로드 API

구군청 담당자가 업체 데이터를 엑셀 파일로 다운받을 수 있습니다.
**기본값**: 영업중 업체만 포함 (폐업·휴업 제외)

### 7-1. 통합 다운로드 (전체 업체)

모든 정보(면허, 물품, 정책, 인증, 쇼핑몰)를 하나의 엑셀로 다운로드합니다.

```
GET /api/download/all-companies
  ?status=active_only        (선택: active_only|exclude_closed|all)
  &limit=50000
```

### 7-2. 면허별 업체 다운로드

```
GET /api/download/license-companies
  ?license_name=건축공사업   (필수)
  &status=active_only        (선택)
```

### 7-3. 물품별 업체 다운로드

```
GET /api/download/product-companies
  ?product_name=컴퓨터       (필수)
  &status=active_only        (선택)
```

### 7-4. 정책업체 다운로드

```
GET /api/download/policy-companies
  ?policy_type=social_enterprise  (선택: 미지정 시 전체)
  &status=active_only        (선택)
```

### 7-5. 종합쇼핑몰 물품 다운로드

```
GET /api/download/shopping-mall-products
  ?contract_type=mas         (선택: 미지정 시 전체)
  &status=active_only        (선택)
```

### 7-6. 인증제품 다운로드

```
GET /api/download/certified-products
  ?cert_type=nep_product     (선택: 미지정 시 전체)
  &status=active_only        (선택)
```

### 영업상태 필터 옵션

| 값 | 의미 | 포함 업체 |
|----|------|----------|
| `active_only` (기본) | 영업중만 | 28,163개사 |
| `exclude_closed` | 폐업 제외 | 28,163 + 117(휴업) + 91(미확인) |
| `all` | 전체 | 46,461개사 (폐업 포함) |

**활용 예시**: "건축공사업 면허 업체 엑셀로 다운받고 싶어" → `/api/download/license-companies?license_name=건축공사업` 안내

---

## 8. 시스템 API

### 8-1. 헬스체크

```
GET /api/chatbot/health
```

DB 연결 상태, 각 테이블별 데이터 건수를 확인합니다.

### 8-2. 버전 정보

```
GET /api/chatbot/version
```

API 버전, 스키마 버전, 지원 기능 목록을 확인합니다.

---

## 9. 챗봇 응답 시 참고사항

### 영업상태 안내
- 모든 업체 데이터는 **국세청 사업자등록상태 조회**(매주 일요일 갱신)를 거친 결과입니다.
- 기본적으로 **영업중인 업체만** 제공하며, 폐업·휴업 업체는 제외됩니다.

### 데이터 출처
- **업체 기본정보**: 나라장터(조달청) 업체등록 마스터
- **면허/물품**: 나라장터 등록 정보
- **종합쇼핑몰**: 공공데이터포털 종합쇼핑몰 API (MAS·일반단가·제3자단가)
- **인증제품**: 공공데이터포털 기술개발제품 API (13종) + 혁신장터 API
- **정책업체**: 나라장터 정책업체 인증 정보
- **영업상태**: 국세청 사업자등록상태 조회 API

### "부산 업체"의 기준
- 나라장터에 등록된 업체 중 **본사 소재지가 부산광역시**인 업체
- 지사만 부산에 있는 경우는 포함하지 않음

### 엑셀 다운로드 안내 시
- URL을 그대로 안내하면 됩니다: `https://busanproduct.co.kr/api/download/...`
- 브라우저에서 해당 URL을 열면 즉시 엑셀 파일이 다운로드됩니다.
- Swagger 문서: `https://busanproduct.co.kr/docs` → "다운로드" 태그에서 확인 가능
