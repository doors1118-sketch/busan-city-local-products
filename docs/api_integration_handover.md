# 부산광역시 조달 모니터링 및 AI 챗봇 API 연계 인수인계서

> **최종 작성일**: 2026-05-26  
> **시스템 담당자**: 부산광역시 지역상품 구매 실적 관리 및 조달 법령 자문 AI 챗봇 운영 담당자  
> **API 서버 구동 환경**: `https://busanproduct.co.kr` (내부 포트: `8000`)

---

## 1. API 시스템 및 전달 대상(수신처) 개요

본 서버(FastAPI, `api_server.py`)는 크게 **세 가지의 명확한 전달 대상**을 위해 API 엔드포인트를 나누어 서빙하고 있습니다.

```
                  ┌───────────────────────────────┐
                  │   FastAPI 백엔드 (8000포트)   │
                  └──────────────┬────────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         ▼                       ▼                       ▼
┌───────────────────┐   ┌───────────────────┐   ┌───────────────────┐
│ [1] 챗봇 에이전트 │   │  [2] 운영 대시보드 │   │ [3] 구군청 실무자 │
│   (/opt/advisor)  │   │  (Streamlit/시청) │   │  (엑셀 다운로드)  │
├───────────────────┤   ├───────────────────┤   ├───────────────────┤
│ - /api/chatbot/*  │   │ - /api/summary    │   │ - /api/download/* │
│ - /api/chatbot/   │   │ - /api/ranking    │   │                   │
│   shopping-mall/* │   │ - /api/leakage    │   │                   │
│                   │   │ - /api/monthly-*  │   │                   │
└───────────────────┘   └───────────────────┘   └───────────────────┘
```

1. **AI 챗봇 에이전트 (`/opt/advisor`)**:
   - **경로**: `/api/chatbot/*`, `/api/chatbot/shopping-mall/*`
   - **목적**: 챗봇이 사용자의 조달 문의에 대해 유효한 면허와 실시간 국세청 영업 상태를 갖춘 부산업체를 매칭·추천하도록 데이터 서빙
   - **보안**: PII(개인정보)의 완전 마스킹 및 27개 화이트리스트 필드 엄격 적용

2. **운영 대시보드 (`dashboard.py` 및 대외 연계)**:
   - **경로**: `/api/summary`, `/api/ranking`, `/api/leakage`, `/api/monthly-trend` 등
   - **목적**: 시/군/구 구매 담당자들이 실시간 수주율, 유출 분석, 보호제도 적용율 및 경제 유발 효과를 대시보드 화면(8501포트)을 통해 한눈에 파악하도록 데이터 공급
   - **특징**: `api_cache.json` 및 `monthly_cache.json`을 사용하여 응답 속도 최적화(0.01초 이내)

3. **구군청 행정 실무자 (직접 추출 및 엑셀 보고)**:
   - **경로**: `/api/download/*`
   - **목적**: 구매 적격 부산업체 명단이나 전체 통합업체 마스터를 즉시 엑셀 파일(`.xlsx`)로 직접 내려받아 보고 및 매칭 업무에 활용
   - **필터**: 기본적으로 국세청 영업 상태가 `영업중`인 정상 업체만 자동 필터링 적용

---

## 2. API 엔드포인트 세부 명세 및 기능 검증

모든 API는 로컬 소스 코드(`server_sync/api_server.py`)의 실제 라우트 구조와 매칭 및 검증이 완료되었습니다.

### 2.1 [수신처: 챗봇 에이전트] 챗봇 전용 API

* **데이터 소스**: `chatbot_company.db` (챗봇 전용 SQLite DB, 매일 05시 갱신)
* **공통 필터**: `status_filter` (기본값: `exclude_closed`로 휴·폐업 제외), `validity_filter` (기본값: `valid_only`로 유효한 인증 제품만 노출)

| API URL 경로 | HTTP 메서드 | 주요 요청 파라미터 | 주요 기능 및 검증 결과 |
| :--- | :---: | :--- | :--- |
| `/api/chatbot/company/license-search` | `GET` | `license_name` (필수)<br>`limit` (선택, 기본 50) | 특정 면허(예: 건축공사업) 보유 부산업체 리스트 제공 |
| `/api/chatbot/company/product-search` | `GET` | `product_name` (필수)<br>`limit` (선택, 기본 50) | 특정 세부품명(예: CCTV) 등록 부산업체 리스트 제공 |
| `/api/chatbot/company/category-search` | `GET` | `q` (필수, 분류코드 또는 명칭) | 대분류 품목코드 혹은 분류명(예: 가구, 식품) 기준 검색 |
| `/api/chatbot/company/manufacturers` | `GET` | `limit` (선택, 기본 50) | 부산 소재 제조업체 전체 리스트 제공 |
| `/api/chatbot/company/detail` | `GET` | `company_id` (필수, HMAC 해시) | **(챗봇 핵심 API)** 특정 업체의 면허/인증/MAS/정책 유효성 통합 정보 반환 |
| `/api/chatbot/company/policy-search` | `GET` | `policy_subtype` (여성/장애인/사회적 등) | 여성기업, 장애인기업, 사회적기업 등 우대 기업 검색 |
| `/api/chatbot/product/certified-search` | `GET` | `q` (검색어)<br>`cert_type` (선택) | 신제품(NEP), 신기술(NET), 성능인증 등 13종 기술개발제품 조회 |
| `/api/chatbot/product/innovation-search` | `GET` | `q` (검색어) | 혁신장터 등록 혁신제품 및 혁신시제품 검색 |
| `/api/chatbot/product/priority-purchase-search` | `GET` | `q` (검색어) | 우선구매 대상 기술개발인증 제품 검색 |
| `/api/chatbot/product/excellent-procurement-search` | `GET` | `q` (검색어) | 조달청 지정 우수조달물품 검색 |
| `/api/chatbot/shopping-mall/search` | `GET` | `q` (검색어)<br>`contract_type` (선택) | 종합쇼핑몰(MAS, 일반단가, 제3자단가 등) 가입 물품 검색 |
| `/api/chatbot/health` | `GET` | - | DB 연결 상태 및 테이블별 적재 데이터 건수 반환 |
| `/api/chatbot/version` | `GET` | - | API 버전, 스키마 및 가용한 플래그 정보 확인 |

---

### 2.2 [수신처: 운영 대시보드] 통계 및 실적 분석 API

* **데이터 소스**: `api_cache.json` 및 `monthly_cache.json` (매일 04시 크론탭 자동 생성)
* **특징**: 복잡한 수주율 및 유출액 계산 로직(`core_calc.py`)이 사전 계산된 정적 캐시 파일에서 로드되므로 CPU 부하가 전혀 없음

| API URL 경로 | HTTP 메서드 | 주요 요청 파라미터 | 주요 기능 및 검증 결과 |
| :--- | :---: | :--- | :--- |
| `/api/summary` | `GET` | - | 전체 수주율, 분야별(공사/용역/물품/쇼핑몰) 및 그룹별 수주율 통계 |
| `/api/ranking` | `GET` | - | 부산시 본청, 산하기관, 자치구별 수주 실적 랭킹 상/하위 10선 |
| `/api/ranking/{sector}` | `GET` | `{sector}` (공사/용역/물품/쇼핑몰) | 특정 업종별 수요기관 수주율 순위 분석 |
| `/api/leakage` | `GET` | - | 종합쇼핑몰 및 주요 계약건에 대한 관외(비부산) 유출 종합 분석 |
| `/api/protection` | `GET` | - | 지역제한 및 의무공동도급 제도 적용율 및 미적용 상위 기관 분석 |
| `/api/private-contract` | `GET` | - | 수의계약 대상 지역업체 수주 현황 및 관외 유출 현황 분석 |
| `/api/local-companies` | `GET` | - | 조달 등록된 부산 지역업체의 업종별/품목별 수적 분포 현황 |
| `/api/economic-impact` | `GET` | - | 한은 지역산업연관표 기준 부산 생산유발액 및 고용유발 기여도 |
| `/api/monthly-trend` | `GET` | - | 단월 및 누적 수주율 월별 시계열 추이 및 변동 사유 텍스트 분석 |
| `/api/agency/search` | `GET` | `q` (수요기관명 키워드) | 특정 구청, 산하기관의 구매 실적, 수주율, 주요 유출 계약 분석 |
| `/api/agency/suui-search` | `GET` | `q` (수요기관명 키워드) | 특정 기관의 수의계약 내 관외 유출 현황 집중 검색 |
| `/api/agency/shop-search` | `GET` | `q` (수요기관명 키워드) | 특정 기관의 나라장터 종합쇼핑몰 구매 유출 현황 검색 |

---

## 2.3 [수신처: 구군청 실무자] 엑셀 다운로드 API

* **데이터 소스**: `chatbot_company.db` 및 `company_business_status` 조인
* **다운로드 포맷**: Microsoft Excel 파일 (`.xlsx` 바이너리 스트림)

| API URL 경로 | HTTP 메서드 | 주요 요청 파라미터 | 엑셀 데이터 결과 필드 구성 |
| :--- | :---: | :--- | :--- |
| `/api/download/all-companies` | `GET` | `status` (active_only/all 등)<br>`limit` (선택, 기본 50,000) | **[전체 통합 엑셀]** 부산 지역업체 전체 명단 + 면허 + 등록물품 + 인증제품 + 정책기업 + 쇼핑몰 유형 통합 테이블 |
| `/api/download/license-companies` | `GET` | `license_name` (필수)<br>`status` (선택, 기본 영업중) | 특정 면허 보유 업체의 연락처, 주소, 본사구분, 대표품명 |
| `/api/download/product-companies` | `GET` | `product_name` (필수)<br>`status` (선택) | 특정 세부품명 납품 가능 업체의 주소 및 대표자명 |
| `/api/download/policy-companies` | `GET` | `policy_type` (선택)<br>`status` (선택) | 여성기업/장애인기업/사회적기업 분류별 업체 주소록 및 상세 명단 |
| `/api/download/shopping-mall-products`| `GET` | `contract_type` (선택)<br>`status` (선택) | 종합쇼핑몰(MAS, 제3자단가 등)에 계약 등록된 부산 제품 및 단가 리스트 |
| `/api/download/certified-products` | `GET` | `cert_type` (선택)<br>`status` (선택) | 기술인증(NEP, NET 등) 및 우수조달물품 인증을 보유한 제품 및 업체 명단 |

---

## 3. 연계 운영 시 주의사항 (인수인계 핵심)

1. **개인정보(PII) 누출 원천 차단**:
   - 외부 공개용 API 및 챗봇 API 응답에는 대표자명, 사업자번호(해시로 대체), 대표전화번호 등 민감한 개인정보가 반환되지 않도록 코드가 작성되어 있습니다. 향후 새로운 엔드포인트를 개설할 때도 `_build_chatbot_response()` 내의 필드 화이트리스트 규칙을 엄수해야 합니다.
   
2. **국세청 영업상태 정합성 동기화**:
   - 다운로드 API와 챗봇 상세 API는 `chatbot_company_candidate_view` 뷰를 사용하여 실시간으로 영업 여부(`active`/`closed`/`suspended`)를 확인합니다. 매주 일요일 새벽 06:00에 구동되는 `nts_batch_sync.py` 크론 배치가 정상 작동해야 국세청 최신 데이터 정합성이 유지됩니다.

3. **캐시 빌드 및 서버 리스타트 규칙**:
   - 통계 API(`/api/summary` 등)의 결과값 수정을 수동 반영한 경우, 서버에서 `/opt/busan/venv/bin/python3 build_api_cache.py`를 실행하여 캐시 파일(`api_cache.json`)을 갱신한 뒤 `sudo systemctl restart busan-api` 명령어로 API 서비스를 재시작해 주어야 즉시 반영됩니다.

---

## 4. 데이터 파이프라인 연휴 장애 원인 및 조치 내역 (2026-05-26 핫픽스)

연휴 기간(금~월) 동안 챗봇 DB 적재 지연 및 실패 SMS 경보 문자가 계속 전송된 원인과 2026년 5월 26일 자로 처리한 서버 복구 내역은 다음과 같습니다.

### 4.1 장애 현상 및 원인
1. **서버 `.env` 파일 유실 및 환경변수 전파 오류**:
   - `/opt/busan/.env` 파일 내에 필수 환경변수(`SERVICE_KEY`, `COMPANY_ID_HMAC_SECRET` 등)가 미작성 상태로 초기화되었습니다.
   - 크론탭 쉘 스크립트 실행 환경에서 환경변수가 파이썬의 `os.environ`으로 전파되려면 변수명 앞에 `export` 키워드가 명시되어야 하나, 이것이 누락되어 `Bootstrap failed` 및 `serviceKey not configured` 에러가 발생했습니다.
2. **크론탭 적재 스크립트 오매핑**:
   - 크론탭 스케줄러(05:35분)에 존재하지 않는 `import_direct_production_cert_api.py` 파일 실행 구문이 등록되어 있어 매일 `No such file or directory` 에러가 적재 로그에 기록되었습니다.

### 4.2 조치 및 해결 사항
1. **환경변수 복구 및 `export` 키워드 보강**:
   - `/opt/busan/.env` 파일을 재생성하고, 크론탭 쉘의 환경변수 소싱(`. .env`) 시 정상적으로 전파되도록 모든 변수 선언 앞에 `export` 키워드를 추가 완료했습니다.
2. **크론탭 맵핑 수정**:
   - 직접생산확인 API 수집 기능은 `import_policy_company.py` 내에 통합 처리되므로, 크론탭의 05:35 스케줄 대상을 **`import_policy_company.py`로 정상 교체 및 적용**하였습니다.
3. **수동 파이프라인 싱크 및 검증**:
   - 오늘 자(5/26) 적재 상태를 `success`로 갱신하기 위해 `bootstrap_master_data.py`, `import_certified_product_api.py`를 수동으로 강제 구동하여 정상 실행 완료 및 성공 로그 저장을 확인했습니다.
   - 이에 따라 내일부터는 챗봇 DB 적재 지연 경고 문자가 발송되지 않습니다.
