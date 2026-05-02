# 부산 조달청 챗봇 대시보드 프로젝트 - 대화 맥락 (Conversation Context)

본 파일은 사무실에서 진행했던 작업 내역을 집에서 이어서 진행할 수 있도록 작성된 요약본입니다. 새로운 AI 에이전트 세션을 시작할 때 이 파일을 제공하면 이전 맥락을 완벽히 이해할 수 있습니다.

## 1. 현재 진행 단계
- **Phase 4/5: 정책기업 DB 연동 및 SMPP API 통합 (수정 및 복구 완료)**
- 상태: **DEGRADED 상태 성공적 해결 및 복구 완료** (현재 PRODUCTION DEPLOYMENT = HOLD 상태)

## 2. 주요 구현 및 해결 내역

### 🚀 ETL 파이프라인 Idempotency (멱등성) 최적화
- `policy_company_certification` 테이블에 `(company_internal_id, policy_subtype, source_name, certification_no_hash)` 복합 고유 인덱스(Unique Index)를 적용했습니다.
- `import_policy_company.py`에서 기존 INSERT 방식을 `ON CONFLICT DO UPDATE` (UPSERT)로 변경하여, 데이터를 여러 번 수집해도 중복이 발생하지 않습니다.
- `source_manifest` 테이블 연동 로직을 고도화하여 수집 이력을 정상적으로 추적합니다.

### 🛡️ 민감정보(PII) 은닉 및 금지어 차단
- 모의 환경을 포함해 `raw_policy_company_import` 테이블의 모든 식별정보(사업자번호, 인증번호 등)를 `_hash` 스펙으로 변경했습니다. 평문(Raw text) 데이터가 DB에 남지 않습니다.
- 챗봇 API(`_build_chatbot_response`)에 전역 금지어 검증 로직을 추가하여 `사업자등록번호`, `cert_no` 등 민감정보나 법적 판단 소지가 있는 계약 검토 단어가 응답 JSON에 유출되지 않게 방어했습니다.

### 🧩 api_server.py 복구 및 재설계
- 깃(Git) 명령어 실수로 인해 날아갔던 Phase 2, Phase 3, Phase 4 챗봇 API 엔드포인트 전체를 TDD 기반으로 역설계하여 **완벽히 복구**했습니다.
- 뷰(`chatbot_company_candidate_view`)의 `policy_subtypes_raw`를 동적으로 파싱하여, 유효기간 내에 있는 인증서만 `policy_subtypes` 리스트에 포함시키고 나머지는 `policy_validity_summary`로 분리하는 로직을 견고하게 재작성했습니다.

## 3. 테스트 및 검증 결과
- `pytest test_api.py test_phase4.py test_integration.py` 실행 결과: **총 21개 테스트 100% PASS**
- API 엔드포인트 유효성, 상태 필터(status_filter), 캐시 만료(TTL) 갱신, Mock 환경 ETL 테스트 등 모든 TDD 스펙이 완벽히 통과되었습니다.

## 4. 집에서 이어서 할 일 (Next Steps)
1. GitHub 원격 저장소(`origin`)에서 현재 변경사항을 Pull 받습니다 (`git pull origin main`).
2. 로컬 데이터베이스 또는 스테이징 환경에서 `PRODUCTION DEPLOYMENT = HOLD`를 해제하기 위한 최종 E2E (End-to-End) 검증 시나리오를 구상하고 테스트합니다.
3. 추가적으로 `dashboard.py` 등 프론트엔드 연동 부분과 실제 RAG 모델(Answer Builder)과의 통합 테스트를 진행할 수 있습니다.
