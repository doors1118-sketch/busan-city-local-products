# 조달청 가격정보 API 보강 검토 기록

작성일: 2026-06-11

## 결론

조달청_나라장터 가격정보현황서비스는 업체추천의 자격 판정 데이터가 아니라, 공사용자재/시설공통자재/시장시공가격 여부를 설명하는 보조 근거 데이터로 분리 관리한다.

다만 2026-06-11 운영 서버 실호출 결과, 개발계정 트래픽과 페이지 크기 제한 때문에 전체 데이터를 API로 정기 풀수집하는 방식은 현재 조건에서 운영 불가능하다.

따라서 운영 방향은 다음과 같다.

1. 적격조합, 조합공동사업, 공사용자재 직접구매 대상 원천자료는 사용자가 정기 수동 다운로드 후 임포트한다.
2. 가격정보 API는 운영계정 트래픽 증설 전까지 전체 풀수집이 아니라 대상 품목 중심 또는 제한적 rolling 수집으로만 사용한다.
3. API로 수집되는 가격정보는 별도 테이블에 raw JSON과 정규화 필드를 함께 보존하고, 추천 점수에 즉시 섞지 않는다.
4. 프런트/업체추천 응답에서는 "조달청 가격정보 존재", "시설공통자재/시장시공가격 참고자료 존재" 정도의 근거로 사용한다.

## 공식 출처 기준

- 공공데이터포털: 조달청_나라장터 가격정보현황서비스
  - URL: https://www.data.go.kr/data/15129415/openapi.do
  - 공식 설명: 시설공통자재, 시장시공가격, 공종분류, 표준시장단가, 자원분류, 시설공통자재 종합 가격정보 제공
  - 공식 한계: 자재가격은 조달청 시설공사 원가계산 시 적용하는 참고 가격이며, 수량/난이도/수급상황에 따라 변동 가능
  - 수정일: 2026-03-25
  - 개발계정 트래픽: 1,000

- 공공데이터포털: 조달청_공사용자재 직접구매 대상 조달 내역
  - URL: https://www.data.go.kr/data/15050840/fileData.do
  - 보고서ID: UI-ADOXAA-005R
  - 성격: 파일데이터
  - 추출기준: 전일(D-1)까지 데이터, 계약/납품요구일자 기준
  - 주요 필드: MAS여부, 계약명, 계약방법, 공급금액, 물품분류, 물품식별, 세부품명, 수요기관, 업체, 업체사업자등록번호, 업체소재시도, 조달방식구분

## 서버 실호출 검증

검증 위치: `/opt/busan`

검증일: 2026-06-11

인증키: `/opt/busan/.env`의 `SERVICE_KEY`

실제 동작 base URL:

```text
http://apis.data.go.kr/1230000/ao/PriceInfoService
```

사용자가 제공한 `PriceInfoServiceNO` 경로는 실호출에서 API 없음/404로 확인됐다.

### 호출 가능 엔드포인트와 totalCount

| 엔드포인트 | 의미 | HTTP | totalCount |
|---|---:|---:|---:|
| `getPriceInfoListFcltyCmmnMtrilEngrk` | 시설공통자재(토목) | 200 | 639 |
| `getPriceInfoListFcltyCmmnMtrilBildng` | 시설공통자재(건축) | 200 | 10,749 |
| `getPriceInfoListFcltyCmmnMtrilMchnEqp` | 시설공통자재(기계설비) | 200 | 9,985 |
| `getPriceInfoListFcltyCmmnMtrilElctyIrmc` | 시설공통자재(전기/정보통신) | 200 | 6,112 |
| `getPriceInfoListMrktCnstrctPcEngrk` | 시장시공가격(토목) | 200 | 116 |
| `getPriceInfoListMrktCnstrctPcBildng` | 시장시공가격(건축) | 200 | 1,928 |
| `getPriceInfoListMrktCnstrctPcMchnEqp` | 시장시공가격(기계설비) | 200 | 957 |
| `getCnsttyClsfcInfoList` | 공종분류및세부공종 | 200 | 44,751 |
| `getNetRsceinfoList` | 자원분류및순수자원 | 200 | 58,812 |

### 공식 목록에는 있으나 실호출 실패한 엔드포인트

| 엔드포인트 | 의미 | 실호출 결과 |
|---|---|---:|
| `getStdMarkUprcinfoList` | 표준시장단가및시장시공가격 | HTTP 404 |
| `getPriceInfoListFcltyCmmnMtrilTotal` | 시설공통자재(종합) | HTTP 404 |

이 2개는 공식 참고문서 또는 Swagger 상세에서 실제 operation path 재확인이 필요하다.

## 2026-06-11 장애성 검증 결과

전체 수집을 시도했으나, API가 `numOfRows=1000`을 받아도 실제 응답은 10건으로 제한됐다.

그 결과 전체 수집에는 대략 다음 호출량이 필요하다.

```text
가격정보 7개 엔드포인트: 약 3,050 호출
공종분류/자원분류 2개 엔드포인트: 약 10,357 호출
총합: 약 13,400 호출 이상
```

개발계정 트래픽 1,000을 초과하여 `HTTP 429: API token quota exceeded`가 발생했다.

따라서 현재 키/트래픽 조건에서는 전체 풀수집 크론을 등록하면 매번 실패한다.

## 서버 반영 상태

추가 스크립트:

```text
/opt/busan/import_procurement_price_info_api.py
```

로컬 원본:

```text
C:\Users\COMTREE\Desktop\busanproduct-advisor\scripts\import_procurement_price_info_api.py
```

생성 대상 테이블:

```text
procurement_price_info
procurement_price_info_summary
construction_work_classification
construction_resource_classification
```

2026-06-11 전체 수집 실패 후 불완전 적재 데이터는 초기화했다.

현재 운영 DB 확인값:

```text
procurement_price_info = 0
procurement_price_info_summary = 0
construction_work_classification = 0
construction_resource_classification = 0
```

`source_manifest`의 가격정보 항목은 `paused` 상태로 정리했다.

## 운영 판단

현재 확정된 원천DB 보강 상태는 다음과 같이 본다.

### 완료 또는 운영 가능

- 부산 조달업체 마스터
- 업체 면허/업종
- 유효 사업자/휴폐업 상태
- 정책기업 중 여성기업/장애인기업 등 사업자번호 기반 확인 가능 항목
- 직접생산확인증명서
- 기술개발제품 13종
- MAS/종합쇼핑몰 상품
- 벤처나라 상품/지정업체
- 중기간경쟁제품/필수특이사항/조합/공사용자재 계열 수동임포트 구조

### 조건부 가능

- 조달청 가격정보 API
  - API 실호출 가능성은 확인했다.
  - 전체 자동 적재는 현재 트래픽 조건에서 불가.
  - 운영계정 트래픽 증설 또는 대상 품목 중심 수집 전략이 필요하다.

### 사용자가 계속 수동 관리해야 하는 영역

- SMPP 적격조합 현황
- 조합공동사업제품
- 공사용자재 품목/직접구매 대상 원천 파일
- 사회적기업 등 사업자번호 기반 자동검증이 어려운 정책기업 원천자료

## 다음 결정 필요사항

1. 가격정보 API 운영계정 트래픽 증설 신청 여부
   - 개발계정 1,000 호출로는 전체 수집 불가
   - 운영계정 활용사례 등록 후 트래픽 증가 가능 여부 확인 필요

2. 가격정보를 전수 DB화할지, 대상 품목 중심으로만 조회할지 결정
   - 전수 DB화: 트래픽 증설 필요
   - 대상 품목 중심: 업체추천에서 실제 자주 나오는 품목/공사용자재 품목만 주기 조회

3. 가격정보를 프런트에 표시하는 방식
   - 추천 업체 점수에 직접 반영하지 않는다.
   - "가격정보 참고자료 있음", "공사용 시설자재 성격", "시장시공가격 참고 가능"처럼 별도 안내 카드로 표시한다.

