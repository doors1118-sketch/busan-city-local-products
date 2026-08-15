# 사회적기업 엑셀 임포트 및 구매율 대시보드 구축 메모

작성일: 2026-07-22

## 목적

사회적기업은 여성기업/장애인기업과 달리 사업자등록번호를 입력해 일일 검증하는 안정적인 API가 확인되지 않았다. 따라서 고용노동부 인증 사회적기업 원천 엑셀을 수동 업로드하고, 시스템이 사업자등록번호 기준으로 조달업체 DB와 매칭해 업체추천서비스 및 사회적기업 구매율 대시보드에 반영하는 구조로 관리한다.

## 원천 파일

- 파일: `★260612_홈페이지용 사회적기업 리스트(3729개, 26년 제2차, 사업자등록번호 포함) (1).xlsx`
- 서버 업로드 경로: `/opt/busan/import_sources/social_enterprise_20260612/social_enterprise_source_20260612.xlsx`
- 시트: `인증사회적기업`
- 헤더 행: 4행
- 주요 컬럼: 지역, 인증번호, 기관명, 사업내용, 사회적목적실현유형, 사회서비스분야, 사업자등록번호, 대표자, 소재지, 홈페이지, 인증일자, 인증회차

## 임포트 스크립트

- 로컬/서버 파일: `import_social_enterprise_excel.py`
- 서버 배치 위치: `/opt/busan/import_social_enterprise_excel.py`
- 기본 동작: `--apply`가 없으면 읽기 전용 dry-run
- 운영 DB: `/opt/busan/chatbot_company.db`

dry-run:

```bash
cd /opt/busan
sudo -u busan-monitor ./venv/bin/python3 import_social_enterprise_excel.py \
  --source-file /opt/busan/import_sources/social_enterprise_20260612/social_enterprise_source_20260612.xlsx \
  --db /opt/busan/chatbot_company.db \
  --source-refreshed-at '2026-06-12 00:00:00'
```

적용:

```bash
cd /opt/busan
sudo -u busan-monitor ./venv/bin/python3 import_social_enterprise_excel.py \
  --source-file /opt/busan/import_sources/social_enterprise_20260612/social_enterprise_source_20260612.xlsx \
  --db /opt/busan/chatbot_company.db \
  --source-refreshed-at '2026-06-12 00:00:00' \
  --uploaded-by admin \
  --apply
```

## dry-run 검증 결과

2026-07-22 현재 운영 DB 기준:

| 항목 | 건수 |
|---|---:|
| 원천 전체 | 3,729 |
| 원천 부산 소재 | 168 |
| 조달업체 DB 사업자번호 매칭 | 140 |
| 조달업체 DB 부산업체 매칭 | 140 |
| 원천 부산 소재 중 조달업체 매칭 | 137 |
| 원천 부산 소재 중 조달업체 미매칭 | 31 |
| 전체 미매칭 | 3,589 |

해석:

- 업체추천서비스에 바로 반영 가능한 사회적기업 후보는 현재 조달업체 DB와 매칭되는 140건이다.
- 원천 부산 소재 168건 중 31건은 현재 조달업체 DB와 사업자번호가 매칭되지 않는다. 조달등록이 없거나, 조달업체 마스터에 누락된 경우일 수 있으므로 별도 검토 대상이다.
- 전체 사회적기업 3,729건은 사회적기업 구매율 대시보드의 원천 마스터로 보존할 수 있으나, 업체추천서비스 후보는 부산 조달등록 업체와 매칭되는 건만 노출하는 것이 안전하다.

## 2026-07-22 운영 반영 결과

실행 명령:

```bash
cd /opt/busan
sudo -u busan-monitor ./venv/bin/python3 import_social_enterprise_excel.py \
  --source-file /opt/busan/import_sources/social_enterprise_20260612/social_enterprise_source_20260612.xlsx \
  --db /opt/busan/chatbot_company.db \
  --source-refreshed-at '2026-06-12 00:00:00' \
  --uploaded-by codex \
  --apply
```

반영 결과:

| 항목 | 건수 |
|---|---:|
| 원천 전체 | 3,729 |
| 조달업체 DB 매칭 | 140 |
| `policy_company_certification` 반영 | 140 |

백업:

- `/opt/busan/backups/chatbot_company.before_social_enterprise_20260722_175141.db`

기존 구버전 사회적기업 source 정리:

- 기존 `bootstrap_policy_social_excel` 175건이 valid로 남아 있어 후보뷰에 `social_enterprise:valid`가 중복 표시되는 상태였다.
- 2026-06-12 고용노동부 원천 엑셀을 최신 기준으로 보고, 기존 source는 삭제하지 않고 `validity_status='expired'`, `source_manifest.status='superseded'` 처리했다.
- 정리 전 백업: `/opt/busan/backups/chatbot_company.before_social_source_supersede_20260722_175253.db`
- 정리 후 valid 사회적기업 업체 수: 140

미매칭 검토 파일:

- 로컬: `artifacts/social_enterprise_20260722/social_enterprise_unmatched_20260722.xlsx`
- 시트:
  - `Summary`
  - `Busan_Unmatched_31`
  - `All_Unmatched_3589`

업체추천 API 확인:

- `GET http://127.0.0.1:8001/vendor-recommendations/search?q=데스크탑컴퓨터&limit=2&include_product_policy=true`
- 응답 구조는 `candidates`가 아니라 `rows` 기준이다.
- 예시 후보 `주식회사 비엔피`에서 `policy_subtypes='social_enterprise'`, `policy_company_labels='사회적기업'` 표시 확인.

## 생성/갱신 예정 테이블

스크립트는 `--apply` 시에만 아래 테이블을 생성 또는 갱신한다.

- `social_enterprise_master`
  - 사회적기업 원천 엑셀 전체를 사업자번호 기준으로 보존
  - 조달업체 DB 매칭 여부, 부산 조달업체 여부, 원천 파일명/해시/반영배치 ID 저장
- `social_enterprise_import_log`
  - 파일별 업로드/검증/반영 이력 저장
- `policy_company_certification`
  - 조달업체 DB와 매칭된 사회적기업을 `policy_subtype='social_enterprise'`로 반영
  - source_name은 `moel_social_enterprise_excel` 사용
- `source_manifest`
  - 원천 파일 해시, 기준일, 행 수, 상태 저장

## 구매율 산식

사회적기업 구매율은 지역업체 수주율과 산식이 다르므로 별도 계산해야 한다. 2026-07-22 운영 기준은 **쇼핑몰 포함**이다.

```text
사회적기업 구매율 = 사회적기업 수주액 / 물품·용역·쇼핑몰 발주액 * 100

분모: 대상기관의 부산 현장/소비처 물품 + 용역 + 쇼핑몰 발주액
분자: 대상기관의 부산 현장/소비처 공사 + 용역 + 물품 + 쇼핑몰 중 사회적기업 수주액
```

주의:

- 현재 모니터링 DB는 계약자료 기반이므로 공식 집행실적과 차이가 날 수 있다.
- 대시보드에는 `계약자료 기반 추정치` 문구를 표시해야 한다.
- 쇼핑몰 제외 수치도 검증용으로 보관하지만, 대시보드 기본값과 보고 기준은 쇼핑몰 포함 수치다.

## 관리자 페이지 설계

관리자 페이지는 바로 DB 반영을 하지 않고 2단계로 구성한다.

1. 파일 업로드 및 검증
   - 전체 건수
   - 사업자번호 유효 건수
   - 중복/누락 건수
   - 부산 소재 건수
   - 조달업체 DB 매칭 건수
   - 미매칭 건수
2. 사용자가 검증 결과 확인 후 반영
   - 반영 전 DB 백업
   - `social_enterprise_master` 갱신
   - `policy_company_certification` 연동
   - `source_manifest` 및 import log 기록

관리자 인증은 하드코딩하지 않고 환경변수 기반으로 구성해야 한다.

## 사회적기업 구매율 API

대시보드는 계산을 매 요청마다 수행하지 않고, 서버에서 생성한 캐시를 읽는다.

- 운영 캐시 JSON: `/opt/busan/social_purchase_cache.json`
- 운영 캐시 XLSX: `/opt/busan/social_purchase_cache.xlsx`
- 생성 스크립트: `/opt/busan/build_social_purchase_validation.py`
- 기본 수주율 기준: 쇼핑몰 포함
- 자동 갱신: `busan-monitor` crontab에서 매일 04:20 실행
- 자동 갱신 로그: `/opt/busan/sync_log/social_purchase_cache.log`
- 관리자 수동 업로드 후 갱신 로그: `/opt/busan/sync_log/social_purchase_cache_manual.log`

운영 기준:

- 조달청 계약 데이터는 기존 일일 파이프라인과 캐시 빌드가 끝난 뒤 사회적기업 구매율 캐시를 별도 재생성한다.
- 사회적기업 명단은 수동 업로드 방식이지만, 관리자가 `DB 반영`을 실행하면 `/opt/busan/chatbot_company.db` 반영 직후 사회적기업 구매율 캐시 재생성을 백그라운드로 요청한다.
- 캐시 재생성에는 2026-07-27 실측 기준 약 60초가 걸렸다. 관리자 API 응답 타임아웃을 피하기 위해 동기 실행하지 않고 백그라운드 실행으로 처리한다.
- 수동 업로드 후 즉시 캐시 재생성이 실패하더라도 다음 04:20 크론에서 재생성되므로, 최소 1일 이내 대시보드에 반영된다.

공개 API:

| 경로 | 용도 |
|---|---|
| `GET /api/social-enterprise/purchase` | 대시보드용 전체 페이로드 |
| `GET /api/social-enterprise/purchase/summary` | 전체 평균, 모수, 사회적기업 수주액 |
| `GET /api/social-enterprise/purchase/monthly` | 월별 평균값 변동 |
| `GET /api/social-enterprise/purchase/agencies` | 개별 기관별 수주율 |
| `GET /api/social-enterprise/purchase/agency-monthly` | 개별 기관의 월별 수주율 변동 |
| `GET /api/social-enterprise/purchase/contracts` | 개별 기관의 사회적기업 구매실적 |
| `GET /api/social-enterprise/purchase/download` | 전체 또는 기관별 검증 XLSX 다운로드 |
| `GET /api/social-enterprise/dashboard` | 사회적기업 구매 모니터링 HTML 대시보드 |

주요 쿼리 파라미터:

- `/monthly`: `month=YYYY-MM`
- `/agencies`: `q=기관명`, `sort=social_amount|rate|denominator|name`, `limit=100`
- `/agency-monthly`: `agency=기관명`, `month=YYYY-MM`
- `/contracts`: `agency=기관명`, `sector=공사|용역|물품|쇼핑몰`
- `/download`: `agency=기관명`, `sector=공사|용역|물품|쇼핑몰`

API 응답에는 쇼핑몰 포함 기준 필드를 별도로 붙인다.

- `기본모수`: `모수_물품용역쇼핑몰`
- `기본수주율`: `수주율_쇼핑몰포함`
- `기본수주율_기준`: `쇼핑몰 포함`

## 2026-07-22 사회적기업 구매 모니터링 대시보드

공개 URL:

- `https://busanproduct.co.kr/api/social-enterprise/dashboard`

화면 제목:

- `주요 정부 및 국가공공기관 부산 사회적기업 구매 모니터링 대시보드`

화면 구성:

- 상단 KPI: 이번달 사회적기업 수주액, 전월 대비 증감, 목표 수주율 5% 대비 달성률, 전체 누계 수주율과 사회적기업 수주액, 월별 수주율 변동, 분야별 사회적기업 수주액, 사회적기업 수주액 상위 5개사
- 기관별 누계 표: 22개 대상기관의 기관명, 발주액, 사회적기업 수주액, 쇼핑몰 포함 수주율, 용역/물품/쇼핑몰 발주액을 수주율 높은 순서로 표시
- 기관별 카드: 기관명, 사회적기업 수주액, 수주율, 월별 수주율 변동, 주요 계약 3건, 기관별 구매실적 XLSX 다운로드
- 전체 다운로드: `전체 사회적기업 구매실적 XLSX` 버튼은 `/api/social-enterprise/purchase/download`를 호출

제약:

- 목표 5%는 행정 목표값이며, 달성률은 `현재 쇼핑몰 포함 수주율 / 5%`로 계산한다.
- 물품 소비처 판정은 명시적 납품지 필드가 아니라 기존 모니터링 물품 현장/기관 필터를 사용한다.
- 기관별 카드의 주요 계약은 사회적기업 구매실적 원자료 기준 상위 3건 요약이다. 전체 내역은 기관별 XLSX로 확인한다.

## 2026-07-22 관리자 페이지 운영 반영

운영 서버에 사회적기업 엑셀 업로드 관리자 기능을 추가했다.

- 관리자 URL: `https://busanproduct.co.kr/api/admin/social-enterprise`
- 상태 API: `GET /api/admin/social-enterprise/status`
- 업로드/검증/반영 API: `POST /api/admin/social-enterprise/import`
- 인증 방식: HTTP Header `X-Admin-Token`
- 서버 토큰 설정: `/opt/busan/.env`의 `SOCIAL_ADMIN_TOKEN`
- 로컬 토큰 보관: `artifacts/social_enterprise_admin_20260722/admin_token.txt`
- 업로드 저장 경로: `/opt/busan/import_sources/social_enterprise_admin_uploads/`
- 지원 파일: `.xlsx`, `.xlsm`
- 업로드 제한: Nginx `/api/` location에 `client_max_body_size 25m`

동작 방식:

1. 관리자가 사회적기업 XLSX를 선택한다.
2. `검증만 실행`을 누르면 DB를 변경하지 않고 dry-run 결과만 반환한다.
3. `DB 반영`을 누르면 `/opt/busan/chatbot_company.db` 백업 후 `social_enterprise_master`, `policy_company_certification`, `source_manifest`, `social_enterprise_import_log`를 갱신한다.
4. `DB 반영` 후 사회적기업 구매율 캐시(`/opt/busan/social_purchase_cache.json`, `.xlsx`) 재생성을 백그라운드로 요청한다.
5. 반영 후 업체추천 서비스에 즉시 반영하려면 `/opt/advisor` 업체 DB 캐시 동기화가 필요하다.

중요 제약:

- `busan-api.service`는 `busan-monitor` 계정으로 실행된다.
- 업체추천 서비스는 `/opt/advisor/cache/company/cache_current/chatbot_company.db`를 조회한다.
- `/opt/advisor/cache/company/archive`는 root 소유이므로 관리자 페이지에서 자동 캐시 교체를 수행하지 않는다.
- 따라서 관리자 페이지의 `DB 반영`은 `/opt/busan/chatbot_company.db` 반영까지이며, 업체추천 화면 반영은 아래 명령으로 별도 동기화해야 한다.

```bash
cd /opt/advisor
python3 scripts/sync_company_view_db.py \
  --source /opt/busan/chatbot_company.db \
  --cache-root /opt/advisor/cache/company \
  --keep-archives 3 \
  --apply
systemctl restart busan-advisor-pilot.service
```

검증 결과:

- 미인증 요청: `401 Invalid admin token`
- 관리자 페이지 HTML: `200`
- 상태 조회: `200`
- 공개 URL 기준 dry-run 업로드: `200`, `total=3729`, `source_busan=168`, `matched=140`, `policy_upsert=0`
- 서비스 상태: `busan-api`, `nginx`, `busan-dashboard`, `busan-advisor-pilot` 모두 active

## 2026-07-22 추가 운영 기준

향후 사회적기업 엑셀은 전국 원천 전체가 아니라 부산 소재 사회적기업만 업로드하는 방식으로 운영한다.

- 관리자 업로드 기능은 전국 원천 파일과 부산 소재 파일을 모두 처리할 수 있다.
- 운영 기준은 부산 소재 기업만 업로드하는 방식이다.
- 부산 외 사회적기업은 업체추천서비스와 부산 사회적기업 구매율 산정에 직접 필요하지 않으므로 원천 마스터에 보존하지 않아도 된다.
- 단, 엑셀에는 사업자등록번호가 반드시 있어야 한다. 사업자등록번호가 없으면 조달업체 DB와 안정적으로 매칭할 수 없다.
- 조달업체 DB에 없는 부산 사회적기업은 `social_enterprise_master`에 미매칭 상태로 보존한다. 이후 해당 업체가 조달등록되거나 계약 데이터에 등장하면 사업자번호 기준으로 재매칭할 수 있다.
- 관리자 화면의 `실행 결과` JSON 원문 패널은 제거했다. 검증/반영 결과는 상단 상태 메시지, 현재 상태 카드, 최근 임포트 이력으로 확인한다.

## 2026-07-22 디스크 경고 조치

사회적기업 반영 과정에서 `/opt/busan/backups`에 `chatbot_company.db` 원본 백업 2개가 생성되어 루트 디스크 사용률이 90%를 초과했다. 운영 서비스는 정상이었으나, 백업/캐시 보존량을 조정했다.

조치 전:

- `/` 사용률: 92%
- `/opt/busan/backups`: 5.8GB
- `/opt/advisor/cache/company/archive`: 6.9GB

조치:

- `/opt/busan/backups/chatbot_company.before_social_source_supersede_20260722_175253.db` 삭제
- `/opt/busan/backups/chatbot_company.before_social_enterprise_20260722_175141.db`는 gzip 압축 보관
- `/opt/advisor/cache/company/archive/20260720_221016` 삭제
- `/opt/advisor/cache/company/cache_current`와 `cache_previous` 대상은 삭제하지 않음

조치 후:

- `/` 사용률: 78%
- 여유 공간: 약 11GB
- `/opt/advisor/cache/company/archive`는 현재본 `20260722_090053`과 직전본 `20260721_221017`만 유지
- `busan-api`, `busan-dashboard`, `busan-advisor-pilot`, `nginx` 모두 active
